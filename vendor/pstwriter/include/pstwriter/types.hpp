// pstwriter/types.hpp
//
// Core scalar and identifier types used throughout the pstwriter library.
// All sizes and bit-layouts target the Unicode PST format (wVer = 23) per
// [MS-PST] v11.2.
//
// Strict rules:
//   * No std::span (C++20). Use pointer + length or const std::vector<uint8_t>&.
//   * No std::bit_cast (C++20). Use std::memcpy for type-punning.
//   * Structures defined here are *value types*, not on-disk layouts.
//     On-disk serialization is done field-by-field with explicit
//     little-endian helpers; never reinterpret_cast a struct to a byte buffer.

#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string>

using namespace std;

namespace pstwriter {

// ============================================================================
// Byte-order helpers (little-endian write/read, MSVC-safe).
//
// All on-disk integers in PST are little-endian. These helpers serialize a
// value into a caller-provided buffer at a given offset without invoking
// undefined behaviour on misaligned access. They are constexpr-noexcept
// inline so they're trivially inlined under MSVC /O2.
// ============================================================================
namespace detail {

inline void writeU8(uint8_t* buf, size_t off, uint8_t v) noexcept
{
    buf[off] = v;
}

inline void writeU16(uint8_t* buf, size_t off, uint16_t v) noexcept
{
    buf[off + 0] = static_cast<uint8_t>( v        & 0xFFu);
    buf[off + 1] = static_cast<uint8_t>((v >> 8 ) & 0xFFu);
}

inline void writeU32(uint8_t* buf, size_t off, uint32_t v) noexcept
{
    buf[off + 0] = static_cast<uint8_t>( v        & 0xFFu);
    buf[off + 1] = static_cast<uint8_t>((v >> 8 ) & 0xFFu);
    buf[off + 2] = static_cast<uint8_t>((v >> 16) & 0xFFu);
    buf[off + 3] = static_cast<uint8_t>((v >> 24) & 0xFFu);
}

inline void writeU64(uint8_t* buf, size_t off, uint64_t v) noexcept
{
    buf[off + 0] = static_cast<uint8_t>( v        & 0xFFu);
    buf[off + 1] = static_cast<uint8_t>((v >> 8 ) & 0xFFu);
    buf[off + 2] = static_cast<uint8_t>((v >> 16) & 0xFFu);
    buf[off + 3] = static_cast<uint8_t>((v >> 24) & 0xFFu);
    buf[off + 4] = static_cast<uint8_t>((v >> 32) & 0xFFu);
    buf[off + 5] = static_cast<uint8_t>((v >> 40) & 0xFFu);
    buf[off + 6] = static_cast<uint8_t>((v >> 48) & 0xFFu);
    buf[off + 7] = static_cast<uint8_t>((v >> 56) & 0xFFu);
}

inline uint16_t readU16(const uint8_t* buf, size_t off) noexcept
{
    return static_cast<uint16_t>(
        static_cast<uint16_t>(buf[off + 0])      |
        static_cast<uint16_t>(buf[off + 1]) << 8 );
}

inline uint32_t readU32(const uint8_t* buf, size_t off) noexcept
{
    return  static_cast<uint32_t>(buf[off + 0])        |
           (static_cast<uint32_t>(buf[off + 1]) << 8 ) |
           (static_cast<uint32_t>(buf[off + 2]) << 16) |
           (static_cast<uint32_t>(buf[off + 3]) << 24);
}

inline uint64_t readU64(const uint8_t* buf, size_t off) noexcept
{
    return  static_cast<uint64_t>(buf[off + 0])        |
           (static_cast<uint64_t>(buf[off + 1]) << 8 ) |
           (static_cast<uint64_t>(buf[off + 2]) << 16) |
           (static_cast<uint64_t>(buf[off + 3]) << 24) |
           (static_cast<uint64_t>(buf[off + 4]) << 32) |
           (static_cast<uint64_t>(buf[off + 5]) << 40) |
           (static_cast<uint64_t>(buf[off + 6]) << 48) |
           (static_cast<uint64_t>(buf[off + 7]) << 56);
}

} // namespace detail

// ============================================================================
// PST format-wide constants
// ============================================================================
constexpr size_t kPageSize        = 512;   // every page in a PST is 512 B
constexpr size_t kBlockAlignment  = 64;    // blocks are 64-byte aligned
constexpr size_t kMaxBlockPayload = 8176;  // 8176 + 16-byte trailer = 8192

// HEADER ([MS-PST] §2.2.2.6, verified against §3.2 sample, SPEC_GROUND_TRUTH).
// HEADER is 564 bytes (0x000..0x233). There is NO header copy at 0x200 —
// the bytes at 0x200..0x233 are the *tail* of a single HEADER (bSentinel,
// bCryptMethod, bidNextB, dwCRCFull, …). Bytes 0x234..0x3FF are zero
// padding; the first AMap begins at 0x400.
constexpr size_t kHeaderOffset      = 0x000;
constexpr size_t kHeaderSize        = 564;   // 0x234
constexpr size_t kFirstAMapOffset   = 0x400;

// HEADER magic values  ([MS-PST] §2.2.2.6)
constexpr uint32_t kMagicDword     = 0x4E444221u; // "!BDN" (file)
constexpr uint16_t kMagicClient    = 0x4D53u;     // "SM"
constexpr uint16_t kVerUnicode     = 23u;         // wVer for ANSI was 14/15
constexpr uint16_t kVerClient      = 19u;
constexpr uint8_t  kPlatformCreate = 0x01u;
constexpr uint8_t  kPlatformAccess = 0x01u;
constexpr uint8_t  kSentinelByte   = 0x80u;       // HEADER offset 0x1E0

// HEADER CRC ranges  ([MS-PST] §2.2.2.6, end-to-end verified against the
// §3.2 sample header in SPEC_GROUND_TRUTH.md):
//   dwCRCPartial at 0x004: CRC of bytes [0x008 .. 0x1DF) = 471 bytes
//   dwCRCFull    at 0x20C: CRC of bytes [0x008 .. 0x20C) = 516 bytes
// Both verified to reproduce the §3.2 sample's stored CRCs:
//   crc32(bytes 0x008..0x1DE) == 0x379AA90E (dwCRCPartial)
//   crc32(bytes 0x008..0x20B) == 0x1FD283D6 (dwCRCFull)
constexpr size_t kHdrCrcPartialOff = 0x004;
constexpr size_t kHdrCrcPartialBeg = 0x008;
constexpr size_t kHdrCrcPartialLen = 471;
constexpr size_t kHdrCrcFullOff    = 0x20C;
constexpr size_t kHdrCrcFullBeg    = 0x008;
constexpr size_t kHdrCrcFullLen    = 516;

// ============================================================================
// Crypt method  ([MS-PST] §1.3.1.5, §5.1, §5.2)
// ============================================================================
enum class CryptMethod : uint8_t {
    None    = 0x00, // no encoding; data stored as-is
    Permute = 0x01, // NDB_CRYPT_PERMUTE — default for new files (§5.1)
    Cyclic  = 0x02, // NDB_CRYPT_CYCLIC  (§5.2)
};

// ============================================================================
// Node-type values stored in the low 5 bits of an NID  ([MS-PST] §2.2.2.1)
// ============================================================================
enum class NidType : uint8_t {
    HID                          = 0x00, // not a node — heap id (LTP layer)
    Internal                     = 0x01, // NID_TYPE_INTERNAL (e.g. message store)
    NormalFolder                 = 0x02, // NID_TYPE_NORMAL_FOLDER
    SearchFolder                 = 0x03, // NID_TYPE_SEARCH_FOLDER
    NormalMessage                = 0x04, // NID_TYPE_NORMAL_MESSAGE
    Attachment                   = 0x05, // NID_TYPE_ATTACHMENT
    SearchUpdateQueue            = 0x06,
    SearchCriteriaObject         = 0x07,
    AssocMessage                 = 0x08, // FAI message
    ContentsTableIndex           = 0x0A,
    ReceiveFolderTable           = 0x0B,
    OutgoingQueueTable           = 0x0C,
    HierarchyTable               = 0x0D,
    ContentsTable                = 0x0E,
    AssocContentsTable           = 0x0F,
    SearchContentsTable          = 0x10,
    AttachmentTable              = 0x11,
    RecipientTable               = 0x12,
    SearchTableIndex             = 0x13,
    LtpReserved                  = 0x1F,
};

// ============================================================================
// NID — Node Identifier (32-bit). [MS-PST] §2.2.2.1
//   bits[4:0]   nidType
//   bits[31:5]  nidIndex (27-bit monotonic counter)
// ============================================================================
struct Nid {
    uint32_t value{};

    constexpr Nid() noexcept = default;
    constexpr explicit Nid(uint32_t raw) noexcept : value(raw) {}

    constexpr Nid(NidType t, uint32_t idx) noexcept
        : value(((idx & 0x07FFFFFFu) << 5) | (static_cast<uint32_t>(t) & 0x1Fu))
    {}

    constexpr NidType  type()  const noexcept { return static_cast<NidType>(value & 0x1Fu); }
    constexpr uint32_t index() const noexcept { return value >> 5; }

    constexpr bool operator==(const Nid& o) const noexcept { return value == o.value; }
    constexpr bool operator!=(const Nid& o) const noexcept { return value != o.value; }
    constexpr bool operator< (const Nid& o) const noexcept { return value <  o.value; }
};

static_assert(sizeof(Nid) == 4, "Nid must occupy exactly 4 bytes");

// Well-known NIDs (every valid Unicode PST must contain these). [MS-PST] §2.4.8
// Each value here matches: (index << 5) | nidType.
constexpr Nid kNidMessageStore           { 0x00000021u }; // (0x01 << 5) | 0x01
constexpr Nid kNidNameToIdMap            { 0x00000061u };
constexpr Nid kNidNormalFolderTemplate   { 0x000000A1u };
constexpr Nid kNidSearchFolderTemplate   { 0x000000C1u };
constexpr Nid kNidRootFolder             { 0x00000122u }; // (0x09 << 5) | 0x02 — Top of PST
constexpr Nid kNidSearchManagementQueue  { 0x000001E1u };
constexpr Nid kNidSearchActivityList     { 0x00000201u };
constexpr Nid kNidSearchDomainAlternative{ 0x00000241u };
constexpr Nid kNidSearchDomainObject     { 0x00000261u };
constexpr Nid kNidSearchGathererQueue    { 0x00000281u };
constexpr Nid kNidSearchGathererDescriptor{0x000002A1u };
constexpr Nid kNidWasteBasket            { 0x00000225u };
constexpr Nid kNidFinderFolder           { 0x00000242u };
constexpr Nid kNidSpoolerQueue           { 0x000002E1u };

// ============================================================================
// BID — Block Identifier (64-bit). [MS-PST] §2.2.2.2
//   bit[0]       reserved 'A' flag (1 = internal)
//   bit[1]       'B' flag         (1 = internal block, 0 = data block)
//   bits[63:2]   bidIndex (monotonic counter)
//
// On disk the *raw* 64-bit value is stored. Counters increment by 4
// (because the bottom two bits encode flags), so allocator code generates
// successive BIDs as bid.value += 4.
// ============================================================================
struct Bid {
    uint64_t value{};

    constexpr Bid() noexcept = default;
    constexpr explicit Bid(uint64_t raw) noexcept : value(raw) {}

    constexpr static Bid makeData(uint64_t idx) noexcept
    {
        // bit[0] and bit[1] both 0 => external/data block.
        return Bid{ (idx & 0x3FFFFFFFFFFFFFFFull) << 2 };
    }
    constexpr static Bid makeInternal(uint64_t idx) noexcept
    {
        // bit[1] set => internal block (XBLOCK/XXBLOCK/SLBLOCK/SIBLOCK).
        // The spec requires bit[1] = 1 for any internal BID. Bit[0] is also
        // commonly set on internal blocks; pstwriter sets both to be safe and
        // consistent with what Outlook produces.
        return Bid{ ((idx & 0x3FFFFFFFFFFFFFFFull) << 2) | 0x3ull };
    }

    // AMap (and PMap) PAGETRAILER.bid is the page's own file offset per
    // [MS-PST] §2.2.2.7.2 + §2.6.1 — Outlook walks `Read(@ib)` and asserts
    // trailer.bid == ib. See KNOWN_UNVERIFIED.md M11-E for the diagnostic
    // that exposed this ("Read(@400): Expected bid=400, but read bid=6").
    constexpr static Bid makeAmap(uint64_t ib) noexcept { return Bid{ib}; }

    constexpr bool     isInternal() const noexcept { return (value & 0x2ull) != 0; }
    constexpr bool     isData()     const noexcept { return (value & 0x2ull) == 0; }
    constexpr uint64_t index()      const noexcept { return value >> 2; }

    constexpr bool operator==(const Bid& o) const noexcept { return value == o.value; }
    constexpr bool operator!=(const Bid& o) const noexcept { return value != o.value; }
    constexpr bool operator< (const Bid& o) const noexcept { return value <  o.value; }
};

static_assert(sizeof(Bid) == 8, "Bid must occupy exactly 8 bytes");

// Distinguished BID values used by the format itself.
constexpr Bid kBidNil { 0x0000000000000000ull };

// ============================================================================
// IB — Byte offset within the PST file (64-bit). [MS-PST] §2.2.2.3
// ============================================================================
struct Ib {
    uint64_t value{};

    constexpr Ib() noexcept = default;
    constexpr explicit Ib(uint64_t raw) noexcept : value(raw) {}

    constexpr bool operator==(const Ib& o) const noexcept { return value == o.value; }
    constexpr bool operator!=(const Ib& o) const noexcept { return value != o.value; }
    constexpr bool operator< (const Ib& o) const noexcept { return value <  o.value; }
};

static_assert(sizeof(Ib) == 8, "Ib must occupy exactly 8 bytes");

// ============================================================================
// BREF — (BID, IB) pair; 16 bytes on disk for Unicode PST. [MS-PST] §2.2.2.4
// ============================================================================
struct Bref {
    Bid bid{};
    Ib  ib{};

    constexpr Bref() noexcept = default;
    constexpr Bref(Bid b, Ib i) noexcept : bid(b), ib(i) {}

    constexpr bool operator==(const Bref& o) const noexcept
    {
        return bid == o.bid && ib == o.ib;
    }
};

static_assert(sizeof(Bref) == 16, "BREF must occupy exactly 16 bytes (Unicode PST)");

// ============================================================================
// FILETIME — Windows 100-ns ticks since 1601-01-01 UTC. 8 bytes on disk.
// ============================================================================
struct FileTime {
    uint64_t ticks{};

    constexpr FileTime() noexcept = default;
    constexpr explicit FileTime(uint64_t v) noexcept : ticks(v) {}
};

static_assert(sizeof(FileTime) == 8, "FileTime must occupy exactly 8 bytes");

// ============================================================================
// Property type (low 16 bits of a PropTag). [MS-OXCDATA] §2.11.1
// Only the values used by pstwriter are listed here; we add more in M4–M6.
// ============================================================================
enum class PropType : uint16_t {
    Unspecified  = 0x0000,
    Null         = 0x0001,
    Int16        = 0x0002, // PT_SHORT
    Int32        = 0x0003, // PT_LONG
    Float32      = 0x0004,
    Float64      = 0x0005,
    Currency     = 0x0006,
    AppTime      = 0x0007,
    ErrorCode    = 0x000A,
    Boolean      = 0x000B,
    Object       = 0x000D,
    Int64        = 0x0014, // PT_LONGLONG
    String8      = 0x001E, // ANSI string (legacy)
    Unicode      = 0x001F, // UTF-16LE, null-terminated
    SystemTime   = 0x0040, // FILETIME
    ClassId      = 0x0048, // GUID
    Binary       = 0x0102,
    MvInt16      = 0x1002,
    MvInt32      = 0x1003,
    MvFloat32    = 0x1004,
    MvFloat64    = 0x1005,
    MvCurrency   = 0x1006,
    MvAppTime    = 0x1007,
    MvInt64      = 0x1014,
    MvString8    = 0x101E,
    MvUnicode    = 0x101F,
    MvSystemTime = 0x1040,
    MvClassId    = 0x1048,
    MvBinary     = 0x1102,
};

// ============================================================================
// PropTag — 32-bit (id << 16) | type.  [MS-OXCDATA] §2.9
// ============================================================================
struct PropTag {
    uint32_t value{};

    constexpr PropTag() noexcept = default;
    constexpr explicit PropTag(uint32_t raw) noexcept : value(raw) {}
    constexpr PropTag(uint16_t propId, PropType t) noexcept
        : value((static_cast<uint32_t>(propId) << 16) |
                 static_cast<uint32_t>(t))
    {}

    constexpr uint16_t id()   const noexcept { return static_cast<uint16_t>(value >> 16); }
    constexpr PropType type() const noexcept { return static_cast<PropType>(value & 0xFFFFu); }

    constexpr bool operator==(const PropTag& o) const noexcept { return value == o.value; }
    constexpr bool operator!=(const PropTag& o) const noexcept { return value != o.value; }
    constexpr bool operator< (const PropTag& o) const noexcept { return value <  o.value; }
};

static_assert(sizeof(PropTag) == 4, "PropTag must occupy exactly 4 bytes");

// ============================================================================
// pid:: — well-known property tag values used by the messaging layer.
// Source: [MS-OXPROPS] (canonical names).  Only tags actually referenced by
// pstwriter are declared here; the list grows as later milestones land.
// ============================================================================
namespace pid {

// --- Generic / message-store-required (used heavily in M5) -------------------
constexpr PropTag DisplayName                  { 0x3001u, PropType::Unicode    };
constexpr PropTag Comment                      { 0x3004u, PropType::Unicode    };
constexpr PropTag CreationTime                 { 0x3007u, PropType::SystemTime };
constexpr PropTag LastModificationTime         { 0x3008u, PropType::SystemTime };
constexpr PropTag RecordKey                    { 0x0FF9u, PropType::Binary     };
constexpr PropTag StoreSupportMask             { 0x340Du, PropType::Int32      };
constexpr PropTag StoreState                   { 0x340Eu, PropType::Int32      };
constexpr PropTag IpmSubTreeEntryId            { 0x35E0u, PropType::Binary     };
constexpr PropTag IpmWastebasketEntryId        { 0x35E3u, PropType::Binary     };
constexpr PropTag FinderEntryId                { 0x35E7u, PropType::Binary     };

// --- Folder properties -------------------------------------------------------
constexpr PropTag ContentCount                 { 0x3602u, PropType::Int32      };
constexpr PropTag ContentUnreadCount           { 0x3603u, PropType::Int32      };
constexpr PropTag Subfolders                   { 0x360Au, PropType::Boolean    };
constexpr PropTag ContainerClass               { 0x3613u, PropType::Unicode    };

// --- Message properties (used in M6) -----------------------------------------
constexpr PropTag MessageClass                 { 0x001Au, PropType::Unicode    };
constexpr PropTag Subject                      { 0x0037u, PropType::Unicode    };
constexpr PropTag Body                         { 0x1000u, PropType::Unicode    };
constexpr PropTag MessageDeliveryTime          { 0x0E06u, PropType::SystemTime };
constexpr PropTag MessageFlags                 { 0x0E07u, PropType::Int32      };
constexpr PropTag MessageSize                  { 0x0E08u, PropType::Int32      };
constexpr PropTag SenderName                   { 0x0C1Au, PropType::Unicode    };
constexpr PropTag SenderEmailAddress           { 0x0C1Fu, PropType::Unicode    };

} // namespace pid

} // namespace pstwriter
