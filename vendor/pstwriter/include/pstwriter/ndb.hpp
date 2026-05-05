// pstwriter/ndb.hpp
//
// Layer-1 (NDB) on-disk structures and serialization for Unicode PST.
//
// ALL on-disk integers are little-endian. ALL structures are serialized
// field-by-field via detail::writeU8/16/32/64 — never via memcpy of a
// host struct, because MSVC will pad and corrupt the file (KNOWN BUG #7).
//
// The ROOT offsets inside HEADER are spelled out in KNOWN BUG #3 of
// SPEC_BRIEF.md and asserted byte-for-byte in test_ndb.cpp. Do not
// move any of the offset constants below without re-running the gate.

#pragma once

#include "types.hpp"

#include <array>
#include <cstdint>
#include <cstddef>
#include <vector>

using namespace std;

namespace pstwriter {

// ============================================================================
// PageTrailer ([MS-PST] §2.2.2.7.1) — last 16 bytes of every 512-byte page.
// ============================================================================
namespace ptype {

constexpr uint8_t kBBT   = 0x80; // Block BTree page
constexpr uint8_t kNBT   = 0x81; // Node BTree page
constexpr uint8_t kFMap  = 0x82; // deprecated
constexpr uint8_t kPMap  = 0x83; // deprecated
constexpr uint8_t kAMap  = 0x84; // Allocation Map
constexpr uint8_t kFPMap = 0x85; // deprecated
constexpr uint8_t kDList = 0x86; // Density List

} // namespace ptype

// PageTrailer write offsets within a 512-byte page buffer.
constexpr size_t kPageTrailerOffset = 496;
constexpr size_t kPageBodySize      = 496;
constexpr size_t kPageTrailerSize   = 16;

static_assert(kPageBodySize + kPageTrailerSize == kPageSize,
              "Page body + trailer must equal kPageSize");

// Compute the wSig used by both PAGETRAILER and BLOCKTRAILER per
// [MS-PST] §5.5.  The spec's reference C is for 32-bit ANSI IB:
//
//     ib  ^= bid;
//     return (WORD)((ib >> 16) ^ (WORD)ib);
//
// For Unicode PST (64-bit IB and BID) we truncate the XOR result to 32
// bits before folding — this matches what libpff/mfcmapi compute in
// practice and is what the §3.5 spec sample assumes (its bid=0x246,
// ib=0x900200 produce wSig=0x00D6 only with the fold, not with a naive
// `(ib ^ bid) & 0xFFFF`).
//
// CAUTION: use bid.value (the raw 64-bit value), NOT bid.index() — that
// is KNOWN BUG #5.
inline uint16_t computeBlockSig(Bid bid, Ib ib) noexcept
{
    const uint32_t mix = static_cast<uint32_t>((ib.value ^ bid.value) & 0xFFFFFFFFull);
    return static_cast<uint16_t>((mix >> 16) ^ (mix & 0xFFFFu));
}

// Write a 16-byte PageTrailer to the trailing 16 bytes of `page` and
// compute dwCRC over the first 496 bytes.
//
// `wSig` is 0 for AMap/PMap/FMap/FPMap/DList pages (the spec says so);
//        for NBT and BBT pages it is computed exactly like a block sig
//        from (ib ^ bid.value).
// `bid`  is 0 for AMap/PMap/FMap pages; otherwise the page's own BID.
//
// Caller is responsible for filling bytes 0..495 first.
void writePageTrailer(array<uint8_t, kPageSize>& page,
                      uint8_t ptype,
                      Bid     bid,
                      Ib      ib) noexcept;

// ============================================================================
// BlockTrailer ([MS-PST] §2.2.2.8.1) — last 16 bytes of every block.
// ============================================================================
constexpr size_t kBlockTrailerSize = 16;

// Append a 16-byte BlockTrailer to `bytes`, computing the CRC over the
// already-encoded payload bytes that precede it.  `cbPlaintext` is the
// pre-encoding payload size (this is what goes into the trailer's `cb`
// field, NOT the encoded size).
//
// Order of operations the caller MUST follow:
//   1.  encode the plaintext via encodePermute / encodeCyclic
//   2.  call appendBlockTrailer with the encoded buffer
// Computing the CRC over plaintext silently breaks Outlook compatibility.
void appendBlockTrailer(vector<uint8_t>& bytes,
                        uint16_t         cbPlaintext,
                        Bid              bid,
                        Ib               ib) noexcept;

// ============================================================================
// ROOT — bytes 0xB4..0xFB of HEADER ([MS-PST] §2.2.2.6, KNOWN BUG #3)
// ============================================================================
struct Root {
    Ib       ibFileEof  {};   // total file size, written at HEADER+0xB8
    Ib       ibAMapLast {};   // offset of last AMap page, at HEADER+0xC0
    uint64_t cbAMapFree {};   // free bytes advertised by AMaps, at HEADER+0xC8
    uint64_t cbPMapFree {};   // free bytes advertised by PMaps, at HEADER+0xD0
    Bref     brefNbt    {};   // NBT root, bid at +0xD8, ib at +0xE0
    Bref     brefBbt    {};   // BBT root, bid at +0xE8, ib at +0xF0
    uint8_t  fAMapValid {};   // at HEADER+0xF8 (0=invalid, 1=valid, 2=valid2)
};

constexpr uint8_t kAMapValid2 = 0x02;     // value Outlook writes for new files

// Spelled-out HEADER offsets — single source of truth.
// Verified end-to-end against [MS-PST] §3.2 sample header (SPEC_GROUND_TRUTH).
namespace hdr {

constexpr size_t kMagic          = 0x000;
constexpr size_t kCrcPartial     = 0x004;
constexpr size_t kMagicClient    = 0x008;
constexpr size_t kVer            = 0x00A;
constexpr size_t kVerClient      = 0x00C;
constexpr size_t kPlatformCreate = 0x00E;
constexpr size_t kPlatformAccess = 0x00F;
constexpr size_t kReserved1      = 0x010;
constexpr size_t kReserved2      = 0x014;
constexpr size_t kBidUnused      = 0x018; // 8 bytes (Unicode)
constexpr size_t kBidNextP       = 0x020; // 8 bytes
constexpr size_t kDwUnique       = 0x028;
constexpr size_t kRgnid          = 0x02C; // 32 NIDs = 128 bytes
constexpr size_t kQwUnused       = 0x0AC; // 8 bytes

// ROOT (still 0x0B4..0x0FB).
constexpr size_t kRootBegin      = 0x0B4;
constexpr size_t kRootDwReserved = 0x0B4;
constexpr size_t kIbFileEof      = 0x0B8;
constexpr size_t kIbAMapLast     = 0x0C0;
constexpr size_t kCbAMapFree     = 0x0C8;
constexpr size_t kCbPMapFree     = 0x0D0;
constexpr size_t kBrefNbtBid     = 0x0D8;
constexpr size_t kBrefNbtIb      = 0x0E0;
constexpr size_t kBrefBbtBid     = 0x0E8;
constexpr size_t kBrefBbtIb      = 0x0F0;
constexpr size_t kFAMapValid     = 0x0F8;
constexpr size_t kRootBReserved  = 0x0F9;
constexpr size_t kRootWReserved  = 0x0FA;
constexpr size_t kRootEnd        = 0x0FC; // exclusive

constexpr size_t kDwAlign        = 0x0FC;
constexpr size_t kRgbFM          = 0x100; // 128 bytes, all 0xFF (deprecated FMap)
constexpr size_t kRgbFP          = 0x180; // 128 bytes, all 0xFF (deprecated FPMap)

// Trailing fields — these are at the *real* offsets per [MS-PST] §3.2 sample.
// SPEC_BRIEF placed bSentinel at 0x1E0 etc.; that was wrong — it conflated
// HEADER size with kPageSize. The real HEADER is 564 bytes.
constexpr size_t kBSentinel      = 0x200;
constexpr size_t kBCryptMethod   = 0x201;
constexpr size_t kRgbReserved    = 0x202; // 2 bytes
constexpr size_t kBidNextB       = 0x204; // 8 bytes
constexpr size_t kCrcFull        = 0x20C;
constexpr size_t kRgbReserved2   = 0x210; // 3 bytes
constexpr size_t kBReserved      = 0x213;
constexpr size_t kRgbReserved3   = 0x214; // 32 bytes
constexpr size_t kHeaderEnd      = 0x234; // exclusive — total HEADER size

static_assert(kHeaderEnd == kHeaderSize, "hdr::kHeaderEnd must match types.hpp kHeaderSize");

} // namespace hdr

// ============================================================================
// WriterState — everything the HEADER serializer needs from M2 layer.
// (M3+ will enrich this; HEADER-relevant fields stay stable.)
//
// rgnid[32] holds the per-NID-type starting nidIndex array. For a brand
// new PST, fill it via populateFreshRgnid(); for tests that round-trip an
// existing HEADER (e.g. the §3.2 sample) the caller copies the on-disk
// values directly so byte-for-byte equality is possible.
// ============================================================================
struct WriterState {
    Root            root {};
    Bid             bidNextP {Bid{0x00000000FFFFFFFFull}}; // any sentinel; overwritten by writer
    Bid             bidNextB {Bid{0x0000000000000004ull}};
    uint32_t        dwUnique {1u};
    array<uint32_t, 32> rgnid {}; // see populateFreshRgnid

    // "Reserved/unused" fields the spec says creators set to 0 for fresh
    // PSTs.  Real Outlook-produced files (and the §3.2 sample) sometimes
    // carry non-zero scratch values here; exposing them lets us round-
    // trip an existing HEADER byte-for-byte.  Default = 0 = fresh PST.
    uint32_t        dwReserved1 {0u};
    uint32_t        dwReserved2 {0u};
    uint64_t        bidUnused   {0ull};
};

// Fills `s.rgnid` with the spec-mandated starting indices for a fresh PST
// (cf. [MS-PST] §2.2.2.6 / SPEC_GROUND_TRUTH).
void populateFreshRgnid(WriterState& s) noexcept;

// ============================================================================
// HEADER serialization — produce the 564-byte Unicode HEADER given the
// writer state. Both CRCs (dwCRCPartial at 0x004 and dwCRCFull at 0x20C)
// are computed inside this function over the spec-mandated ranges:
//   dwCRCPartial = crc32(buf + 0x008, 471)
//   dwCRCFull    = crc32(buf + 0x008, 516)
// Both ranges start AFTER the CRC fields, so write order does not matter
// for correctness. (We do them in declaration order anyway.)
// ============================================================================
array<uint8_t, kHeaderSize> serializeHeader(const WriterState& s) noexcept;

// Page-level builders (buildAMap / buildEmptyNbtLeaf / buildEmptyBbtLeaf)
// live in page.hpp.

// ============================================================================
// AMap geometry constants — exposed so test_ndb can recompute coverage.
// ============================================================================
constexpr size_t kAMapBitmapBytes = 496;                                 // bytes of bitmap before trailer
constexpr size_t kBytesPerAMapBit = 64;                                  // each bit covers 64 file bytes
constexpr size_t kAMapCoverage    = kAMapBitmapBytes * 8 * kBytesPerAMapBit; // 253,952 bytes per AMap

// ============================================================================
// BTPAGE entry-size constants ([MS-PST] §2.2.2.7.7)
// ============================================================================
constexpr size_t kBtPageEntriesArea = 488;      // bytes 0..487 in a BTPAGE
constexpr size_t kBtPagePadBytes    = 4;        // KNOWN BUG #2 — 4, not 8
constexpr size_t kNbtLeafEntrySize  = 32;       // Unicode NBTENTRY size
constexpr size_t kBbtLeafEntrySize  = 24;       // Unicode BBTENTRY size

// BTPAGE field offsets within the 488..495 trailer-prefix area
// (SPEC_VERIFIED: control bytes precede dwPadding, NOT the other way around).
constexpr size_t kBtPageCEnt    = 488;
constexpr size_t kBtPageCEntMax = 489;
constexpr size_t kBtPageCbEnt   = 490;
constexpr size_t kBtPageCLevel  = 491;
constexpr size_t kBtPageDwPad   = 492; // 4 bytes, must be 0 (KNOWN BUG #2: 4 bytes, not 8)

} // namespace pstwriter
