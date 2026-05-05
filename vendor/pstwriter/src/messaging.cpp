// pstwriter/src/messaging.cpp
//
// M6 Messaging Core — implementations.
//
// First builder: buildMessageStorePc (§2.7.1 NID_MESSAGE_STORE).
// Subsequent Phase B/C builders (Root Folder PC, sub-folder PCs,
// hierarchy/contents/FAI TCs, templates, recipient/attachment, NameToId
// map) layer on top of the same M4 LTP primitives.

#include "messaging.hpp"

#include "block.hpp"
#include "ltp.hpp"
#include "types.hpp"
#include "writer.hpp"

#include <array>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <vector>

using namespace std;
using namespace pstwriter;

namespace pstwriter {

// ----------------------------------------------------------------------------
// EntryID encoder — 4-byte rgbFlags + 16-byte ProviderUID + 4-byte NID.
// Verified against [MS-PST] §3.10 sample's 3 EntryID values (2026-05-04).
// ----------------------------------------------------------------------------
array<uint8_t, kEntryIdSize>
makeEntryId(const array<uint8_t, 16>& providerUid,
            Nid                       entryNid,
            uint32_t                  rgbFlags) noexcept
{
    array<uint8_t, kEntryIdSize> out{};
    detail::writeU32(out.data(),  0, rgbFlags);
    std::memcpy   (out.data() + 4, providerUid.data(), 16);
    detail::writeU32(out.data(), 20, entryNid.value);
    return out;
}

// ----------------------------------------------------------------------------
// buildMessageStorePc — 11-property PC matching the §3.10 schema.
//
// Property list and order (sorted by PidTag id ascending — buildPropertyContext
// re-sorts internally so caller order is irrelevant; we list in spec order
// for readability):
//
//   0x0E34  PidTagReplVersionhistory     PtypBinary    24 B  (HN-stored)
//   0x0E38  PidTagReplFlags              PtypInteger32  4 B  (inline)
//   0x0FF9  PidTagRecordKey              PtypBinary    16 B  (HN-stored)
//   0x3001  PidTagDisplayName            PtypString    var.  (HN-stored)
//   0x35DF  PidTagValidFolderMask        PtypInteger32  4 B  (inline)
//   0x35E0  PidTagIpmSubTreeEntryId      PtypBinary    24 B  (HN-stored)
//   0x35E3  PidTagIpmWastebasketEntryId  PtypBinary    24 B  (HN-stored)
//   0x35E7  PidTagFinderEntryId          PtypBinary    24 B  (HN-stored)
//   0x6633  (PstHidden* boolean)         PtypBoolean    1 B  (inline)
//   0x66FA  (PstHidden* int32)           PtypInteger32  4 B  (inline)
//   0x67FF  PidTagPstPassword            PtypInteger32  4 B  (inline)
// ----------------------------------------------------------------------------
PcResult buildMessageStorePc(const MessageStoreSchema& schema,
                             Nid                       firstSubnodeNid)
{
    // ---- HN-stored value buffers ----
    // PidTagReplVersionhistory: prefix(4) + providerUid(16) + suffix(4).
    array<uint8_t, 24> replVersion{};
    detail::writeU32(replVersion.data(),  0, schema.replVersionPrefix);
    std::memcpy   (replVersion.data() + 4, schema.providerUid.data(), 16);
    detail::writeU32(replVersion.data(), 20, schema.replVersionSuffix);

    const auto subtreeEntryId = makeEntryId(schema.providerUid, schema.ipmSubTreeNid);
    const auto wastebasketEid = makeEntryId(schema.providerUid, schema.ipmWastebasketNid);
    const auto finderEntryId  = makeEntryId(schema.providerUid, schema.finderNid);

    // ---- Inline value buffers (LE-packed, portable across endianness) ----
    array<uint8_t, 4> replFlagsBytes{};
    detail::writeU32(replFlagsBytes.data(), 0, schema.replFlags);

    array<uint8_t, 4> validFolderMaskBytes{};
    detail::writeU32(validFolderMaskBytes.data(), 0, schema.validFolderMask);

    array<uint8_t, 1> pidTag6633Bytes{ schema.pidTag6633Boolean };

    array<uint8_t, 4> pidTag66FABytes{};
    detail::writeU32(pidTag66FABytes.data(), 0, schema.pidTag66FAInteger32);

    array<uint8_t, 4> pstPasswordBytes{};
    detail::writeU32(pstPasswordBytes.data(), 0, schema.pstPassword);

    // ---- Property descriptor array ----
    PcProperty props[11] = {
        // PidTagReplVersionhistory (HN-stored, 24 B)
        { 0x0E34u, PropType::Binary,
          replVersion.data(), 24u, PropStorageHint::Auto },
        // PidTagReplFlags (inline)
        { 0x0E38u, PropType::Int32,
          replFlagsBytes.data(), 4u, PropStorageHint::Auto },
        // PidTagRecordKey (HN-stored, 16 B)
        { 0x0FF9u, PropType::Binary,
          schema.providerUid.data(), 16u, PropStorageHint::Auto },
        // PidTagDisplayName (HN-stored, variable)
        { 0x3001u, PropType::Unicode,
          schema.displayNameUtf16le, schema.displayNameSize,
          PropStorageHint::Auto },
        // PidTagValidFolderMask (inline)
        { 0x35DFu, PropType::Int32,
          validFolderMaskBytes.data(), 4u, PropStorageHint::Auto },
        // PidTagIpmSubTreeEntryId (HN-stored, 24 B)
        { 0x35E0u, PropType::Binary,
          subtreeEntryId.data(), 24u, PropStorageHint::Auto },
        // PidTagIpmWastebasketEntryId (HN-stored, 24 B)
        { 0x35E3u, PropType::Binary,
          wastebasketEid.data(), 24u, PropStorageHint::Auto },
        // PidTagFinderEntryId (HN-stored, 24 B)
        { 0x35E7u, PropType::Binary,
          finderEntryId.data(), 24u, PropStorageHint::Auto },
        // PidTag 0x6633 PtypBoolean (inline; §3.9 BTH extra)
        { 0x6633u, PropType::Boolean,
          pidTag6633Bytes.data(), 1u, PropStorageHint::Auto },
        // PidTag 0x66FA PtypInteger32 (inline; §3.9 BTH extra)
        { 0x66FAu, PropType::Int32,
          pidTag66FABytes.data(), 4u, PropStorageHint::Auto },
        // PidTagPstPassword (inline)
        { 0x67FFu, PropType::Int32,
          pstPasswordBytes.data(), 4u, PropStorageHint::Auto },
    };

    return buildPropertyContext(props, 11, firstSubnodeNid);
}

// ----------------------------------------------------------------------------
// buildFolderPc — 4-property PC for NORMAL_FOLDER nodes (§3.12 schema).
//
// Used for: Root Folder (NID 0x122), IPM SuBTree (0x8022), Finder (0x8042),
// Deleted Items (0x8062), and any caller-defined sub-folder.
// ----------------------------------------------------------------------------
PcResult buildFolderPc(const FolderPcSchema& schema,
                       Nid                   firstSubnodeNid)
{
    // Inline value buffers (LE-packed).
    array<uint8_t, 4> contentCountBytes{};
    detail::writeU32(contentCountBytes.data(), 0, schema.contentCount);

    array<uint8_t, 4> contentUnreadBytes{};
    detail::writeU32(contentUnreadBytes.data(), 0, schema.contentUnreadCount);

    array<uint8_t, 1> subfoldersBytes{
        static_cast<uint8_t>(schema.hasSubfolders ? 1u : 0u)
    };

    PcProperty props[4] = {
        // PidTagDisplayName (HN-stored, variable)
        { 0x3001u, PropType::Unicode,
          schema.displayNameUtf16le, schema.displayNameSize,
          PropStorageHint::Auto },
        // PidTagContentCount (inline)
        { 0x3602u, PropType::Int32,
          contentCountBytes.data(), 4u, PropStorageHint::Auto },
        // PidTagContentUnreadCount (inline)
        { 0x3603u, PropType::Int32,
          contentUnreadBytes.data(), 4u, PropStorageHint::Auto },
        // PidTagSubfolders (inline, Boolean — 1 byte zero-extended to 4)
        { 0x360Au, PropType::Boolean,
          subfoldersBytes.data(), 1u, PropStorageHint::Auto },
    };

    return buildPropertyContext(props, 4, firstSubnodeNid);
}

// ----------------------------------------------------------------------------
// buildFolderHierarchyTc — 13-column TC matching §3.12 Hierarchy schema.
// ----------------------------------------------------------------------------
namespace {

// §3.12 13-column schema (sorted by PidTag-tag ascending — buildTableContext
// re-sorts internally so caller order is irrelevant; we list in spec order
// for readability).
constexpr TcColumn kHierarchyCols[13] = {
    // PidTag, propType, ibData, cbData, iBit
    // M11-J: 0x0E30 is PtypBinary (PR_REPLICA_VERSION); was Int32
    // — scanpst flagged "missing required column (0E300102)" because
    // TCOLDESC.tag would have read 0x0E300003 instead of 0x0E300102.
    // Same cell size (4-byte HID slot), same row layout — only the
    // type bits in the tag change.
    { 0x0E30u, PropType::Binary,   20,  4,  6 },  // ReplItemid (Binary HID)
    { 0x0E33u, PropType::Int64,    24,  8,  7 },  // ReplChangenum (PtypInteger64 0x14)
    { 0x0E34u, PropType::Binary,   32,  4,  8 },  // ReplVersionhistory (HID)
    { 0x0E38u, PropType::Int32,    36,  4,  9 },  // ReplFlags
    { 0x3001u, PropType::Unicode,   8,  4,  2 },  // DisplayName_W (HID)
    { 0x3602u, PropType::Int32,    12,  4,  3 },  // ContentCount
    { 0x3603u, PropType::Int32,    16,  4,  4 },  // ContentUnreadCount
    { 0x360Au, PropType::Boolean,  52,  1,  5 },  // Subfolders
    { 0x3613u, PropType::Unicode,  40,  4, 10 },  // ContainerClass_W (HID)
    { 0x6635u, PropType::Int32,    44,  4, 11 },  // (PstHiddenCount)
    { 0x6636u, PropType::Int32,    48,  4, 12 },  // (PstHiddenUnread)
    { 0x67F2u, PropType::Int32,     0,  4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,     4,  4,  1 },  // LtpRowVer
};

// Per-row matrix size derived from rgib (53 fixed + 2 CEB = 55).
constexpr size_t kHierarchyRowSize = 55;
// CEB byte count = ceil(13 / 8) = 2.
constexpr size_t kHierarchyCebSize = 2;

// CEB bit pattern for a "present" row: which columns of the 13 are
// actually populated. Per §3.12 Row 0 (and the M6 default schema):
// LtpRowId(0), LtpRowVer(1), DisplayName_W(2), ContentCount(3),
// ContentUnreadCount(4), Subfolders(5), ReplChangenum(7) — 7 cells set.
// Bit layout: byte 0 high-bit-first for iBit 0..7; byte 1 high-bit-first
// for iBit 8..15.
//   byte 0: iBit 0,1,2,3,4,5,_,7 → 0b11111101 = 0xFD
//   byte 1: (iBit 8..12 all clear; iBit 13..15 unused) → 0x00
constexpr uint8_t kHierarchyCebByte0 = 0xFDu;
constexpr uint8_t kHierarchyCebByte1 = 0x00u;

} // namespace

TcResult buildFolderHierarchyTc(const HierarchyTcRow* rows, size_t rowCount)
{
    // Pre-pack each row's 55 bytes. Varlen DisplayName_W cells are slotted
    // separately as TcVarlenCell entries; the writer patches the row's
    // 4-byte HID slot at ibData=8 with the assigned HID.
    vector<array<uint8_t, kHierarchyRowSize>> rowBuffers(rowCount);
    vector<TcVarlenCell>                      varlenStore;
    varlenStore.reserve(rowCount);            // 1 varlen cell per row (DisplayName)

    // Per-row varlen cells need stable storage during the buildTableContext
    // call. We use a flat vector and per-row slices.
    vector<TcRow>          tcRows(rowCount);
    vector<vector<TcVarlenCell>> perRowVarlen(rowCount);

    for (size_t r = 0; r < rowCount; ++r) {
        const HierarchyTcRow& src = rows[r];
        uint8_t* dst = rowBuffers[r].data();
        std::memset(dst, 0, kHierarchyRowSize);

        // Bytes 0..3:   LtpRowId
        detail::writeU32(dst,  0, src.rowId.value);
        // Bytes 4..7:   LtpRowVer
        detail::writeU32(dst,  4, src.rowVer);
        // Bytes 8..11:  DisplayName_W HID (placeholder; writer patches)
        // Bytes 12..15: ContentCount
        detail::writeU32(dst, 12, src.contentCount);
        // Bytes 16..19: ContentUnreadCount
        detail::writeU32(dst, 16, src.contentUnreadCount);
        // Bytes 20..23: ReplItemid (zero, CEB clear)
        // Bytes 24..31: ReplChangenum (zero, CEB set per §3.12)
        // Bytes 32..35: ReplVersionhistory HID (zero, CEB clear)
        // Bytes 36..39: ReplFlags (zero, CEB clear)
        // Bytes 40..43: ContainerClass_W HID (zero, CEB clear)
        // Bytes 44..47: 0x6635 (zero, CEB clear)
        // Bytes 48..51: 0x6636 (zero, CEB clear)
        // Byte  52:     Subfolders
        dst[52] = src.hasSubfolders ? 1u : 0u;
        // Bytes 53..54: CEB
        dst[53] = kHierarchyCebByte0;
        dst[54] = kHierarchyCebByte1;

        // Find the column-index of DisplayName_W in our schema (its
        // colIndex is the position in kHierarchyCols, which is sorted by
        // tag ascending — DisplayName tag 0x3001001F is at index 4).
        constexpr size_t kDisplayNameColIdx = 4;

        if (src.displayNameUtf16le != nullptr && src.displayNameSize > 0) {
            perRowVarlen[r].push_back({ kDisplayNameColIdx,
                                        src.displayNameUtf16le,
                                        src.displayNameSize });
        }

        tcRows[r].rowId       = src.rowId.value;
        tcRows[r].rowBytes    = rowBuffers[r].data();
        tcRows[r].rowSize     = kHierarchyRowSize;
        tcRows[r].varlenCells = perRowVarlen[r].data();
        tcRows[r].varlenCount = perRowVarlen[r].size();
    }

    return buildTableContext(kHierarchyCols, 13,
                             tcRows.data(), rowCount);
}

// ----------------------------------------------------------------------------
// buildFolderContentsTc — 27-column Contents TC matching §3.12 schema.
// Always 0-row in M6.
// ----------------------------------------------------------------------------
namespace {

// 28-col schema (was 27) — adds PidTagChangeKey (0x3013) at iBit 27.
// scanpst flags 0x3013 as required Contents-TC column. The HID slot
// for ChangeKey takes the 4-byte slot at ibData=116 (formerly the
// MessageToMe boolean), pushing the booleans to 120/121 and CEB to 122.
// Per-row endBm = 126 (122 fixed + 4 CEB). [Tier 3 ISSUE H.]
constexpr TcColumn kContentsCols[28] = {
    // tag-sorted ascending
    { 0x0017u, PropType::Int32,      20, 4,  5 },  // Importance
    { 0x001Au, PropType::Unicode,    12, 4,  3 },  // MessageClass_W
    { 0x0036u, PropType::Int32,      60, 4, 15 },  // Sensitivity
    { 0x0037u, PropType::Unicode,    28, 4,  7 },  // Subject_W
    { 0x0039u, PropType::SystemTime, 40, 8,  9 },  // ClientSubmitTime
    { 0x0042u, PropType::Unicode,    24, 4,  6 },  // SentRepresentingName_W
    { 0x0057u, PropType::Boolean,   120, 1, 13 },  // MessageToMe (was 116)
    { 0x0058u, PropType::Boolean,   121, 1, 14 },  // MessageCcMe (was 117)
    { 0x0070u, PropType::Unicode,    68, 4, 17 },  // ConversationTopic_W
    { 0x0071u, PropType::Binary,     72, 4, 18 },  // ConversationIndex
    { 0x0E03u, PropType::Unicode,    56, 4, 12 },  // DisplayCc_W
    { 0x0E04u, PropType::Unicode,    52, 4, 11 },  // DisplayTo_W
    { 0x0E06u, PropType::SystemTime, 32, 8,  8 },  // MessageDeliveryTime
    { 0x0E07u, PropType::Int32,      16, 4,  4 },  // MessageFlags
    { 0x0E08u, PropType::Int32,      48, 4, 10 },  // MessageSize
    { 0x0E17u, PropType::Int32,       8, 4,  2 },  // MessageStatus
    // M11-J: 0x0E30 is PtypBinary, not Int32 (scanpst expects 0x0E300102).
    { 0x0E30u, PropType::Binary,     88, 4, 21 },  // ReplItemId (Binary HID)
    { 0x0E33u, PropType::Int64,      92, 8, 22 },  // ReplChangenum
    { 0x0E34u, PropType::Binary,    100, 4, 23 },  // ReplVersionhistory
    { 0x0E38u, PropType::Int32,     112, 4, 26 },  // ReplFlags
    { 0x0E3Cu, PropType::Binary,    108, 4, 25 },  // ReplCopiedfromVersionhistory
    { 0x0E3Du, PropType::Binary,    104, 4, 24 },  // ReplCopiedfromItemid
    { 0x1097u, PropType::Int32,      64, 4, 16 },  // ItemTemporaryFlags
    { 0x3008u, PropType::SystemTime, 80, 8, 20 },  // LastModificationTime
    { 0x3013u, PropType::Binary,    116, 4, 27 },  // ChangeKey (NEW: Tier 3 H)
    { 0x65C6u, PropType::Int32,      76, 4, 19 },  // SecureSubmitFlags
    { 0x67F2u, PropType::Int32,       0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,       4, 4,  1 },  // LtpRowVer
};

constexpr TcColumn kFaiContentsCols[17] = {
    // tag-sorted ascending
    { 0x001Au, PropType::Unicode,    12, 4,  3 },  // MessageClass_W
    { 0x003Au, PropType::Unicode,    60, 4, 16 },  // ReportName_W
    { 0x0070u, PropType::Unicode,    56, 4, 15 },  // ConversationTopic_W
    { 0x0E07u, PropType::Int32,      16, 4,  4 },  // MessageFlags
    { 0x0E17u, PropType::Int32,       8, 4,  2 },  // MessageStatus
    { 0x3001u, PropType::Unicode,    20, 4,  5 },  // DisplayName_W
    { 0x67F2u, PropType::Int32,       0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,       4, 4,  1 },  // LtpRowVer
    { 0x6800u, PropType::Unicode,    44, 4, 11 },  // MapiformMessageclass_W (alias)
    { 0x6803u, PropType::Boolean,    64, 1, 12 },  // FormMultCategorized (alias)
    { 0x6805u, PropType::MvInt32,    48, 4, 13 },  // OfflineAddressBookTruncatedProperties
    { 0x682Fu, PropType::Unicode,    52, 4, 14 },  // ReplItemid (Unicode in FAI per §3.12)
    { 0x7003u, PropType::Int32,      24, 4,  6 },  // ViewDescriptorFlags
    { 0x7004u, PropType::Binary,     28, 4,  7 },  // ViewDescriptorLinkTo
    { 0x7005u, PropType::Binary,     32, 4,  8 },  // ViewDescriptorViewFolder
    { 0x7006u, PropType::Unicode,    36, 4,  9 },  // ViewDescriptorName_W
    { 0x7007u, PropType::Int32,      40, 4, 10 },  // ViewDescriptorVersion
};

} // namespace

TcResult buildFolderContentsTc()
{
    return buildTableContext(kContentsCols, 28, nullptr, 0);
}

namespace {

// Per-row matrix size derived from the 28-col Contents schema (M11-I):
// 122 bytes of fixed data + 4-byte CEB = 126.
constexpr size_t kContentsRowSize = 126;
constexpr size_t kContentsCebOff  = 122;
constexpr size_t kContentsCebSize = 4;

// colIndex of each column in the tag-sorted kContentsCols[] schema.
// (Matches the order kContentsCols is declared.) Used to address
// varlen cells via TcVarlenCell.colIndex.
constexpr size_t kColImportance              = 0;
constexpr size_t kColMessageClass            = 1;
constexpr size_t kColSensitivity             = 2;
constexpr size_t kColSubject                 = 3;
constexpr size_t kColClientSubmitTime        = 4;
constexpr size_t kColSentRepresentingName    = 5;
constexpr size_t kColMessageToMe             = 6;
constexpr size_t kColMessageCcMe             = 7;
constexpr size_t kColConversationTopic       = 8;
constexpr size_t kColConversationIndex       = 9;
constexpr size_t kColDisplayCc               = 10;
constexpr size_t kColDisplayTo               = 11;
constexpr size_t kColMessageDeliveryTime     = 12;
constexpr size_t kColMessageFlags            = 13;
constexpr size_t kColMessageSize             = 14;
constexpr size_t kColMessageStatus           = 15;
constexpr size_t kColLastModificationTime    = 23;
constexpr size_t kColChangeKey               = 24;  // M11-I: 0x3013 (Binary)
constexpr size_t kColLtpRowId                = 26;
constexpr size_t kColLtpRowVer               = 27;

// Set the CEB bit for `iBit` (high-bit-first byte numbering — bit 0
// of byte 0 is bit 7 of CEB[0], same convention as the Hierarchy TC).
inline void setCebBit(uint8_t* ceb, unsigned iBit) noexcept
{
    const unsigned byteIdx = iBit / 8u;
    const unsigned bitInByte = 7u - (iBit % 8u);
    ceb[byteIdx] |= static_cast<uint8_t>(1u << bitInByte);
}

} // namespace

TcResult buildFolderContentsTc(const ContentsTcRow* rows, size_t rowCount,
                               Nid firstSubnodeNid)
{
    if (rowCount == 0u) {
        return buildTableContext(kContentsCols, 28, nullptr, 0);
    }

    // Stable per-row buffers + varlen-cell descriptors. Pointers into
    // these vectors must stay valid until buildTableContext returns;
    // we reserve up front so push_back never reallocates.
    vector<array<uint8_t, kContentsRowSize>> rowBuffers(rowCount);
    vector<vector<TcVarlenCell>>             perRowVarlen(rowCount);
    vector<TcRow>                            tcRows(rowCount);

    for (size_t r = 0; r < rowCount; ++r) {
        const ContentsTcRow& src = rows[r];
        uint8_t* dst = rowBuffers[r].data();
        std::memset(dst, 0, kContentsRowSize);

        uint8_t* ceb = dst + kContentsCebOff;

        // ---- Fixed Int32 / SystemTime / Boolean cells ----
        // ibData / iBit values lifted verbatim from kContentsCols above.

        // LtpRowId @ ibData=0, iBit=0 — always present.
        detail::writeU32(dst, 0, src.rowId.value);
        setCebBit(ceb, 0);

        // LtpRowVer @ ibData=4, iBit=1 — always present.
        detail::writeU32(dst, 4, src.rowVer);
        setCebBit(ceb, 1);

        // MessageStatus @ ibData=8, iBit=2 — always emit (default 0).
        detail::writeU32(dst, 8, static_cast<uint32_t>(src.messageStatus));
        setCebBit(ceb, 2);

        // MessageFlags @ ibData=16, iBit=4 — always.
        detail::writeU32(dst, 16, static_cast<uint32_t>(src.messageFlags));
        setCebBit(ceb, 4);

        // Importance @ ibData=20, iBit=5 — always.
        detail::writeU32(dst, 20, static_cast<uint32_t>(src.importance));
        setCebBit(ceb, 5);

        // MessageDeliveryTime @ ibData=32, iBit=8 — when non-zero.
        if (src.messageDeliveryTime != 0u) {
            detail::writeU64(dst, 32, src.messageDeliveryTime);
            setCebBit(ceb, 8);
        }

        // ClientSubmitTime @ ibData=40, iBit=9 — when non-zero.
        if (src.clientSubmitTime != 0u) {
            detail::writeU64(dst, 40, src.clientSubmitTime);
            setCebBit(ceb, 9);
        }

        // MessageSize @ ibData=48, iBit=10 — always.
        detail::writeU32(dst, 48, static_cast<uint32_t>(src.messageSize));
        setCebBit(ceb, 10);

        // Sensitivity @ ibData=60, iBit=15 — always.
        detail::writeU32(dst, 60, static_cast<uint32_t>(src.sensitivity));
        setCebBit(ceb, 15);

        // LastModificationTime @ ibData=80, iBit=20 — when non-zero.
        if (src.lastModificationTime != 0u) {
            detail::writeU64(dst, 80, src.lastModificationTime);
            setCebBit(ceb, 20);
        }

        // MessageToMe @ ibData=120, iBit=13 / MessageCcMe @ 121, iBit=14
        // — always emit the boolean (0 = false). [M11-I: shifted from
        // 116/117 to make room for ChangeKey HID at 116..119.]
        dst[120] = src.messageToMe ? 1u : 0u;
        setCebBit(ceb, 13);
        dst[121] = src.messageCcMe ? 1u : 0u;
        setCebBit(ceb, 14);

        // ---- Varlen cells ----
        // The HID slot at each varlen column's ibData is left as zero;
        // buildTableContext patches it with the assigned HID after
        // allocating the underlying HN range.
        auto pushVarlen = [&](size_t colIdx, unsigned iBit,
                              const uint8_t* bytes, size_t size) {
            if (bytes == nullptr || size == 0u) return;
            perRowVarlen[r].push_back({ colIdx, bytes, size });
            setCebBit(ceb, iBit);
        };

        // MessageClass_W (colIdx=1, ibData=12, iBit=3)
        pushVarlen(kColMessageClass, 3,
                   src.messageClassUtf16le, src.messageClassSize);
        // SentRepresentingName_W (colIdx=5, ibData=24, iBit=6)
        pushVarlen(kColSentRepresentingName, 6,
                   src.sentRepresentingNameUtf16le,
                   src.sentRepresentingNameSize);
        // Subject_W (colIdx=3, ibData=28, iBit=7)
        pushVarlen(kColSubject, 7, src.subjectUtf16le, src.subjectSize);
        // DisplayTo_W (colIdx=11, ibData=52, iBit=11)
        pushVarlen(kColDisplayTo, 11, src.displayToUtf16le, src.displayToSize);
        // DisplayCc_W (colIdx=10, ibData=56, iBit=12)
        pushVarlen(kColDisplayCc, 12, src.displayCcUtf16le, src.displayCcSize);
        // ConversationTopic_W (colIdx=8, ibData=68, iBit=17)
        pushVarlen(kColConversationTopic, 17,
                   src.conversationTopicUtf16le,
                   src.conversationTopicSize);
        // ConversationIndex (colIdx=9, ibData=72, iBit=18 — Binary)
        pushVarlen(kColConversationIndex, 18,
                   src.conversationIndexBytes,
                   src.conversationIndexSize);
        // ChangeKey (colIdx=24, ibData=116, iBit=27 — Binary, M11-I)
        pushVarlen(kColChangeKey, 27,
                   src.changeKeyBytes, src.changeKeySize);

        tcRows[r].rowId       = src.rowId.value;
        tcRows[r].rowBytes    = rowBuffers[r].data();
        tcRows[r].rowSize     = kContentsRowSize;
        tcRows[r].varlenCells = perRowVarlen[r].empty()
                                  ? nullptr : perRowVarlen[r].data();
        tcRows[r].varlenCount = perRowVarlen[r].size();
    }

    return buildTableContext(kContentsCols, 28, tcRows.data(), rowCount,
                             firstSubnodeNid);
}

TcResult buildFolderFaiContentsTc()
{
    return buildTableContext(kFaiContentsCols, 17, nullptr, 0);
}

// ----------------------------------------------------------------------------
// buildNameToIdMapPc — empty Name-to-ID Map per §2.4.7 + §2.7.1.
//
// M11-K P5: scanpst flagged invalid HNIDs (0x60/0x80/0xA0) on the
// three stream properties because we emitted them with 0-byte
// allocations. Outlook's parser rejects zero-length HN allocations
// referenced by a property HNID — the slot exists in rgibAlloc but
// the parser expects at least 1 byte of content. Emit 4-byte stub
// values (DWORD-aligned, single zero entry) so each HID resolves to
// a non-empty allocation.
//
// Per [MS-PST] §2.4.7:
//   * NameidBucketCount: 251 (SHOULD value for hash bucketing)
//   * NameidStreamGuid:  16 B per GUID entry — empty map = 0 entries,
//                        but we emit 16 zero bytes as a stub GUID so
//                        the stream allocation is non-empty.
//   * NameidStreamEntry: 8 B per name-id entry — empty map = 0 entries;
//                        emit 8 zero bytes (1 stub entry that bucket-
//                        count of 251 will never select).
//   * NameidStreamString: variable; 4 zero bytes is enough to satisfy
//                         scanpst's "non-empty" requirement.
// ----------------------------------------------------------------------------
PcResult buildNameToIdMapPc(Nid firstSubnodeNid)
{
    // PidTagNameidBucketCount: 251 (the "SHOULD" value per §2.4.7).
    array<uint8_t, 4> bucketCountBytes{};
    detail::writeU32(bucketCountBytes.data(), 0, 251u);

    // M11-K P5: non-empty stub buffers so scanpst sees valid HNIDs.
    // 16/8/4 byte allocations; content all zero (empty-map placeholder).
    static constexpr array<uint8_t, 16> kStreamGuidStub{};
    static constexpr array<uint8_t,  8> kStreamEntryStub{};
    static constexpr array<uint8_t,  4> kStreamStringStub{};

    PcProperty props[4] = {
        // 0x00010003 PidTagNameidBucketCount (inline, Int32)
        { 0x0001u, PropType::Int32,
          bucketCountBytes.data(), 4u, PropStorageHint::Auto },
        // 0x00020102 PidTagNameidStreamGuid (HN-stored Binary, 16 B stub)
        { 0x0002u, PropType::Binary,
          kStreamGuidStub.data(), kStreamGuidStub.size(),
          PropStorageHint::Auto },
        // 0x00030102 PidTagNameidStreamEntry (HN-stored Binary, 8 B stub)
        { 0x0003u, PropType::Binary,
          kStreamEntryStub.data(), kStreamEntryStub.size(),
          PropStorageHint::Auto },
        // 0x00040102 PidTagNameidStreamString (HN-stored Binary, 4 B stub)
        { 0x0004u, PropType::Binary,
          kStreamStringStub.data(), kStreamStringStub.size(),
          PropStorageHint::Auto },
    };

    return buildPropertyContext(props, 4, firstSubnodeNid);
}

// ----------------------------------------------------------------------------
// buildRecipientTemplateTc — 14-column TC per [MS-PST] Recipient Template.
//
// Layout: LtpRowId at 0, LtpRowVer at 4, then 4-byte cols (incl. HID slots),
// then 1-byte cols. iBit assignments place LtpRowId/Ver at 0/1 then
// remaining cols by tag-ascending. Per-row endBm = 52.
// ----------------------------------------------------------------------------
namespace {

constexpr TcColumn kRecipientCols[14] = {
    // tag-sorted ascending; ibData chosen for compact 4B-then-1B layout
    { 0x0C15u, PropType::Int32,    8, 4,  2 },  // RecipientType
    { 0x0E0Fu, PropType::Boolean, 48, 1,  3 },  // Responsibility
    { 0x0FF9u, PropType::Binary,  12, 4,  4 },  // RecordKey (HID)
    { 0x0FFEu, PropType::Int32,   16, 4,  5 },  // ObjectType
    { 0x0FFFu, PropType::Binary,  20, 4,  6 },  // EntryId (HID)
    { 0x3001u, PropType::Unicode, 24, 4,  7 },  // DisplayName (HID)
    { 0x3002u, PropType::Unicode, 28, 4,  8 },  // AddressType (HID)
    { 0x3003u, PropType::Unicode, 32, 4,  9 },  // EmailAddress (HID)
    { 0x300Bu, PropType::Binary,  36, 4, 10 },  // SearchKey (HID)
    { 0x3900u, PropType::Int32,   40, 4, 11 },  // DisplayType
    { 0x39FFu, PropType::Unicode, 44, 4, 12 },  // 7BitDisplayName (HID)
    { 0x3A40u, PropType::Boolean, 49, 1, 13 },  // SendRichInfo
    { 0x67F2u, PropType::Int32,    0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,    4, 4,  1 },  // LtpRowVer
};

constexpr TcColumn kAttachmentCols[6] = {
    { 0x0E20u, PropType::Int32,    8, 4,  2 },  // AttachSize
    { 0x3704u, PropType::Unicode, 12, 4,  3 },  // AttachFilenameW (HID)
    { 0x3705u, PropType::Int32,   16, 4,  4 },  // AttachMethod
    { 0x370Bu, PropType::Int32,   20, 4,  5 },  // RenderingPosition
    { 0x67F2u, PropType::Int32,    0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,    4, 4,  1 },  // LtpRowVer
};

} // namespace

TcResult buildRecipientTemplateTc()
{
    return buildTableContext(kRecipientCols, 14, nullptr, 0);
}

TcResult buildAttachmentTemplateTc()
{
    return buildTableContext(kAttachmentCols, 6, nullptr, 0);
}

// ----------------------------------------------------------------------------
// buildReceiveFolderTableTc — NID 0x0617 Receive Folder Table.
//
// Per [MS-OXCSTOR] §2.2.4 + [MS-PST] §2.4.5, the Receive Folder Table
// maps message classes to receiving folders. Required schema:
//   0x001A001F  PidTagMessageClass_W      Unicode  (key column)
//   0x36670102  PidTagReceiveFolderID     Binary 24 B (folder ENTRYID)
//   0x30080040  PidTagLastModificationTime SystemTime
//   0x67F2/3    LtpRowId / LtpRowVer (mandatory in every TC)
//
// M11-J P5 schema was wrong (used LtpRowId-as-NID + DisplayName);
// scanpst still flagged "Receive folder table missing default message
// class". M11-K P3: rebuild with the correct columns AND emit one
// row mapping default class "" → ENTRYID(IPM Subtree NID 0x8022).
//
// ENTRYID format: 4-byte rgbFlags + 16-byte ProviderUID + 4-byte NID,
// = 24 bytes total per [MS-OXCDATA] §2.2.4.2 / [MS-PST] §3.10 sample.
// ----------------------------------------------------------------------------
namespace {

constexpr TcColumn kReceiveFolderCols[5] = {
    // tag-sorted ascending; 4-byte cells [0..15], CEB at 16. endBm=17.
    { 0x001Au, PropType::Unicode,    8, 4,  2 },  // MessageClass_W (HID; key column)
    { 0x3008u, PropType::SystemTime, 16, 8,  4 }, // LastModificationTime (8 B)
    { 0x3667u, PropType::Binary,    12, 4,  3 },  // ReceiveFolderID (HID; ENTRYID)
    { 0x67F2u, PropType::Int32,      0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,      4, 4,  1 },  // LtpRowVer
};
// 24 bytes fixed (4+4+4+4+8) + 1 CEB byte = 25.
constexpr size_t kReceiveFolderRowSize = 25;
constexpr size_t kReceiveFolderCebOff  = 24;

// Default ProviderUID — used when the writer doesn't have access to
// the message-store's actual ProviderUID for ENTRYID construction.
// Real-Outlook later rewrites this row when it mounts the PST.
constexpr std::array<uint8_t, 16> kDefaultRftProviderUid = {
    0x22u, 0x9Du, 0xB5u, 0x0Au, 0xDCu, 0xD9u, 0x94u, 0x43u,
    0x85u, 0xDEu, 0x90u, 0xAEu, 0xB0u, 0x7Du, 0x12u, 0x70u
};

} // namespace

TcResult buildReceiveFolderTableTc()
{
    // Build the ENTRYID for IPM Subtree (NID 0x8022) — the default
    // recipient for messages with no specific class match.
    static const auto inboxEntryId =
        makeEntryId(kDefaultRftProviderUid, Nid{0x00008022u}, 0u);

    array<uint8_t, kReceiveFolderRowSize> row{};
    detail::writeU32(row.data(), 0, 1u);         // LtpRowId (1 = first row)
    detail::writeU32(row.data(), 4, 1u);         // LtpRowVer
    // Bytes 8..11: MessageClass_W HID (zero — buildTableContext patches
    //              with the assigned HID for the empty-string varlen)
    // Bytes 12..15: ReceiveFolderID HID (zero — patched with ENTRYID HID)
    // Bytes 16..23: LastModificationTime (zero = absent, CEB clear)
    // Byte 24: CEB. iBit 0,1,3 set (LtpRowId, LtpRowVer, ReceiveFolderID).
    // MessageClass iBit=2 — empty string, but we still set CEB bit so
    // scanpst sees the "default class" row. iBit 4 (LastModTime) clear.
    // High-bit-first byte 0: iBits 0,1,2,3 → 0b11110000 = 0xF0.
    row[kReceiveFolderCebOff] = 0xF0u;

    // MessageClass is the empty string ("" = 0 bytes UTF-16-LE);
    // ReceiveFolderID is the 24-byte ENTRYID for IPM Subtree.
    TcVarlenCell varlen[1];
    varlen[0].colIndex = 2;  // index 2 = 0x3667 ReceiveFolderID in tag-sorted array
    varlen[0].bytes    = inboxEntryId.data();
    varlen[0].size     = inboxEntryId.size();

    TcRow tcRow{};
    tcRow.rowId       = 1u;
    tcRow.rowBytes    = row.data();
    tcRow.rowSize     = kReceiveFolderRowSize;
    tcRow.varlenCells = varlen;
    tcRow.varlenCount = 1;

    return buildTableContext(kReceiveFolderCols, 5, &tcRow, 1);
}

// ----------------------------------------------------------------------------
// buildSearchContentsTemplateTc — NID 0x610 Outgoing/Search Contents template.
//
// scanpst's NID 0x610 schema requires 17 columns per [MS-OXOSRCH] /
// [MS-OXOMSG]. Earlier (M11-I) we delegated to buildFolderContentsTc,
// but Contents has 28 columns (replication/retention) that don't
// belong here, AND was missing 3 required ones:
//   0x67F1  PR_PF_PROXY              Int32
//   0x0E05  PR_PARENT_ENTRYID_W      Unicode (HID)  — NID 0x610 specific
//   0x0E2A  PR_HASATTACH             Boolean
//
// M11-K P4: dedicated 19-column schema (17 required + LtpRowId/Ver).
// ----------------------------------------------------------------------------
namespace {

constexpr TcColumn kSearchContentsCols[20] = {
    // tag-sorted ascending; 4-byte/8-byte cells in [0..71], 1-byte in
    // [72..74], CEB (3 bytes for 20 iBits) at 75. endBm = 78.
    { 0x0017u, PropType::Int32,      20, 4,  5 },  // Importance
    { 0x001Au, PropType::Unicode,    12, 4,  3 },  // MessageClass_W
    { 0x0036u, PropType::Int32,      60, 4, 15 },  // Sensitivity
    { 0x0037u, PropType::Unicode,    28, 4,  7 },  // Subject_W
    { 0x0039u, PropType::SystemTime, 40, 8,  9 },  // ClientSubmitTime
    { 0x0042u, PropType::Unicode,    24, 4,  6 },  // SentRepresentingName_W
    { 0x0057u, PropType::Boolean,    72, 1, 13 },  // MessageToMe
    { 0x0058u, PropType::Boolean,    73, 1, 14 },  // MessageCcMe
    { 0x0E03u, PropType::Unicode,    56, 4, 12 },  // DisplayCc_W
    { 0x0E04u, PropType::Unicode,    52, 4, 11 },  // DisplayTo_W
    { 0x0E05u, PropType::Unicode,    64, 4, 16 },  // ParentEntryID_W (M11-K)
    { 0x0E06u, PropType::SystemTime, 32, 8,  8 },  // MessageDeliveryTime
    { 0x0E07u, PropType::Int32,      16, 4,  4 },  // MessageFlags
    { 0x0E08u, PropType::Int32,      48, 4, 10 },  // MessageSize
    { 0x0E17u, PropType::Int32,       8, 4,  2 },  // MessageStatus
    { 0x0E2Au, PropType::Boolean,    74, 1, 17 },  // HasAttach (M11-K)
    { 0x3008u, PropType::SystemTime, 76, 8, 18 },  // LastModTime
    { 0x67F1u, PropType::Int32,      68, 4, 19 },  // PfProxy (M11-K)
    { 0x67F2u, PropType::Int32,       0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,       4, 4,  1 },  // LtpRowVer
};

} // namespace

TcResult buildSearchContentsTemplateTc()
{
    // Schema declared above; LtpRowVer omitted to keep iBit count at 19
    // (≤ 24 for 3-byte CEB). Rows always 0 in M6 baseline — this is a
    // template TC for Outlook to clone when a search folder is created.
    return buildTableContext(kSearchContentsCols,
                             sizeof(kSearchContentsCols) / sizeof(kSearchContentsCols[0]),
                             nullptr, 0);
}

// ----------------------------------------------------------------------------
// buildSearchFolderPc — best-guess; reuses regular FolderPc schema.
// KNOWN_UNVERIFIED: spec doesn't pin Search Folder PC's exact property set.
// ----------------------------------------------------------------------------
PcResult buildSearchFolderPc(const FolderPcSchema& schema,
                             Nid                   firstSubnodeNid)
{
    return buildFolderPc(schema, firstSubnodeNid);
}

// ----------------------------------------------------------------------------
// buildEmptyNodePayload — 4 zero bytes for bare-node "Empty" §2.7.1 entries.
// ----------------------------------------------------------------------------
vector<uint8_t> buildEmptyNodePayload()
{
    return vector<uint8_t>(4u, 0u);
}

// ----------------------------------------------------------------------------
// writeM6Pst — Phase D end-to-end PST writer.
//
// Assembles the 27 §2.7.1 mandatory nodes and dispatches to writeM5Pst.
// Internally:
//   1. Builds each node's HN body / payload.
//   2. Allocates Bid::makeData(i+1) per block.
//   3. Wraps each in buildDataBlock at sequential 64-byte-aligned IBs.
//   4. Composes M5DataBlockSpec + M5Node lists with proper nidParent wiring.
//   5. Calls writeM5Pst.
// ----------------------------------------------------------------------------
namespace {

// Encode an ASCII C-string as UTF-16-LE bytes (no terminator, no BOM).
// Used for default folder display names.
vector<uint8_t> utf16leAscii(const char* s)
{
    vector<uint8_t> out;
    while (*s) {
        out.push_back(static_cast<uint8_t>(*s));
        out.push_back(0u);
        ++s;
    }
    return out;
}

// One node's logical body + wiring metadata.
struct M6NodeBuild {
    Nid             nid;
    Nid             nidParent;
    vector<uint8_t> body;   // HN bytes (PC/TC) or raw payload (bare node)
};

} // namespace

WriteResult writeM6Pst(const M6PstConfig& config) noexcept
{
    try {
        // Default UTF-16-LE display names — caller knobs added later as M6
        // gains config surface.
        const auto nameTopOfPersonal = utf16leAscii("Top of Personal Folders");
        const auto nameSearchRoot    = utf16leAscii("Search Root");
        const auto nameSpamSearch    = utf16leAscii("Spam Search Folder");
        const auto nameDeletedItems  = utf16leAscii("Deleted Items");

        // firstSubnodeNid required by buildPropertyContext but unused for
        // M6 schemas (no oversize props promote to subnode). Use a non-HID
        // NID outside the §2.4.1 reserved set.
        const Nid kDummySub{0x00000041u};

        vector<M6NodeBuild> nodes;
        nodes.reserve(27);

        // ---- Helper to append a PC node ----
        auto pushPc = [&](Nid nid, Nid parent, PcResult&& r) {
            if (!r.subnodes.empty()) {
                throw std::logic_error(
                    "writeM6Pst: M6 PC unexpectedly produced subnodes");
            }
            nodes.push_back({ nid, parent, std::move(r.hnBytes) });
        };
        auto pushTc = [&](Nid nid, Nid parent, TcResult&& r) {
            nodes.push_back({ nid, parent, std::move(r.hnBytes) });
        };

        // ---- 1. Message Store PC (0x21) ----
        MessageStoreSchema mss{};
        mss.providerUid = config.providerUid;
        pushPc(Nid{0x00000021u}, Nid{0u}, buildMessageStorePc(mss, kDummySub));

        // ---- 2. NameToIdMap PC (0x61) ----
        pushPc(Nid{0x00000061u}, Nid{0u}, buildNameToIdMapPc(kDummySub));

        // ---- 3. Root Folder PC (0x122; nidParent = self) ----
        FolderPcSchema rootSchema{};
        rootSchema.hasSubfolders = true;
        pushPc(Nid{0x00000122u}, Nid{0x00000122u},
               buildFolderPc(rootSchema, kDummySub));

        // ---- 4. Root Folder Hierarchy TC (0x12D) — 3 rows per §3.12 ----
        HierarchyTcRow rootHier[3];
        rootHier[0].rowId              = Nid{0x00002223u};
        rootHier[0].displayNameUtf16le = nameSpamSearch.data();
        rootHier[0].displayNameSize    = nameSpamSearch.size();
        rootHier[0].hasSubfolders      = false;
        rootHier[1].rowId              = Nid{0x00008022u};
        rootHier[1].displayNameUtf16le = nameTopOfPersonal.data();
        rootHier[1].displayNameSize    = nameTopOfPersonal.size();
        rootHier[1].hasSubfolders      = true;   // IPM contains Deleted Items
        rootHier[2].rowId              = Nid{0x00008042u};
        rootHier[2].displayNameUtf16le = nameSearchRoot.data();
        rootHier[2].displayNameSize    = nameSearchRoot.size();
        rootHier[2].hasSubfolders      = false;
        pushTc(Nid{0x0000012Du}, Nid{0u}, buildFolderHierarchyTc(rootHier, 3));

        // ---- 5. Root Folder Contents TC (0x12E) ----
        pushTc(Nid{0x0000012Eu}, Nid{0u}, buildFolderContentsTc());
        // ---- 6. Root Folder FAI Contents TC (0x12F) ----
        pushTc(Nid{0x0000012Fu}, Nid{0u}, buildFolderFaiContentsTc());

        // ---- 7. SearchManagementQueue (bare, 0x1E1) ----
        nodes.push_back({ Nid{0x000001E1u}, Nid{0u}, buildEmptyNodePayload() });
        // ---- 8. SearchActivityList (bare, 0x201) ----
        nodes.push_back({ Nid{0x00000201u}, Nid{0u}, buildEmptyNodePayload() });

        // ---- 9-12. Templates ----
        pushTc(Nid{0x0000060Du}, Nid{0u}, buildFolderHierarchyTc(nullptr, 0));
        pushTc(Nid{0x0000060Eu}, Nid{0u}, buildFolderContentsTc());
        pushTc(Nid{0x0000060Fu}, Nid{0u}, buildFolderFaiContentsTc());
        pushTc(Nid{0x00000610u}, Nid{0u}, buildSearchContentsTemplateTc());

        // ---- 13. Attachment Template (0x671) ----
        pushTc(Nid{0x00000671u}, Nid{0u}, buildAttachmentTemplateTc());
        // ---- 14. Recipient Template (0x692) ----
        pushTc(Nid{0x00000692u}, Nid{0u}, buildRecipientTemplateTc());

        // ---- 15. Spam Search Folder PC (0x2223; parent = Root) ----
        FolderPcSchema spamSchema{};
        spamSchema.displayNameUtf16le = nameSpamSearch.data();
        spamSchema.displayNameSize    = nameSpamSearch.size();
        pushPc(Nid{0x00002223u}, Nid{0x00000122u},
               buildSearchFolderPc(spamSchema, kDummySub));

        // ---- 16-19. IPM Subtree (0x8022; parent = Root) + tables ----
        FolderPcSchema ipmSchema{};
        ipmSchema.displayNameUtf16le = nameTopOfPersonal.data();
        ipmSchema.displayNameSize    = nameTopOfPersonal.size();
        ipmSchema.hasSubfolders      = true;   // contains Deleted Items
        pushPc(Nid{0x00008022u}, Nid{0x00000122u},
               buildFolderPc(ipmSchema, kDummySub));
        pushTc(Nid{0x0000802Du}, Nid{0u}, buildFolderHierarchyTc(nullptr, 0));
        pushTc(Nid{0x0000802Eu}, Nid{0u}, buildFolderContentsTc());
        pushTc(Nid{0x0000802Fu}, Nid{0u}, buildFolderFaiContentsTc());

        // ---- 20-23. Search Root / Finder (0x8042; parent = Root) + tables ----
        FolderPcSchema finderSchema{};
        finderSchema.displayNameUtf16le = nameSearchRoot.data();
        finderSchema.displayNameSize    = nameSearchRoot.size();
        pushPc(Nid{0x00008042u}, Nid{0x00000122u},
               buildFolderPc(finderSchema, kDummySub));
        pushTc(Nid{0x0000804Du}, Nid{0u}, buildFolderHierarchyTc(nullptr, 0));
        pushTc(Nid{0x0000804Eu}, Nid{0u}, buildFolderContentsTc());
        pushTc(Nid{0x0000804Fu}, Nid{0u}, buildFolderFaiContentsTc());

        // ---- 24-27. Deleted Items (0x8062; parent = IPM Subtree) + tables ----
        FolderPcSchema deletedSchema{};
        deletedSchema.displayNameUtf16le = nameDeletedItems.data();
        deletedSchema.displayNameSize    = nameDeletedItems.size();
        pushPc(Nid{0x00008062u}, Nid{0x00008022u},
               buildFolderPc(deletedSchema, kDummySub));
        pushTc(Nid{0x0000806Du}, Nid{0u}, buildFolderHierarchyTc(nullptr, 0));
        pushTc(Nid{0x0000806Eu}, Nid{0u}, buildFolderContentsTc());
        pushTc(Nid{0x0000806Fu}, Nid{0u}, buildFolderFaiContentsTc());

        if (nodes.size() != 27u) {
            return { false, "writeM6Pst: internal — expected 27 nodes" };
        }

        // ---- Encode each node's payload as a data block ----
        // Block layout starts at 0x4600 (matches writeM5Pst's expectation:
        // kIbAMap=0x4400 + 0x200 AMap page, M11-G).
        // BIDs assigned sequentially as Bid::makeData(i+1).
        constexpr uint64_t kBlocksStart = 0x4600u;
        vector<M5DataBlockSpec> blocks;
        vector<M5Node>          m5nodes;
        blocks.reserve(27);
        m5nodes.reserve(27);

        uint64_t cursorIb = kBlocksStart;
        for (size_t i = 0; i < nodes.size(); ++i) {
            const Bid bid = Bid::makeData(static_cast<uint64_t>(i + 1));
            const auto encoded = buildDataBlock(
                nodes[i].body.data(), nodes[i].body.size(),
                bid, Ib{cursorIb}, CryptMethod::Permute);
            blocks.push_back({ bid, encoded,
                               static_cast<uint16_t>(nodes[i].body.size()) });
            m5nodes.push_back({ nodes[i].nid, bid, Bid{0u}, nodes[i].nidParent });
            cursorIb += encoded.size();
        }

        return writeM5Pst(config.path, blocks, m5nodes);
    } catch (const std::exception& e) {
        return { false, std::string("writeM6Pst: ") + e.what() };
    } catch (...) {
        return { false, "writeM6Pst: unknown exception" };
    }
}

} // namespace pstwriter
