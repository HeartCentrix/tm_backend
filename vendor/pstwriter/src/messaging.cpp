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
#include "graph_convert.hpp"  // graph::deriveMessageSearchKey
#include "ltp.hpp"
#include "mail.hpp"            // M7FolderSchema + buildFolderPcExtended decl
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
// buildFolderPcExtended — 14-property folder PC envelope used by the three
// per-content-type wrappers (buildMailFolderPc / buildCalendarFolderPc /
// buildContactFolderPc). Universal across mail / calendar / contacts; the
// only per-type variation is the PidTagContainerClass bytes the caller
// passes in.
//
// History: M12.6 (2026-05-12) added 9 Outlook-canonical properties after
// byte-diff against a real Outlook-exported PST showed the prior 5-prop
// folders were silently rejected by Outlook's Import wizard. M12.7
// (2026-05-12) split the single mail-shaped builder into per-content-type
// wrappers (one per cpp), with this universal envelope living here.
//
// Properties emitted (all stub values; Outlook overwrites at runtime):
//   0x3001 PidTagDisplayName_W         — UTF-16 folder name
//   0x7B9E PR_CONTAINER_HIERARCHY      — display name (sort key)
//   0x3602 PidTagContentCount          — message count
//   0x3603 PidTagContentUnreadCount    — unread count
//   0x360A PidTagSubfolders            — hasSubfolders boolean
//   0x3613 PidTagContainerClass_W      — "IPF.Note" / "IPF.Appointment" /
//                                         "IPF.Contact" (per-type bytes)
//   0x3007 PR_CREATION_TIME            — 8B FILETIME stub
//   0x3008 PR_LAST_MODIFICATION_TIME   — 8B FILETIME stub
//   0x300B PR_SEARCH_KEY               — 16-byte hash of display name
//   0x3900 PR_DISPLAY_TYPE             — 0x01000000 (DT_FOLDER)
//   0x123A (unnamed)                   — 2 (folder type marker)
//   0x301D (unnamed)                   — 128 (0x80 folder flag)
//   0x7C0E PR_PST_FOLDER_DESIGN_ID     — 3749765689 (REF value)
//   0x7C0F PR_PST_FOLDER_DESIGN_CLS    — 6 (REF value)
//   0x6635/0x6636 PR_PstHidden{Count,Unread}  — only when non-zero
// ----------------------------------------------------------------------------
PcResult buildFolderPcExtended(const M7FolderSchema& schema,
                               Nid                   firstSubnodeNid,
                               const uint8_t*        containerClassUtf16le,
                               size_t                containerClassSize)
{
    array<uint8_t, 4> contentCountBytes{};
    detail::writeU32(contentCountBytes.data(), 0, schema.contentCount);

    array<uint8_t, 4> contentUnreadBytes{};
    detail::writeU32(contentUnreadBytes.data(), 0, schema.contentUnreadCount);

    array<uint8_t, 1> subfoldersBytes{
        static_cast<uint8_t>(schema.hasSubfolders ? 1u : 0u)
    };

    array<uint8_t, 4> hiddenCountBytes{};
    detail::writeU32(hiddenCountBytes.data(), 0, schema.pstHiddenCount);
    array<uint8_t, 4> hiddenUnreadBytes{};
    detail::writeU32(hiddenUnreadBytes.data(), 0, schema.pstHiddenUnreadCount);

    // M12.6 additions — storage owned here so pointers stay valid through
    // buildPropertyContext().
    array<uint8_t, 8> creationTimeBytes{};
    detail::writeU64(creationTimeBytes.data(), 0, 1ull); // FILETIME 1 stub
    array<uint8_t, 8> modTimeBytes{};
    detail::writeU64(modTimeBytes.data(), 0, 1ull);
    array<uint8_t, 4> displayTypeBytes{};
    detail::writeU32(displayTypeBytes.data(), 0, 0x01000000u);
    array<uint8_t, 4> folderFlagsBytes{};
    detail::writeU32(folderFlagsBytes.data(), 0, 2u);
    array<uint8_t, 4> hierFlagBytes{};
    detail::writeU32(hierFlagBytes.data(), 0, 0x80u);
    array<uint8_t, 4> designIdBytes{};
    detail::writeU32(designIdBytes.data(), 0, 0xDF80E239u);
    array<uint8_t, 4> designClassBytes{};
    detail::writeU32(designClassBytes.data(), 0, 6u);
    // Search key: 16-byte deterministic hash of the folder's display name
    // so the value is stable across rebuilds and matches what an Outlook
    // Sync-style consumer would expect to see.
    vector<uint8_t> searchKeyBytes;
    if (schema.displayNameSize > 0) {
        const std::string dn(
            reinterpret_cast<const char*>(schema.displayNameUtf16le),
            schema.displayNameSize);
        const auto sk = graph::deriveMessageSearchKey(dn);
        searchKeyBytes.assign(sk.begin(), sk.end());
    } else {
        searchKeyBytes.assign(16, 0u);
    }

    vector<PcProperty> props;
    props.reserve(16);

    if (schema.displayNameSize > 0)
        props.push_back({ 0x3001u, PropType::Unicode,
                          schema.displayNameUtf16le, schema.displayNameSize,
                          PropStorageHint::Auto });
    // M12.6: PR_CONTAINER_HIERARCHY mirrors display name. Outlook reads it
    // for folder-tree sorting and breadcrumb navigation.
    if (schema.displayNameSize > 0)
        props.push_back({ 0x7B9Eu, PropType::Unicode,
                          schema.displayNameUtf16le, schema.displayNameSize,
                          PropStorageHint::Auto });
    props.push_back({ 0x3602u, PropType::Int32,
                      contentCountBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x3603u, PropType::Int32,
                      contentUnreadBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x360Au, PropType::Boolean,
                      subfoldersBytes.data(), 1u, PropStorageHint::Auto });
    if (containerClassSize > 0)
        props.push_back({ 0x3613u, PropType::Unicode,
                          containerClassUtf16le, containerClassSize,
                          PropStorageHint::Auto });

    // M12.6 — Outlook-canonical folder envelope.
    props.push_back({ 0x3007u, PropType::SystemTime,
                      creationTimeBytes.data(), 8u, PropStorageHint::Auto });
    props.push_back({ 0x3008u, PropType::SystemTime,
                      modTimeBytes.data(), 8u, PropStorageHint::Auto });
    props.push_back({ 0x300Bu, PropType::Binary,
                      searchKeyBytes.data(), searchKeyBytes.size(),
                      PropStorageHint::Auto });
    props.push_back({ 0x3900u, PropType::Int32,
                      displayTypeBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x123Au, PropType::Int32,
                      folderFlagsBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x301Du, PropType::Int32,
                      hierFlagBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x7C0Eu, PropType::Int32,
                      designIdBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x7C0Fu, PropType::Int32,
                      designClassBytes.data(), 4u, PropStorageHint::Auto });

    // PR_PstHiddenCount / PR_PstHiddenUnreadCount: emit when non-zero, OR
    // when the caller has set `emitPstHiddenZero` (M12.15: REF carries
    // both tags with value 0 on user-visible contact folders so the IPM
    // Subtree Hierarchy TC row's CEB bits 11/12 line up with the PC).
    if (schema.pstHiddenCount != 0u || schema.emitPstHiddenZero)
        props.push_back({ 0x6635u, PropType::Int32,
                          hiddenCountBytes.data(), 4u, PropStorageHint::Auto });
    if (schema.pstHiddenUnreadCount != 0u || schema.emitPstHiddenZero)
        props.push_back({ 0x6636u, PropType::Int32,
                          hiddenUnreadBytes.data(), 4u, PropStorageHint::Auto });

    // M12.15: explicit PR_ATTR_HIDDEN (0x10F4) — emit when caller opts in.
    array<uint8_t, 1> attrHiddenBytes{
        static_cast<uint8_t>(schema.attrHiddenValue ? 1u : 0u)
    };
    if (schema.emitAttrHidden) {
        props.push_back({ 0x10F4u, PropType::Boolean,
                          attrHiddenBytes.data(), 1u, PropStorageHint::Auto });
    }

    return buildPropertyContext(props.data(), props.size(), firstSubnodeNid);
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
constexpr size_t kHierarchyCebOff  = 53;

// Set the CEB bit for `iBit` (high-bit-first byte numbering — bit 0
// of byte 0 is bit 7 of CEB[0]). Shared by all TC row builders below.
inline void setCebBit(uint8_t* ceb, unsigned iBit) noexcept
{
    const unsigned byteIdx   = iBit / 8u;
    const unsigned bitInByte = 7u - (iBit % 8u);
    ceb[byteIdx] |= static_cast<uint8_t>(1u << bitInByte);
}

} // namespace

TcResult buildFolderHierarchyTc(const HierarchyTcRow* rows, size_t rowCount)
{
    vector<array<uint8_t, kHierarchyRowSize>> rowBuffers(rowCount);
    vector<TcRow>                             tcRows(rowCount);
    vector<vector<TcVarlenCell>>              perRowVarlen(rowCount);

    // colIndex constants — match kHierarchyCols tag-ascending order.
    // The hierarchy row's DisplayName HID lives at colIndex=4 (0x3001),
    // ContainerClass_W at colIndex=8 (0x3613).
    constexpr size_t kDisplayNameColIdx    = 4;
    constexpr size_t kContainerClassColIdx = 8;

    for (size_t r = 0; r < rowCount; ++r) {
        const HierarchyTcRow& src = rows[r];
        uint8_t* dst = rowBuffers[r].data();
        std::memset(dst, 0, kHierarchyRowSize);
        uint8_t* ceb = dst + kHierarchyCebOff;

        // Bytes 0..3:   LtpRowId
        detail::writeU32(dst,  0, src.rowId.value);
        // Bytes 4..7:   LtpRowVer
        detail::writeU32(dst,  4, src.rowVer);
        // Bytes 8..11:  DisplayName_W HID (writer patches)
        // Bytes 12..15: ContentCount
        detail::writeU32(dst, 12, src.contentCount);
        // Bytes 16..19: ContentUnreadCount
        detail::writeU32(dst, 16, src.contentUnreadCount);
        // Bytes 44..47: PstHiddenCount (col 0x6635, iBit 11)
        detail::writeU32(dst, 44, src.pstHiddenCount);
        // Bytes 48..51: PstHiddenUnread (col 0x6636, iBit 12)
        detail::writeU32(dst, 48, src.pstHiddenUnreadCount);
        // Byte  52:     Subfolders
        dst[52] = src.hasSubfolders ? 1u : 0u;

        // M11-M (Tier 8): CEB now built dynamically based on what
        // values are actually meaningful, matching what
        // buildMailFolderPc / buildFolderPc emit in the child folder's
        // PC at NID = src.rowId. Previous static `kHierarchyCebByte0=0xFD`
        // claimed PR_CHANGENUM (iBit 7) was present even though the
        // folder PC doesn't emit it, AND failed to claim PR_CONTAINER_CLASS
        // (iBit 10) even when the folder PC has it. scanpst flagged this
        // as "Hierarchy Table for X, row doesn't match sub-object" and
        // real Outlook strict mode rejected the file.
        //
        // Always-present cells (folder PC always has these):
        //   iBit 0  LtpRowId
        //   iBit 1  LtpRowVer
        //   iBit 3  ContentCount       (buildMailFolderPc / buildFolderPc)
        //   iBit 4  ContentUnreadCount
        //   iBit 5  Subfolders
        // Conditional:
        //   iBit 2  DisplayName        (set when src.displayNameSize > 0)
        //   iBit 10 ContainerClass     (set when src.containerClassSize > 0)
        setCebBit(ceb, 0);
        setCebBit(ceb, 1);
        setCebBit(ceb, 3);
        setCebBit(ceb, 4);
        setCebBit(ceb, 5);

        if (src.displayNameUtf16le != nullptr && src.displayNameSize > 0) {
            perRowVarlen[r].push_back({ kDisplayNameColIdx,
                                        src.displayNameUtf16le,
                                        src.displayNameSize });
            setCebBit(ceb, 2);
        }

        if (src.containerClassUtf16le != nullptr && src.containerClassSize > 0) {
            perRowVarlen[r].push_back({ kContainerClassColIdx,
                                        src.containerClassUtf16le,
                                        src.containerClassSize });
            setCebBit(ceb, 10);
        }

        // M12.15: claim PstHiddenCount / PstHiddenUnread cells when caller
        // opts in (REF sets these on the user contacts folder row with
        // value 0; the child folder's PC must emit the matching tags via
        // schema.emitPstHiddenZero so the row/PC pair stays consistent).
        if (src.emitPstHidden) {
            setCebBit(ceb, 11);
            setCebBit(ceb, 12);
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

// 29-col schema (Round L) — byte-diff against real-Outlook backup.pst
// revealed our 28-col layout was missing PR_SEARCH_KEY (0x300B0102)
// AND had several ibData/iBit positions that didn't match what scanpst
// expects. Backup.pst's Contents Table:
//   * 29 columns (we had 28; missing 0x300B PR_SEARCH_KEY)
//   * row size 130 bytes (we had 126)
//   * CEB at offset 126 (we had 122)
//   * Booleans (0x0057/0x0058) at 124/125 (we had 120/121)
//   * ChangeKey (0x3013) moved from 116 to 76; SecureSubmitFlags from 76
//     to 80; Repl* group all shifted 4 bytes; LastModificationTime from
//     80 to 84
//
// [MS-PST] §3.12 lists 26 required cols; the 29th (PR_SEARCH_KEY) is
// real-Outlook-specific. scanpst's "row doesn't match sub-object" check
// validates the schema matches this 29-col layout.
constexpr TcColumn kContentsCols[29] = {
    // tag-sorted ascending
    { 0x0017u, PropType::Int32,      20, 4,  5 },  // Importance
    { 0x001Au, PropType::Unicode,    12, 4,  3 },  // MessageClass_W
    { 0x0036u, PropType::Int32,      60, 4, 15 },  // Sensitivity
    { 0x0037u, PropType::Unicode,    28, 4,  7 },  // Subject_W
    { 0x0039u, PropType::SystemTime, 40, 8,  9 },  // ClientSubmitTime
    { 0x0042u, PropType::Unicode,    24, 4,  6 },  // SentRepresentingName_W
    { 0x0057u, PropType::Boolean,   124, 1, 13 },  // MessageToMe (Round L: 120→124)
    { 0x0058u, PropType::Boolean,   125, 1, 14 },  // MessageCcMe (Round L: 121→125)
    { 0x0070u, PropType::Unicode,    68, 4, 17 },  // ConversationTopic_W
    { 0x0071u, PropType::Binary,     72, 4, 18 },  // ConversationIndex
    { 0x0E03u, PropType::Unicode,    56, 4, 12 },  // DisplayCc_W
    { 0x0E04u, PropType::Unicode,    52, 4, 11 },  // DisplayTo_W
    { 0x0E06u, PropType::SystemTime, 32, 8,  8 },  // MessageDeliveryTime
    { 0x0E07u, PropType::Int32,      16, 4,  4 },  // MessageFlags
    { 0x0E08u, PropType::Int32,      48, 4, 10 },  // MessageSize
    { 0x0E17u, PropType::Int32,       8, 4,  2 },  // MessageStatus
    { 0x0E30u, PropType::Binary,     92, 4, 22 },  // ReplItemId (Round L: 88→92, iBit 21→22)
    { 0x0E33u, PropType::Int64,      96, 8, 23 },  // ReplChangenum (Round L: 92→96)
    { 0x0E34u, PropType::Binary,    104, 4, 24 },  // ReplVersionhistory (Round L: 100→104)
    { 0x0E38u, PropType::Int32,     116, 4, 27 },  // ReplFlags (Round L: 112→116)
    { 0x0E3Cu, PropType::Binary,    112, 4, 26 },  // ReplCopiedfromVersionhistory (Round L: 108→112)
    { 0x0E3Du, PropType::Binary,    108, 4, 25 },  // ReplCopiedfromItemid (Round L: 104→108)
    { 0x1097u, PropType::Int32,      64, 4, 16 },  // ItemTemporaryFlags
    { 0x300Bu, PropType::Binary,    120, 4, 28 },  // PR_SEARCH_KEY (Round L: NEW)
    { 0x3008u, PropType::SystemTime, 84, 8, 21 },  // LastModificationTime (Round L: 80→84, iBit 20→21)
    { 0x3013u, PropType::Binary,     76, 4, 19 },  // ChangeKey (Round L: 116→76, iBit 27→19)
    { 0x65C6u, PropType::Int32,      80, 4, 20 },  // SecureSubmitFlags (Round L: 76→80, iBit 19→20)
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
    return buildTableContext(kContentsCols, 29, nullptr, 0);
}

namespace {

// Per-row matrix size derived from the 29-col Contents schema (Round L):
// 126 bytes of fixed data + 4-byte CEB = 130.
constexpr size_t kContentsRowSize = 130;
constexpr size_t kContentsCebOff  = 126;
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
// Round L: indices 16-22 unchanged (ItemTemporaryFlags + Repl* family)
constexpr size_t kColPrSearchKey             = 23;  // 0x300B (Binary, Round L)
constexpr size_t kColLastModificationTime    = 24;  // (was 23)
constexpr size_t kColChangeKey               = 25;  // 0x3013 (was 24)
constexpr size_t kColLtpRowId                = 27;  // (was 26)
constexpr size_t kColLtpRowVer               = 28;  // (was 27)

// (M11-M: setCebBit moved up to file-scope anonymous namespace so the
// Hierarchy row builder can use it. Definition at top of this file.)

} // namespace

TcResult buildFolderContentsTc(const ContentsTcRow* rows, size_t rowCount,
                               Nid firstSubnodeNid)
{
    if (rowCount == 0u) {
        return buildTableContext(kContentsCols, 29, nullptr, 0);
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

        // Round F follow-up (replaces M11-M Tier 8): per [MS-PST]
        // §2.4.4.5.1 ALL 26 Contents-TC required columns have "Copied?
        // Y" — every column MUST appear on both the row (CEB bit set)
        // and the message PC. M11-M had made these conditional to
        // match an incomplete PC; scanpst still flagged "row doesn't
        // match sub-object". Restoring unconditional CEB bits now that
        // buildMailPc emits the matching PC defaults (Sensitivity=0,
        // MessageStatus=0, MessageToMe=false, MessageCcMe=false).

        // MessageStatus @ ibData=8, iBit=2 — only set when caller opts in
        // (mail does, contacts don't — see ContentsTcRow.emitOptionalFixedCells).
        detail::writeU32(dst, 8, static_cast<uint32_t>(src.messageStatus));
        if (src.emitOptionalFixedCells) setCebBit(ceb, 2);

        // MessageFlags @ ibData=16, iBit=4 — buildMailPc and buildContactPc both emit.
        detail::writeU32(dst, 16, static_cast<uint32_t>(src.messageFlags));
        setCebBit(ceb, 4);

        // Importance @ ibData=20, iBit=5 — buildMailPc and buildContactPc both emit.
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

        // MessageSize @ ibData=48, iBit=10 — see emitOptionalFixedCells note above.
        detail::writeU32(dst, 48, static_cast<uint32_t>(src.messageSize));
        if (src.emitOptionalFixedCells) setCebBit(ceb, 10);

        // Sensitivity @ ibData=60, iBit=15 — both mail and contact PCs emit.
        detail::writeU32(dst, 60, static_cast<uint32_t>(src.sensitivity));
        setCebBit(ceb, 15);

        // LastModificationTime @ ibData=84, iBit=21 (Round L: 80→84, 20→21).
        if (src.lastModificationTime != 0u) {
            detail::writeU64(dst, 84, src.lastModificationTime);
            setCebBit(ceb, 21);
        }

        // MessageToMe @ ibData=124, iBit=13 / MessageCcMe @ 125, iBit=14
        // — see emitOptionalFixedCells. Outlook-exported IPM.Contact PCs
        // omit 0x0057/0x0058; setting the row CEB while the PC lacks
        // the tag was the proximate cause of "row doesn't match
        // sub-object" / "Failed to add row to the FLT" on contacts.
        dst[124] = src.messageToMe ? 1u : 0u;
        if (src.emitOptionalFixedCells) setCebBit(ceb, 13);
        dst[125] = src.messageCcMe ? 1u : 0u;
        if (src.emitOptionalFixedCells) setCebBit(ceb, 14);

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
        // ChangeKey (Round L: colIdx 24→25, ibData 116→76, iBit 27→19)
        pushVarlen(kColChangeKey, 19,
                   src.changeKeyBytes, src.changeKeySize);
        // PR_SEARCH_KEY (Round L: NEW colIdx 23, ibData 120, iBit 28 — Binary)
        pushVarlen(kColPrSearchKey, 28,
                   src.searchKeyBytes, src.searchKeySize);

        tcRows[r].rowId       = src.rowId.value;
        tcRows[r].rowBytes    = rowBuffers[r].data();
        tcRows[r].rowSize     = kContentsRowSize;
        tcRows[r].varlenCells = perRowVarlen[r].empty()
                                  ? nullptr : perRowVarlen[r].data();
        tcRows[r].varlenCount = perRowVarlen[r].size();
    }

    return buildTableContext(kContentsCols, 29, tcRows.data(), rowCount,
                             firstSubnodeNid);
}

TcResult buildFolderFaiContentsTc()
{
    return buildTableContext(kFaiContentsCols, 17, nullptr, 0);
}

// ----------------------------------------------------------------------------
// buildNameToIdMapPc — Name-to-Id Map per [MS-PST] §2.4.7.
//
// M10 (2026-05-11): real map registering PSETID_Appointment + 5 numeric
// dispids. Pre-M10 this was a stub — Outlook's Calendar UI couldn't
// resolve PidLidAppointmentStartWhole / EndWhole / Location / Duration
// / SubType, so events landed in the PST but no calendar grid showed.
//
// Layout produced:
//   * 0x0001 PidTagNameidBucketCount       PtypInteger32 = 251
//   * 0x0002 PidTagNameidStreamGuid        PtypBinary
//        16-byte PSETID_Appointment GUID. Referenced as GUID index 3
//        (per §2.4.7: indices 0/1/2 reserved; stream starts at 3).
//   * 0x0003 PidTagNameidStreamEntry       PtypBinary
//        5×8-byte NAMEID entries — one per dispid in this order:
//          dispid(4) | wGuid(2, = (3<<1)|0 = 6) | wPropIdx(2, 0..4)
//        Local ID for entry i = 0x8000 + wPropIdx_i. Order pinned in
//        kAppointmentNamedProps below.
//   * 0x0004 PidTagNameidStreamString      PtypBinary
//        4-byte zero stub (no string-named properties yet, but stream
//        must be non-empty so the HNID has bytes to point at — same
//        M11-K P5 constraint as the pre-M10 stub).
//   * 0x10xx Hash buckets                  PtypBinary
//        One property per non-empty bucket (bucket index = dispid % 251).
//        Each value is the same 8-byte NAMEID shape as the entry stream;
//        all 5 dispids hash to distinct buckets so each property holds
//        exactly one entry. PidTag id = 0x1000 + bucket_index.
//
// All buffers are emitted via PropStorageHint::Auto so buildPropertyContext
// picks inline vs. HN storage. Binary props >4 bytes always promote to HN
// per [MS-PST] §2.3.3.3.
// ----------------------------------------------------------------------------
namespace {

struct NamedPropEntry {
    uint32_t dispid;
    uint16_t localId;   // = 0x8000 + wPropIdx (assigned by order below)
    uint16_t wGuid;     // see kPsetid*WGuid below — encodes GUID idx + kind
};

constexpr uint32_t kNameidBucketCount = 251u;

// wGuid encoding ([MS-PST] §2.4.7.1): low bit = name kind (0=numeric, 1=string);
// high 15 bits = GUID index. GUID indices 0..2 are reserved (none / PS_MAPI /
// PS_PUBLIC_STRINGS); stream entries start at index 3.
constexpr uint16_t kPsetidAppointmentWGuid = (3u << 1) | 0u;  // = 6 — numeric
constexpr uint16_t kPsetidAddressWGuid     = (4u << 1) | 0u;  // = 8 — numeric

// Order pins wPropIdx. DO NOT REORDER without also updating the kLid*
// constants in messaging.hpp — local IDs leak into emitted PSTs.
//
// PSETID_Appointment first (indices 0..4 → local 0x8000..0x8004),
// then PSETID_Address (indices 5..9 → local 0x8005..0x8009). New
// entries are append-only.
constexpr NamedPropEntry kNamedProps[] = {
    // ---- PSETID_Appointment (calendar) ----
    { 0x820Du, kLidAppointmentStartWhole,        kPsetidAppointmentWGuid },
    { 0x820Eu, kLidAppointmentEndWhole,          kPsetidAppointmentWGuid },
    { 0x8208u, kLidLocation,                     kPsetidAppointmentWGuid },
    { 0x8213u, kLidAppointmentDuration,          kPsetidAppointmentWGuid },
    { 0x8215u, kLidAppointmentSubType,           kPsetidAppointmentWGuid },
    // ---- PSETID_Address (contacts) ----
    { 0x8005u, kLidFileUnder,                    kPsetidAddressWGuid     },
    { 0x8080u, kLidEmail1DisplayName,            kPsetidAddressWGuid     },
    { 0x8082u, kLidEmail1AddressType,            kPsetidAddressWGuid     },
    { 0x8083u, kLidEmail1EmailAddress,           kPsetidAddressWGuid     },
    { 0x8084u, kLidEmail1OriginalDisplayName,    kPsetidAddressWGuid     },
    { 0x8007u, kLidContactItemData,              kPsetidAddressWGuid     },
    { 0x8028u, kLidAbProviderEmailList,          kPsetidAddressWGuid     },
    { 0x8029u, kLidAbProviderArrayType,          kPsetidAddressWGuid     },
    { 0x8085u, kLidEmail1OriginalEntryId,        kPsetidAddressWGuid     },
};
constexpr size_t kNamedPropsCount =
    sizeof(kNamedProps) / sizeof(kNamedProps[0]);

// Encode one 8-byte NAMEID entry into out.
inline void encodeNameidEntry(uint8_t* out,
                              uint32_t dispid,
                              uint16_t wGuid,
                              uint16_t wPropIdx) noexcept
{
    detail::writeU32(out, 0, dispid);
    detail::writeU16(out, 4, wGuid);
    detail::writeU16(out, 6, wPropIdx);
}

} // namespace

PcResult buildNameToIdMapPc(Nid firstSubnodeNid)
{
    // ---- Stream buffers ----
    array<uint8_t, 4> bucketCountBytes{};
    detail::writeU32(bucketCountBytes.data(), 0, kNameidBucketCount);

    // NameidStreamGuid: PSETID_Appointment at index 0 (→ GUID idx 3),
    //                   PSETID_Address     at index 1 (→ GUID idx 4).
    array<uint8_t, 32> streamGuid{};
    std::memcpy(streamGuid.data(),      kPsetidAppointment.data(), 16);
    std::memcpy(streamGuid.data() + 16, kPsetidAddress.data(),     16);

    // NameidStreamEntry: 8 bytes per registry row.
    array<uint8_t, 8 * kNamedPropsCount> streamEntry{};
    for (size_t i = 0; i < kNamedPropsCount; ++i) {
        const uint16_t wPropIdx = static_cast<uint16_t>(
            kNamedProps[i].localId - 0x8000u);
        encodeNameidEntry(streamEntry.data() + i * 8u,
                          kNamedProps[i].dispid,
                          kNamedProps[i].wGuid,
                          wPropIdx);
    }

    // NameidStreamString: empty (all named props are numeric). 4-byte
    // zero stub so scanpst sees a non-empty HN allocation when the HNID
    // resolves the slot (M11-K P5 constraint).
    static constexpr array<uint8_t, 4> kStreamStringStub{};

    // ---- Hash bucket payloads (one per non-empty bucket) ----
    //
    // Numeric named-property bucket: `dispid % bucket_count`. Per
    // [MS-PST] §2.4.7, buckets that contain entries are stored as
    // PtypBinary properties at PidTag id (0x1000 + bucket_index). Each
    // bucket entry has the same 8-byte NAMEID layout as the entry
    // stream. We assume each bucket carries exactly one entry — if two
    // dispids collide, throw so the caller adds multi-entry support
    // (rather than silently emitting an ambiguous map).
    struct BucketBuf {
        uint16_t                   tagId;   // 0x1000 + bucket_index
        array<uint8_t, 8>          bytes;
    };
    array<BucketBuf, kNamedPropsCount> buckets{};
    for (size_t i = 0; i < kNamedPropsCount; ++i) {
        const uint32_t bucketIdx =
            kNamedProps[i].dispid % kNameidBucketCount;
        buckets[i].tagId = static_cast<uint16_t>(0x1000u + bucketIdx);
        const uint16_t wPropIdx = static_cast<uint16_t>(
            kNamedProps[i].localId - 0x8000u);
        encodeNameidEntry(buckets[i].bytes.data(),
                          kNamedProps[i].dispid,
                          kNamedProps[i].wGuid,
                          wPropIdx);
    }
    for (size_t i = 0; i < kNamedPropsCount; ++i) {
        for (size_t j = i + 1; j < kNamedPropsCount; ++j) {
            if (buckets[i].tagId == buckets[j].tagId) {
                throw std::logic_error(
                    "buildNameToIdMapPc: named-prop dispids hash to the "
                    "same bucket; need multi-entry bucket support");
            }
        }
    }

    // ---- Property descriptor array (4 streams + N buckets) ----
    constexpr size_t kPropCount = 4 + kNamedPropsCount;
    array<PcProperty, kPropCount> props{};

    props[0] = { 0x0001u, PropType::Int32,
                 bucketCountBytes.data(), 4u, PropStorageHint::Auto };
    props[1] = { 0x0002u, PropType::Binary,
                 streamGuid.data(), streamGuid.size(),
                 PropStorageHint::Auto };
    props[2] = { 0x0003u, PropType::Binary,
                 streamEntry.data(), streamEntry.size(),
                 PropStorageHint::Auto };
    props[3] = { 0x0004u, PropType::Binary,
                 kStreamStringStub.data(), kStreamStringStub.size(),
                 PropStorageHint::Auto };
    for (size_t i = 0; i < kNamedPropsCount; ++i) {
        props[4 + i] = { buckets[i].tagId, PropType::Binary,
                         buckets[i].bytes.data(), buckets[i].bytes.size(),
                         PropStorageHint::Auto };
    }

    return buildPropertyContext(props.data(), props.size(), firstSubnodeNid);
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

// M11-N round 16: scanpst flagged
//   "TC (nid=62B) missing required column (66050003)"
// PidTagPstHashFolder (0x6605, Int32) is the per-row CRC-32 hash of the
// MessageClass — Outlook uses it as the BTH key. With LtpRowId=0 +
// rowID=0, scanpst's BTH walker reports "keys overlap (dwkey=0,
// dwkeyMin=0)". Adding 0x6605 + setting LtpRowId/RowID to a non-zero
// hash satisfies both findings.
constexpr TcColumn kReceiveFolderCols[6] = {
    // tag-sorted ascending; cells laid out 4-byte first then 8-byte.
    { 0x001Au, PropType::Unicode,    8, 4,  2 },  // MessageClass_W
    { 0x3008u, PropType::SystemTime, 20, 8,  5 }, // LastModificationTime (8 B)
    { 0x3667u, PropType::Binary,    12, 4,  3 },  // ReceiveFolderID (HID; ENTRYID)
    { 0x6605u, PropType::Int32,     16, 4,  4 },  // PidTagPstHashFolder (round-16)
    { 0x67F2u, PropType::Int32,      0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,      4, 4,  1 },  // LtpRowVer
};
// 28 bytes fixed (4+4+4+4+4+8) + 1 CEB byte = 29.
constexpr size_t kReceiveFolderRowSize = 29;
constexpr size_t kReceiveFolderCebOff  = 28;

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
    // M11-L P2: RFT row needs both:
    //   1) LtpRowId = hash of message class. For default class "" we
    //      use LtpRowId=0 — the conventional "default" key Outlook
    //      looks up when receiving messages with unknown class.
    //   2) MessageClass column emitted as a NON-EMPTY UTF-16-LE
    //      payload (one NUL char = 2 bytes 0x00 0x00). A 0-byte
    //      varlen value would leave HID=0 in the row cell, which
    //      scanpst treats as "column missing" — same problem as
    //      M11-K P5 (NameId map empty streams).
    //   3) ReceiveFolderID = 24-byte ENTRYID for IPM Subtree.

    // Build the ENTRYID for IPM Subtree (NID 0x8022) — the default
    // recipient for messages with no specific class match.
    static const auto inboxEntryId =
        makeEntryId(kDefaultRftProviderUid, Nid{0x00008022u}, 0u);
    // M11-N: real Outlook-emitted PSTs use the literal class string "IPM"
    // (6 bytes UTF-16-LE) for their default-class row, not the empty
    // string. scanpst's "Receive folder table missing default message
    // class" check looks for either an empty-class row OR a row with
    // class "IPM" — and rejected our previous 2-byte-NUL hack as still
    // matching neither. Using "IPM" satisfies the check and is what
    // Outlook itself writes when it creates a fresh PST.
    static constexpr std::array<uint8_t, 6> ipmClassUtf16 = {
        0x49u, 0x00u,   // 'I'
        0x50u, 0x00u,   // 'P'
        0x4Du, 0x00u,   // 'M'
    };

    // M11-N round 17: scanpst's "RFT row (0), bad NID (6E1F4F31)" check
    // validates LtpRowId/RowID as a real NID (low 5 bits = nidType).
    // 0x6E1F4F31 ends in 0x11 (AttachmentTable type) — invalid for an
    // RFT row. Use IPM Subtree's NID (0x8022 = NormalFolder type 0x02)
    // — semantically "this row maps the IPM class to the IPM Subtree
    // folder" — which is also the value the row's ReceiveFolderID
    // ENTRYID points at.
    constexpr uint32_t kIpmHashFolder = 0x00008022u;

    // M11-N round 18: emit TWO rows so scanpst's "missing default
    // message class" check passes. Real Outlook RFTs have:
    //   row 1: class="" (empty string, the catch-all default)
    //   row 2: class="IPM" (any IPM.* class falls through here)
    // both pointing to IPM Subtree. The two distinct LtpRowIds also
    // give the underlying BTH a non-degenerate keyspace.
    static array<uint8_t, kReceiveFolderRowSize> row1{};  // default ""
    static array<uint8_t, kReceiveFolderRowSize> row2{};  // "IPM"
    static bool rowsInit = false;
    if (!rowsInit) {
        // Row 1: default class (""). Round 18 used 0x22 — scanpst rejected
        // with "missing NID (22)" because that NID doesn't exist in our
        // NBT. Round 19: use 0x122 (Root Folder NID — emitted by
        // pst_baseline.cpp). Distinct from row 2's 0x8022 so the BTH
        // keys are non-degenerate.
        constexpr uint32_t kEmptyClassRowId = 0x00000122u;
        detail::writeU32(row1.data(),  0, kEmptyClassRowId);
        detail::writeU32(row1.data(),  4, 1u);                // LtpRowVer
        detail::writeU32(row1.data(), 16, kEmptyClassRowId);  // PstHashFolder
        row1[kReceiveFolderCebOff] = 0xF8u;                   // bits 0..4 set

        // Row 2: LtpRowId=IPM Subtree NID, class="IPM"
        detail::writeU32(row2.data(),  0, kIpmHashFolder);
        detail::writeU32(row2.data(),  4, 1u);
        detail::writeU32(row2.data(), 16, kIpmHashFolder);
        row2[kReceiveFolderCebOff] = 0xF8u;
        rowsInit = true;
    }

    // varlen cells: row 1 has only ReceiveFolderID (no class string —
    // empty class is encoded as "no varlen cell at colIndex 0").
    // Actually: scanpst rejected zero-length class earlier ("HID=0").
    // Use a single zero-byte UTF-16-LE NUL (2 bytes) for "empty" — the
    // distinguishing factor from row 2's "IPM" (6 bytes) is the byte length.
    static constexpr std::array<uint8_t, 2> emptyClassUtf16 = { 0x00u, 0x00u };

    TcVarlenCell varlen1[2];
    varlen1[0].colIndex = 0; varlen1[0].bytes = emptyClassUtf16.data();
    varlen1[0].size     = emptyClassUtf16.size();
    varlen1[1].colIndex = 2; varlen1[1].bytes = inboxEntryId.data();
    varlen1[1].size     = inboxEntryId.size();

    TcVarlenCell varlen2[2];
    varlen2[0].colIndex = 0; varlen2[0].bytes = ipmClassUtf16.data();
    varlen2[0].size     = ipmClassUtf16.size();
    varlen2[1].colIndex = 2; varlen2[1].bytes = inboxEntryId.data();
    varlen2[1].size     = inboxEntryId.size();

    TcRow tcRows[2];
    tcRows[0].rowId       = 0x00000122u;
    tcRows[0].rowBytes    = row1.data();
    tcRows[0].rowSize     = kReceiveFolderRowSize;
    tcRows[0].varlenCells = varlen1;
    tcRows[0].varlenCount = 2;
    tcRows[1].rowId       = kIpmHashFolder;
    tcRows[1].rowBytes    = row2.data();
    tcRows[1].rowSize     = kReceiveFolderRowSize;
    tcRows[1].varlenCells = varlen2;
    tcRows[1].varlenCount = 2;

    return buildTableContext(kReceiveFolderCols, 6, tcRows, 2);
}

// ----------------------------------------------------------------------------
// buildSearchContentsTemplateTc — NID 0x610 Outgoing/Search Contents template.
//
// M11-L P1: REVERTED M11-K's 20-col dedicated schema. That schema put
// the 8-byte 0x3008 LastModTime at ibData=76 AFTER the booleans (72-74),
// giving TCI_4b=84 > TCI_1b=75 — violates [MS-PST] §2.3.4.1
// `TCI_4b ≤ TCI_2b ≤ TCI_1b ≤ TCI_bm`. scanpst couldn't parse the
// TCINFO and reported all 17 required columns as missing.
//
// New approach (per the Tier 6 prompt): take kContentsCols (28 cols)
// as the base — Contents schema is known-valid — and ADD the 3 columns
// scanpst flags as missing for NID 0x610:
//   0x67F1  PR_PF_PROXY              Int32     (4 B)
//   0x0E05  PR_PARENT_ENTRYID_W      Unicode   (4 B HID)
//   0x0E2A  PR_HASATTACH             Boolean   (1 B)
// Total: 31 cols. Booleans MessageToMe/MessageCcMe shift from
// ibData=120/121 → 128/129 to make room for the two new 4-byte
// cells at 120/124; HasAttach lands at 130. CEB region 122 → 131.
// per-row endBm = 135 (was 126).
// ----------------------------------------------------------------------------
namespace {

constexpr TcColumn kSearchContentsCols[31] = {
    // tag-sorted ascending; 4/8-byte cells in [0..127], 1-byte in
    // [128..130], CEB (4 bytes for 31 iBits) at 131. endBm = 135.
    // Identical to kContentsCols layout for ibData 0..115; new cells
    // and shifted booleans from 116 onwards.
    { 0x0017u, PropType::Int32,      20, 4,  5 },  // Importance
    { 0x001Au, PropType::Unicode,    12, 4,  3 },  // MessageClass_W
    { 0x0036u, PropType::Int32,      60, 4, 15 },  // Sensitivity
    { 0x0037u, PropType::Unicode,    28, 4,  7 },  // Subject_W
    { 0x0039u, PropType::SystemTime, 40, 8,  9 },  // ClientSubmitTime
    { 0x0042u, PropType::Unicode,    24, 4,  6 },  // SentRepresentingName_W
    { 0x0057u, PropType::Boolean,   128, 1, 13 },  // MessageToMe (M11-L: was 120)
    { 0x0058u, PropType::Boolean,   129, 1, 14 },  // MessageCcMe (M11-L: was 121)
    { 0x0070u, PropType::Unicode,    68, 4, 17 },  // ConversationTopic_W
    { 0x0071u, PropType::Binary,     72, 4, 18 },  // ConversationIndex
    { 0x0E03u, PropType::Unicode,    56, 4, 12 },  // DisplayCc_W
    { 0x0E04u, PropType::Unicode,    52, 4, 11 },  // DisplayTo_W
    { 0x0E05u, PropType::Unicode,   124, 4, 28 },  // ParentEntryID_W (NEW)
    { 0x0E06u, PropType::SystemTime, 32, 8,  8 },  // MessageDeliveryTime
    { 0x0E07u, PropType::Int32,      16, 4,  4 },  // MessageFlags
    { 0x0E08u, PropType::Int32,      48, 4, 10 },  // MessageSize
    { 0x0E17u, PropType::Int32,       8, 4,  2 },  // MessageStatus
    { 0x0E2Au, PropType::Boolean,   130, 1, 30 },  // HasAttach (NEW)
    { 0x0E30u, PropType::Binary,     88, 4, 21 },  // ReplItemId
    { 0x0E33u, PropType::Int64,      92, 8, 22 },  // ReplChangenum
    { 0x0E34u, PropType::Binary,    100, 4, 23 },  // ReplVersionhistory
    { 0x0E38u, PropType::Int32,     112, 4, 26 },  // ReplFlags
    { 0x0E3Cu, PropType::Binary,    108, 4, 25 },  // ReplCopiedfromVersionhistory
    { 0x0E3Du, PropType::Binary,    104, 4, 24 },  // ReplCopiedfromItemid
    { 0x1097u, PropType::Int32,      64, 4, 16 },  // ItemTemporaryFlags
    { 0x3008u, PropType::SystemTime, 80, 8, 20 },  // LastModificationTime
    { 0x3013u, PropType::Binary,    116, 4, 27 },  // ChangeKey
    { 0x65C6u, PropType::Int32,      76, 4, 19 },  // SecureSubmitFlags
    { 0x67F1u, PropType::Int32,     120, 4, 29 },  // PfProxy (NEW)
    { 0x67F2u, PropType::Int32,       0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,       4, 4,  1 },  // LtpRowVer
};

} // namespace

TcResult buildSearchContentsTemplateTc()
{
    // 0 rows — template that Outlook clones when it creates a real
    // Outgoing/Search folder. M11-L: 31-col schema with valid TCINFO.
    return buildTableContext(kSearchContentsCols,
                             sizeof(kSearchContentsCols) / sizeof(kSearchContentsCols[0]),
                             nullptr, 0);
}

// ----------------------------------------------------------------------------
// Scanpst-required additional template TCs at NID 0x6B6 / 0x6D7 / 0x6F8.
//
// scanpst.exe (Outlook 16.0.19929) flagged "TC (nid=X) missing required
// column (Y)" after we first emitted these as generic Hierarchy/Contents
// FAI shapes. The columns scanpst names per NID:
//   0x6B6: 0x0E370102 (PtypBinary)
//   0x6D7: 0x0E3E0102 (PtypBinary), 0x0E310102 (PtypBinary)
//   0x6F8: 0x30070040 (PtypSystemTime), 0x0E330014 (PtypInteger64)
// All also need the mandatory PidTagLtpRowId/RowVer columns at
// iBit=0/ibData=0 and iBit=1/ibData=4 per [MS-PST] §2.3.4.4.1.
//
// 0 rows in each — these are templates Outlook clones when it spins up
// the runtime objects that own these column sets.
// ----------------------------------------------------------------------------
namespace {

// 0x6B6: scanpst's required-column list grows across rounds. After
// round 3 it asked for 0x0E330014 (Int64) and 0x0E380003 (Int32) on
// top of the original 0x0E370102 (Binary). All three are Exchange
// replication tags — PR_REPL_CHANGENUM / PR_REPL_FLAGS / PR_REPL_TIME.
constexpr TcColumn kTemplate6B6Cols[5] = {
    // tag-sorted ascending; ibData laid out 4-byte cells then 8-byte.
    { 0x0E33u, PropType::Int64,    12, 8,  3 },  // ReplChangenum
    { 0x0E37u, PropType::Binary,    8, 4,  2 },  // ReplTime (Binary HID)
    { 0x0E38u, PropType::Int32,    20, 4,  4 },  // ReplFlags
    { 0x67F2u, PropType::Int32,     0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,     4, 4,  1 },  // LtpRowVer
};

// 0x6D7: round-3 added 0x0E30/0x0E33/0x0E34/0x0E38/0x001A on top of
// the original 0x0E3E/0x0E31. Looks like a comprehensive folder-
// replication template — all standard PR_REPL_* tags plus
// PR_MESSAGE_CLASS.
constexpr TcColumn kTemplate6D7Cols[9] = {
    // tag-sorted ascending; cells laid out 4-byte first then 8-byte.
    { 0x001Au, PropType::Unicode,   8, 4,  2 },  // MessageClass (Unicode HID)
    { 0x0E30u, PropType::Binary,   12, 4,  3 },  // ReplItemId (Binary HID)
    { 0x0E31u, PropType::Binary,   16, 4,  4 },  // (originally required)
    { 0x0E33u, PropType::Int64,    28, 8,  6 },  // ReplChangenum
    { 0x0E34u, PropType::Binary,   20, 4,  5 },  // ReplVersionhistory (Binary HID)
    { 0x0E38u, PropType::Int32,    24, 4,  7 },  // ReplFlags
    { 0x0E3Eu, PropType::Binary,   36, 4,  8 },  // (originally required)
    { 0x67F2u, PropType::Int32,     0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,     4, 4,  1 },  // LtpRowVer
};

constexpr TcColumn kTemplate6F8Cols[4] = {
    { 0x0E33u, PropType::Int64,    16, 8,  3 },  // required by scanpst
    { 0x3007u, PropType::SystemTime, 8, 8,  2 }, // required by scanpst
    { 0x67F2u, PropType::Int32,     0, 4,  0 },
    { 0x67F3u, PropType::Int32,     4, 4,  1 },
};

} // namespace

TcResult buildTemplate6B6Tc()
{
    return buildTableContext(kTemplate6B6Cols,
                             sizeof(kTemplate6B6Cols) / sizeof(kTemplate6B6Cols[0]),
                             nullptr, 0);
}

TcResult buildTemplate6D7Tc()
{
    return buildTableContext(kTemplate6D7Cols,
                             sizeof(kTemplate6D7Cols) / sizeof(kTemplate6D7Cols[0]),
                             nullptr, 0);
}

TcResult buildTemplate6F8Tc()
{
    return buildTableContext(kTemplate6F8Cols,
                             sizeof(kTemplate6F8Cols) / sizeof(kTemplate6F8Cols[0]),
                             nullptr, 0);
}

// ----------------------------------------------------------------------------
// Minimal valid empty TC with only the mandatory PidTagLtpRowId /
// PidTagLtpRowVer columns. Used as a stand-in for the search-management
// queue (NID 0x1E1), search activity list (NID 0x201), per-search-folder
// update queues (e.g. 0x2226), and criteria objects (e.g. 0x2227) — for
// none of which [MS-PST] pins an explicit column schema. scanpst rejects
// the previous buildEmptyNodePayload() (4 zero bytes) as "corrupt update
// queue" / "corrupt search activity list", but accepts a valid 0-row TC.
// ----------------------------------------------------------------------------
namespace {

constexpr TcColumn kMinimalTcCols[2] = {
    { 0x67F2u, PropType::Int32, 0, 4, 0 },  // LtpRowId
    { 0x67F3u, PropType::Int32, 4, 4, 1 },  // LtpRowVer
};

} // namespace

TcResult buildMinimalEmptyTc()
{
    return buildTableContext(kMinimalTcCols, 2, nullptr, 0);
}

// ----------------------------------------------------------------------------
// buildSearchActivityListTc — Search Activity List (NID 0x201).
//
// Round 5 emitted this as buildMinimalEmptyTc; scanpst still flagged
// "Search activity list corrupt" + "SAL missing entry (nid=2223)".
// SAL is a TC with one row per active search folder — emit a single row
// referencing the Spam Search Folder (NID 0x2223) so both errors clear.
// Schema is the same minimal shape; the row's LtpRowId carries the
// search folder NID.
// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
// buildOutgoingQueueTc — Outgoing Queue Table (NID 0x64C type 0x0C).
//
// Round 15 added NID 0x64C with the generic Folder-Contents schema;
// scanpst replied:
//   "TC (nid=64C) missing required column (0E140003)"
//   "TC (nid=64C) missing required column (0E100003)"
// 0x0E14 / 0x0E10 are PT_LONG (Int32) tags scanpst expects on the
// outgoing-queue TC. Likely PR_SUBMIT_FLAGS / PR_PARENT_KEY-like
// outgoing-queue properties. Empty (0-row) TC; the queue is normally
// empty unless mail is in transit.
// ----------------------------------------------------------------------------
namespace {

// Round 17: scanpst additionally flagged "TC (nid=64C) missing required
// column (00390040)" — PR_CLIENT_SUBMIT_TIME (SystemTime 8B). Added as
// a 5th column at ibData=16 (the 8-byte slot).
constexpr TcColumn kOutgoingQueueCols[5] = {
    // tag-sorted ascending; 4-byte cells then the 8-byte SystemTime.
    { 0x0039u, PropType::SystemTime, 16, 8,  4 },  // ClientSubmitTime (round 17)
    { 0x0E10u, PropType::Int32,       8, 4,  2 },  // required by scanpst
    { 0x0E14u, PropType::Int32,      12, 4,  3 },  // required by scanpst
    { 0x67F2u, PropType::Int32,       0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,       4, 4,  1 },  // LtpRowVer
};

} // namespace

TcResult buildOutgoingQueueTc()
{
    return buildTableContext(kOutgoingQueueCols, 5, nullptr, 0);
}

TcResult buildSearchActivityListTc()
{
    array<uint8_t, 9> rowBytes{};
    detail::writeU32(rowBytes.data(), 0, 0x00002223u);  // LtpRowId = NID 0x2223
    detail::writeU32(rowBytes.data(), 4, 1u);           // LtpRowVer
    rowBytes[8] = 0xC0u;  // CEB byte: bits 0+1 set (LtpRowId/RowVer present)

    TcRow tcRow{};
    tcRow.rowId       = 0x00002223u;
    tcRow.rowBytes    = rowBytes.data();
    tcRow.rowSize     = 9u;
    tcRow.varlenCells = nullptr;
    tcRow.varlenCount = 0u;

    return buildTableContext(kMinimalTcCols, 2, &tcRow, 1);
}

// ----------------------------------------------------------------------------
// buildSearchCriteriaObjectPc — Search Criteria Object (NID type 0x07).
//
// Round 5 emitted this as a TC (bClientSig=0x7C). scanpst replied
// "HN bad signature" + "Search folder missing search flags". Round 6
// switched to a PC with PR_SEARCH_FOLDER_FLAGS (0x6841) — fixed the
// bad-signature finding but "missing search flags" persisted: 0x6841
// is actually PR_SEARCH_FOLDER_TEMPLATE_ID, not the flag scanpst wants.
//
// Round 7: emit PR_FOLDER_FLAGS (0x66CD, Int32) which is the canonical
// "folder flags" property, plus keep PR_SEARCH_FOLDER_FLAGS (0x6841)
// for completeness. Initial value 0x4 sets the SEARCH_FOLDER bit per
// real-Outlook search-folder defaults.
// ----------------------------------------------------------------------------
PcResult buildSearchCriteriaObjectPc(Nid firstSubnodeNid)
{
    // Round L: byte-diff against real-Outlook backup.pst showed the
    // SCO at NID 0x2227 carries EXACTLY ONE property: 0x660B (Int32) = 0.
    // That's the "search flags" property scanpst's HMP walker checks
    // for. Rounds 5-K guessed at every plausible MAPI tag (PR_FOLDER_FLAGS,
    // 0x6840, PR_SEARCH_FOLDER_TEMPLATE_ID, PR_SEARCH_FLAGS=0x36C2, ...);
    // none worked because the actual tag is 0x660B which isn't in any
    // public MAPI documentation.
    array<uint8_t, 4> searchFlagsBytes{};
    detail::writeU32(searchFlagsBytes.data(), 0, 0u);

    PcProperty props[1] = {
        { 0x660Bu, PropType::Int32,
          searchFlagsBytes.data(), 4u, PropStorageHint::Auto },
    };

    return buildPropertyContext(props, 1, firstSubnodeNid);
}

// ----------------------------------------------------------------------------
// buildSearchFolderPc — best-guess; reuses regular FolderPc schema.
// KNOWN_UNVERIFIED: spec doesn't pin Search Folder PC's exact property set.
// ----------------------------------------------------------------------------
PcResult buildSearchFolderPc(const FolderPcSchema& schema,
                             Nid                   firstSubnodeNid)
{
    // Round 21: scanpst's "Search folder (nid=2227) missing search flags"
    // wasn't satisfied by emitting PR_FOLDER_FLAGS on the criteria-object
    // PC at 0x2227. Maybe scanpst checks PR_FOLDER_FLAGS on the search-
    // folder PC itself (0x2223). Build a regular folder PC + tack on
    // PR_FOLDER_FLAGS=4 (SEARCH_FOLDER bit).
    array<uint8_t, 4> contentCountBytes{};
    detail::writeU32(contentCountBytes.data(), 0, schema.contentCount);
    array<uint8_t, 4> contentUnreadBytes{};
    detail::writeU32(contentUnreadBytes.data(), 0, schema.contentUnreadCount);
    array<uint8_t, 1> subfoldersBytes{
        static_cast<uint8_t>(schema.hasSubfolders ? 1u : 0u)
    };
    array<uint8_t, 4> folderFlagsBytes{};
    // Round J: FLDFLAG_SEARCH = 0x2 per [MS-OXCFOLD] (was 0x4 = FLDFLAG_NORMAL).
    detail::writeU32(folderFlagsBytes.data(), 0, 0x00000002u);

    vector<PcProperty> props;
    props.reserve(5);
    if (schema.displayNameSize > 0)
        props.push_back({ 0x3001u, PropType::Unicode,
                          schema.displayNameUtf16le, schema.displayNameSize,
                          PropStorageHint::Auto });
    props.push_back({ 0x3602u, PropType::Int32,
                      contentCountBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x3603u, PropType::Int32,
                      contentUnreadBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x360Au, PropType::Boolean,
                      subfoldersBytes.data(), 1u, PropStorageHint::Auto });
    props.push_back({ 0x66CDu, PropType::Int32,           // PR_FOLDER_FLAGS
                      folderFlagsBytes.data(), 4u, PropStorageHint::Auto });
    return buildPropertyContext(props.data(), props.size(), firstSubnodeNid);
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
        // Block layout starts at 0x4800 (kIbAMap=0x4400 + AMap + PMap).
        // MUST match writer.cpp's kBlocksStart — wSig in block trailers
        // is computed from bid XOR ib at build time and must agree with
        // the file offset Outlook later sees. Round-A re-introduced PMap
        // at 0x4600 (with all-0xFF body, not zeros), shifting blocks by
        // one page.
        constexpr uint64_t kBlocksStart = 0x4800u;
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
