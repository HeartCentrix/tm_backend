// pstwriter/messaging.hpp
//
// M6 Messaging Core ([MS-PST] §2.7.1 mandatory-node schemas).
//
// Layered above the M4 LTP builders: each M6 builder takes high-level
// schema input (display names, ProviderUID, target NIDs) and emits an
// HN body via buildPropertyContext / buildTableContext. M6 owns the
// per-NID-type schema selection — what PidTags go in a Message Store
// PC vs a Folder PC vs a Hierarchy TC, and at what default values.
//
// Reader contract: M6 has no separate reader — the M4 readPropertyContext
// already decodes any PC byte-for-byte regardless of source. Round-trip
// tests verify M6 builders' output decodes back to the schema's logical
// inputs.
//
// Spec references (verified via WebFetch 2026-05-04 unless noted):
//   * [MS-PST] §2.7.1 — Mandatory Nodes (27 nodes verbatim)
//   * [MS-PST] §3.10  — Sample Message Store (9-property prose dump)
//   * [MS-PST] §3.12  — Sample Folder Object (4-prop Root PC + 13/27/17-col TCs)
//   * [MS-PST] §2.4.7 — Named Property Lookup Map (intro)

#pragma once

#include "ltp.hpp"
#include "types.hpp"
#include "writer.hpp"   // for WriteResult (writeM6Pst return type)

#include <array>
#include <cstdint>
#include <string>

namespace pstwriter {

// ============================================================================
// EntryID encoding (verified against [MS-PST] §3.10 sample, 2026-05-04).
//
// Layout (24 bytes total):
//   [bytes  0..  3]   rgbFlags    — 4 bytes, all-zero in §3.10 sample
//   [bytes  4.. 19]   ProviderUID — 16-byte GUID; equals message store's
//                                    PidTagRecordKey across the entire PST
//   [bytes 20.. 23]   entryNid    — 4-byte NID, little-endian
//
// rgbFlags = 0 is the M6 default. Outlook may use non-zero rgbFlags in
// some scenarios per [MS-OXCDATA]; pre-Phase-B verification queue item 5.
// ============================================================================
constexpr size_t kEntryIdSize = 24;

std::array<uint8_t, kEntryIdSize>
makeEntryId(const std::array<uint8_t, 16>& providerUid,
            Nid                            entryNid,
            uint32_t                       rgbFlags = 0u) noexcept;

// ============================================================================
// MessageStoreSchema — input to buildMessageStorePc(...).
//
// Defaults match [MS-PST] §3.10's published values where the spec pins
// them. Caller-supplied for fields where §3.10 only shows a sample value
// (display name, ProviderUID).
//
// Invariant the builder honors: ProviderUID is used as PidTagRecordKey
// AND inside every EntryID's bytes 4..19. Single source of truth — no
// way for the GUID to drift across props within one PST.
// ============================================================================
struct MessageStoreSchema {
    // The 16-byte GUID. Single source of truth for the PST's identity.
    // §3.10 sample value: 22 9D B5 0A DC D9 94 43 85 DE 90 AE B0 7D 12 70.
    std::array<uint8_t, 16> providerUid;

    // PidTagDisplayName UTF-16-LE bytes. May be empty (zero-length).
    // §3.10 sample value: "UNICODE1" (16 bytes).
    const uint8_t* displayNameUtf16le {nullptr};
    size_t         displayNameSize    {0};

    // EntryID-target NIDs for the 3 §2.7.1 sub-folders.
    Nid ipmSubTreeNid     {0x00008022u}; // IPM SuBTree
    Nid ipmWastebasketNid {0x00008062u}; // Deleted Items (wastebasket)
    Nid finderNid         {0x00008042u}; // Search Folder objects (finder)

    // PidTagValidFolderMask. §3.10 sample value: 0x89 (bits 0,3,7 set).
    // Bitmask semantics per [MS-OXCSTOR]; pre-Phase-B verification item 8.
    uint32_t validFolderMask {0x00000089u};

    // PidTagPstPassword. §3.10 sample: 0 (no password).
    uint32_t pstPassword {0u};

    // PidTagReplFlags. §3.10 sample: 0.
    uint32_t replFlags {0u};

    // PidTagReplVersionhistory layout: prefix(4) + providerUid(16) + suffix(4).
    // §3.10 sample: prefix=0x00000001, suffix=0x00000001.
    uint32_t replVersionPrefix {0x00000001u};
    uint32_t replVersionSuffix {0x00000001u};

    // §3.9 BTH extras (PidTags not surfaced in §3.10 prose but present in
    // the §3.9 BTH bytes per M5 closure finding). Canonical names not yet
    // identified — pre-Phase-B verification queue item 6.
    uint8_t  pidTag6633Boolean   {1u};            // §3.9 byte: 0x01
    uint32_t pidTag66FAInteger32 {0x000E000Du};   // §3.9 bytes
};

// ============================================================================
// buildMessageStorePc — emit the §2.7.1 NID_MESSAGE_STORE PC.
//
// Returns the HN body (ready to wrap with buildDataBlock). All 11
// properties are inlined or HN-stored — message store has no subnodes.
//
// `firstSubnodeNid` is required by buildPropertyContext's contract but
// is unused for the message store schema; pass any non-HID NID
// (M5Allocator's reserved-NID lookup is the canonical source).
//
// Throws std::invalid_argument on schema validation failure (e.g.
// firstSubnodeNid has nidType==HID).
// ============================================================================
PcResult buildMessageStorePc(const MessageStoreSchema& schema,
                             Nid                       firstSubnodeNid);

// ============================================================================
// FolderPcSchema — input to buildFolderPc(...).
//
// Covers §2.7.1's "PC / Schema Props" rows for NORMAL_FOLDER nodes:
//   * NID_ROOT_FOLDER (0x122)
//   * <IPM SuBTree>   (0x8022)
//   * <Search Folder objects>, the Finder (0x8042)
//   * <Deleted Items>, wastebasket    (0x8062)
//
// Per §3.12, all 4 share the same 4-property schema:
//   0x3001  PidTagDisplayName        PtypString    UTF-16-LE display name
//   0x3602  PidTagContentCount       PtypInteger32 message count in folder
//   0x3603  PidTagContentUnreadCount PtypInteger32 unread message count
//   0x360A  PidTagSubfolders         PtypBoolean   1 iff folder has children
//
// Search Folder PC (NID 0x2223, nidType=SEARCH_FOLDER) has a different
// schema and uses a separate builder (M6.4, not yet implemented).
// ============================================================================
struct FolderPcSchema {
    // PidTagDisplayName UTF-16-LE bytes. May be empty (zero-length).
    const uint8_t* displayNameUtf16le {nullptr};
    size_t         displayNameSize    {0};

    // PidTagContentCount — number of messages in this folder.
    // Always 0 in M6 (messages arrive in M7).
    uint32_t contentCount {0u};

    // PidTagContentUnreadCount — unread-message subset of contentCount.
    uint32_t contentUnreadCount {0u};

    // PidTagSubfolders — true iff this folder has child folders.
    // §3.12 sample: Root Folder = true (3 sub-folders), each sub-folder = ?
    // (sample doesn't show — we default to false; caller overrides for
    // multi-level hierarchies).
    bool hasSubfolders {false};
};

// Build the folder PC for any NORMAL_FOLDER node. Same builder for Root
// and sub-folders — only the schema input varies. The NID itself is
// caller-supplied at NBT-wiring time, not embedded in the PC bytes.
//
// `firstSubnodeNid` is required by buildPropertyContext but is unused
// for this 4-property schema (everything fits inline or in HN). Pass
// any non-HID NID.
PcResult buildFolderPc(const FolderPcSchema& schema,
                       Nid                   firstSubnodeNid);

// ============================================================================
// HierarchyTcRow — input to buildFolderHierarchyTc(...).
//
// One row per child folder. PidTagLtpRowId is the child folder's NID;
// the M4 reader walks the parent NBT entry's HierarchyTC and uses each
// row's RowId to navigate to the child folder's PC.
//
// §3.12 evidence: each row's "present" cells are exactly:
//   LtpRowId, LtpRowVer, DisplayName_W, ContentCount, ContentUnreadCount,
//   ReplChangenum, Subfolders.
// The other 6 columns (ReplItemid, ReplVersionhistory, ReplFlags,
// ContainerClass_W, 0x6635, 0x6636) have CEB bit clear.
// ============================================================================
struct HierarchyTcRow {
    // PidTagLtpRowId — child folder's NID. Required; identifies the row
    // in the RowIndex BTH and in any cross-table references.
    Nid rowId;

    // PidTagLtpRowVer — row version counter. Spec allows 0 for creators.
    uint32_t rowVer {0u};

    // PidTagDisplayName_W — UTF-16-LE bytes of the child folder's name.
    // Stored as a varlen HN allocation referenced by HID from the row data.
    const uint8_t* displayNameUtf16le {nullptr};
    size_t         displayNameSize    {0};

    // PidTagContentCount / ContentUnreadCount — message counts in child folder.
    uint32_t contentCount       {0u};
    uint32_t contentUnreadCount {0u};

    // PidTagSubfolders — true iff the child folder itself has children.
    bool hasSubfolders {false};
};

// ============================================================================
// buildFolderHierarchyTc — emit the §3.12-schema Hierarchy TC.
//
// Used for: NID_HIERARCHY_TABLE_TEMPLATE (0x060D) + per-folder hierarchy
// tables (0x012D, 0x802D, 0x804D, 0x806D from §2.7.1).
//
// 13-column schema per [SPEC §3.12]:
//   tag (sorted ascending) | ibData | cbData | iBit
//   ----------------------------------------------
//   0x0E300003 ReplItemid          | 20 |   4 |  6
//   0x0E330014 ReplChangenum       | 24 |   8 |  7
//   0x0E340102 ReplVersionhistory  | 32 |   4 |  8
//   0x0E380003 ReplFlags           | 36 |   4 |  9
//   0x3001001F DisplayName_W       |  8 |   4 |  2
//   0x36020003 ContentCount        | 12 |   4 |  3
//   0x36030003 ContentUnreadCount  | 16 |   4 |  4
//   0x360A000B Subfolders          | 52 |   1 |  5
//   0x3613001F ContainerClass_W    | 40 |   4 | 10
//   0x66350003 (PstHiddenCount)    | 44 |   4 | 11
//   0x66360003 (PstHiddenUnread)   | 48 |   4 | 12
//   0x67F20003 LtpRowId            |  0 |   4 |  0
//   0x67F30003 LtpRowVer           |  4 |   4 |  1
//
// Per-row endBm = 55 bytes (53 fixed-data + 2-byte CEB).
//
// Pass `rowCount = 0` for the empty-table case used by template TCs and
// by sub-folder Hierarchy tables that have no children.
// ============================================================================
TcResult buildFolderHierarchyTc(const HierarchyTcRow* rows,
                                size_t                rowCount);

// ============================================================================
// ContentsTcRow — input to the row-populated buildFolderContentsTc(...).
//
// One row per message in the owning folder. Columns map to the §3.12
// 27-col schema; cells whose source field is empty / zero are emitted
// with their CEB bit clear (i.e. "absent"), matching real-Outlook
// behaviour where missing values are signalled via CEB rather than
// stored as zero/empty payloads.
//
// PidTagLtpRowId (rowId) is the message's own NID. Outlook's reader uses
// it as the row's identity in the RowIndex BTH and as the link from a
// contents-table view back to the message PC's NBT entry.
//
// Variable-size cells (Subject/MessageClass/...) are passed as
// UTF-16-LE byte ranges (Unicode columns) or raw bytes
// (PidTagConversationIndex). Caller owns the underlying storage and
// must keep it alive for the duration of buildFolderContentsTc(...).
// ============================================================================
struct ContentsTcRow {
    Nid             rowId;             // PidTagLtpRowId (= message NID)
    uint32_t        rowVer        {0u};// PidTagLtpRowVer

    // Inline fixed-size cells. Set the corresponding *Present flag to
    // emit the cell with its CEB bit set; cells with the flag false are
    // zeroed and CEB-cleared.
    int32_t         importance       {1};   // 0x0017 (PidTagImportance; Normal=1)
    int32_t         sensitivity      {0};   // 0x0036 (None=0)
    int32_t         messageStatus    {0};   // 0x0E17
    int32_t         messageFlags     {0};   // 0x0E07
    int32_t         messageSize      {0};   // 0x0E08
    bool            messageToMe      {false}; // 0x0057
    bool            messageCcMe      {false}; // 0x0058

    // SystemTime cells — pass FILETIME-100ns ticks. Zero = absent
    // (CEB cleared); any non-zero value emits the cell with CEB set.
    uint64_t        clientSubmitTime     {0u};   // 0x0039
    uint64_t        messageDeliveryTime  {0u};   // 0x0E06
    uint64_t        lastModificationTime {0u};   // 0x3008

    // Variable-size cells — nullptr/0 = absent (CEB clear).
    // Unicode cells carry UTF-16-LE bytes (no null terminator); the
    // builder allocates the corresponding HN slot and patches the
    // row-cell HID.
    const uint8_t*  messageClassUtf16le         {nullptr};
    size_t          messageClassSize            {0};   // 0x001A
    const uint8_t*  subjectUtf16le              {nullptr};
    size_t          subjectSize                 {0};   // 0x0037
    const uint8_t*  sentRepresentingNameUtf16le {nullptr};
    size_t          sentRepresentingNameSize    {0};   // 0x0042
    const uint8_t*  conversationTopicUtf16le    {nullptr};
    size_t          conversationTopicSize       {0};   // 0x0070
    const uint8_t*  displayCcUtf16le            {nullptr};
    size_t          displayCcSize               {0};   // 0x0E03
    const uint8_t*  displayToUtf16le            {nullptr};
    size_t          displayToSize               {0};   // 0x0E04
    const uint8_t*  conversationIndexBytes      {nullptr};
    size_t          conversationIndexSize       {0};   // 0x0071 (Binary)
    // PidTagChangeKey (0x3013, Binary) — XID-format change tracker. Outlook
    // uses this for synchronization and conflict detection. scanpst flags
    // it as required column on Contents TC. Caller may pass nullptr/0 for
    // a stub (CEB cleared); a 22-byte XID is the canonical encoding.
    const uint8_t*  changeKeyBytes              {nullptr};
    size_t          changeKeySize               {0};   // 0x3013 (Binary)
};

// ============================================================================
// buildFolderContentsTc — emit the §3.12-schema 27-column Contents TC.
//
// Used for: NID_CONTENTS_TABLE_TEMPLATE (0x060E) + per-folder contents
// tables (0x012E, 0x802E, 0x804E, 0x806E from §2.7.1) + per-folder
// user-folder Contents TCs (M7+).
//
// 27-column schema per [SPEC §3.12], sorted by tag ascending. Per-row
// endBm = 122 (118 fixed + 4 CEB).
//
// No-arg overload emits "Columns Only" (0 rows) for the §2.7.1 baseline
// tables and template TCs that match §3.12's "Row Matrix Data Not
// Present" empty state.
//
// Row-populated overload emits one row per ContentsTcRow; used by M7+
// per-folder Contents TCs to surface the messages each folder contains
// in Outlook's message-list view.
// ============================================================================
TcResult buildFolderContentsTc();
TcResult buildFolderContentsTc(const ContentsTcRow* rows, size_t rowCount,
                               Nid firstSubnodeNid = Nid{0u});

// ============================================================================
// buildFolderFaiContentsTc — emit the §3.12-schema 17-column FAI Contents TC.
//
// Used for: NID_ASSOC_CONTENTS_TABLE_TEMPLATE (0x060F) + per-folder FAI
// tables (0x012F, 0x802F, 0x804F, 0x806F from §2.7.1).
//
// 17-column schema per [SPEC §3.12], sorted by tag ascending. Per-row
// endBm = 68 (65 fixed + 3 CEB). Always 0-row in M6.
// ============================================================================
TcResult buildFolderFaiContentsTc();

// ============================================================================
// buildNameToIdMapPc — emit the §2.4.7 NID_NAME_TO_ID_MAP PC (NID 0x0061).
//
// Per [MS-PST] §2.4.7 (Named Property Lookup Map): "the Name-to-ID-Map is a
// standard PC with some special properties. Specifically, the properties in
// the PC do not refer to real property identifiers, but instead point to
// specific data sections of the Name-to-ID-Map."
//
// §2.7.1 minimum state for NID 0x0061 = "Empty". M6 emits the 4 well-known
// stream/count properties, each at zero-length:
//
//   0x00010003 PidTagNameidBucketCount   PtypInteger32 = 251 (per §2.4.7
//                                          Hash Table page; value SHOULD
//                                          be 251 even when buckets unused)
//   0x00020102 PidTagNameidStreamGuid    PtypBinary    (empty stream)
//   0x00030102 PidTagNameidStreamEntry   PtypBinary    (empty stream)
//   0x00040102 PidTagNameidStreamString  PtypBinary    (empty stream)
//
// **Open question (KNOWN_UNVERIFIED)**: §2.4.7 Hash Table page says hash
// buckets are stored as PC properties at PidTag IDs 0x1000..0x10FA (251
// total). For an empty Name-to-ID Map, are these 251 zero-length bucket
// properties also required, or only the 4 stream/count properties? M6 ships
// the conservative 4-property version; if Outlook rejects at the M6 gate,
// expand to all 255 properties.
// ============================================================================
PcResult buildNameToIdMapPc(Nid firstSubnodeNid);

// ============================================================================
// buildRecipientTemplateTc — emit NID_RECIPIENT_TABLE (0x0692).
//
// 14 columns per [MS-PST] "Recipient Table Template" sub-page:
//   0x0C15 PidTagRecipientType    PtypInteger32
//   0x0E0F PidTagResponsibility   PtypBoolean
//   0x0FF9 PidTagRecordKey        PtypBinary
//   0x0FFE PidTagObjectType       PtypInteger32
//   0x0FFF PidTagEntryId          PtypBinary
//   0x3001 PidTagDisplayName      PtypString
//   0x3002 PidTagAddressType      PtypString
//   0x3003 PidTagEmailAddress     PtypString
//   0x300B PidTagSearchKey        PtypBinary
//   0x3900 PidTagDisplayType      PtypInteger32
//   0x39FF PidTag7BitDisplayName  PtypString
//   0x3A40 PidTagSendRichInfo     PtypBoolean
//   0x67F2 PidTagLtpRowId         PtypInteger32
//   0x67F3 PidTagLtpRowVer        PtypInteger32
//
// Always 0-row in M6 (no messages → no recipients yet).
// ============================================================================
TcResult buildRecipientTemplateTc();

// ============================================================================
// buildAttachmentTemplateTc — emit NID_ATTACHMENT_TABLE (0x0671).
//
// 6 columns per [MS-PST] "Attachment Table Template" sub-page:
//   0x0E20 PidTagAttachSize         PtypInteger32
//   0x3704 PidTagAttachFilenameW    PtypString
//   0x3705 PidTagAttachMethod       PtypInteger32
//   0x370B PidTagRenderingPosition  PtypInteger32
//   0x67F2 PidTagLtpRowId           PtypInteger32
//   0x67F3 PidTagLtpRowVer          PtypInteger32
//
// Always 0-row in M6.
// ============================================================================
TcResult buildAttachmentTemplateTc();

// ============================================================================
// buildSearchContentsTemplateTc — emit NID_SEARCH_CONTENTS_TABLE_TEMPLATE
// (0x0610).
//
// **Open question (KNOWN_UNVERIFIED)**: [MS-PST] does not publish a
// dedicated "Search Contents Table Template" page. M6 ships the same
// schema as the Contents Table Template (0x060E) since search-folder
// contents are message-shaped. Real-Outlook validation may reject if
// search folders need extra columns (search criteria, search state).
// ============================================================================
TcResult buildSearchContentsTemplateTc();

// ============================================================================
// buildSearchFolderPc — emit Search Folder PC (NID 0x2223 = Spam search Folder).
//
// **Open question (KNOWN_UNVERIFIED)**: [MS-PST] §2.7.1 lists 0x2223 as
// "PC / Schema Props" with `nidType = SEARCH_FOLDER` (0x03). The exact
// property schema for a search folder PC is not pinned by the spec text
// reachable so far. M6 ships the same 4-property schema as a regular
// folder PC (DisplayName, ContentCount, ContentUnreadCount, Subfolders).
// Real-Outlook validation may surface additional search-specific properties.
// ============================================================================
PcResult buildSearchFolderPc(const FolderPcSchema& schema,
                             Nid                   firstSubnodeNid);

// ============================================================================
// buildReceiveFolderTableTc — emit NID_RECEIVE_FOLDERS (0x0617).
//
// Per [MS-PST] §2.4.5 + [MS-OXOMSG] §2.2.3: maps message classes to the
// folder NIDs that should "receive" messages of that class. Outlook
// (and scanpst.exe) requires the table to exist with at least one row
// for the default class (empty string ""), otherwise scanpst reports:
//
//   !!Receive folder table missing
//   !!Receive folder table missing default message class
//
// This builder ships a minimal 1-row default-class entry mapping the
// empty class to IPM Subtree (`NID 0x8022`) — the conservative choice
// when the writer doesn't know about a real Inbox node. Real Outlook
// will rewrite this row when it learns the IPM.Note Inbox NID.
//
// Schema (4 columns, tag-sorted):
//   0x001A  PidTagMessageClass         Unicode  HID slot (4 B)
//   0x3001  PidTagDisplayName          Unicode  HID slot (4 B) — folder display name
//   0x67F2  LtpRowId                   Int32    (4 B) — folder NID
//   0x67F3  LtpRowVer                  Int32    (4 B)
//
// Schema kept deliberately minimal — scanpst is permissive about extra
// or missing columns once the table EXISTS with a default-class row.
// ============================================================================
TcResult buildReceiveFolderTableTc();

// ============================================================================
// buildEmptyNodePayload — bare-node payload for §2.7.1 "node / Empty" rows
// (NIDs 0x01E1 NID_SEARCH_MANAGEMENT_QUEUE and 0x0201 NID_SEARCH_ACTIVITY_LIST).
//
// Returns 4 zero bytes. The caller (Phase D end-to-end writer) wraps this
// with buildDataBlock to produce the on-disk encoded data block, then
// registers an NBTENTRY pointing at that block's BID with bidSub=0.
//
// **Open question (KNOWN_UNVERIFIED)**: §2.7.1 says "Empty" but doesn't
// pin a payload size or content. 4 zero bytes is the smallest legal data
// block payload. Real-Outlook validation may reveal a different shape.
// ============================================================================
std::vector<uint8_t> buildEmptyNodePayload();

// ============================================================================
// M6 Phase D — writeM6Pst end-to-end PST writer.
//
// Assembles all 27 §2.7.1 mandatory nodes into a single PST file. Calls
// the M5 plumbing (writeM5Pst) under the hood with the M6 builders'
// outputs as data-block payloads.
//
// Layout:
//   * Each PC/TC's HN body is wrapped in buildDataBlock(...) and laid out
//     at sequential 64-byte-aligned IBs starting at 0x600.
//   * Bare nodes (0x01E1, 0x0201) have a 4-byte zero payload.
//   * Block BIDs assigned sequentially via Bid::makeData(i+1) for
//     i in 0..26.
//
// nidParent wiring (per [SPEC §3.12] + [DESIGN]):
//   * NID_ROOT_FOLDER (0x122)            → self (0x122)
//   * Sub-folder PCs (0x2223, 0x8022,
//                     0x8042)            → Root (0x122)
//   * Deleted Items (0x8062)             → IPM Subtree (0x8022)
//   * All sibling tables / templates /
//     bare nodes / Message Store /
//     NameToIdMap                        → 0 (no parent)
//
// Hierarchy table contents:
//   * Root Folder Hierarchy (0x12D): 3 rows for Spam Search / IPM Subtree /
//     Finder, matching §3.12 sample.
//   * IPM Subtree Hierarchy (0x802D): 0 rows in M6 (KNOWN_UNVERIFIED:
//     §2.7.1 says "2 Rows" minimum but doesn't list specific children).
//   * All other Hierarchy tables: 0 rows.
//   * All Contents / FAI tables: 0 rows.
// ============================================================================
struct M6PstConfig {
    std::string             path;
    std::array<uint8_t, 16> providerUid;
};

WriteResult writeM6Pst(const M6PstConfig& config) noexcept;

} // namespace pstwriter
