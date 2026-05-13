// pstwriter/mail.hpp
//
// M7 — Mail message builders. Layered on M4 (PC/TC), M6 (folder PC),
// and M7 Phase A (graph_message + graph_convert utilities).
//
// Public surface:
//   buildMailPc(GraphMessage)            — IPM.Note PC bytes for one message
//   buildRecipientRow(Recipient, idx)    — packs one row of the recipient TC
//   buildRecipientTc(rows)               — populates recipient TC with rows
//   buildAttachmentRow(Attachment, idx)  — packs one row of the attachment TC
//   buildAttachmentTc(rows)              — populates attachment TC with rows
//   buildAttachmentPc(Attachment)        — per-attachment PC carrying the data
//   writeM7Pst(M7PstConfig)              — Phase E end-to-end PST writer

#pragma once

#include "graph_message.hpp"
#include "ltp.hpp"
#include "messaging.hpp"
#include "types.hpp"
#include "writer.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace pstwriter {

// ============================================================================
// PidTagMessageFlags bits — [MS-OXOMSG] §2.2.1.6.
// ============================================================================
constexpr uint32_t kMsgFlagRead         = 0x00000001u;  // mfRead
constexpr uint32_t kMsgFlagUnmodified   = 0x00000002u;  // mfUnmodified
constexpr uint32_t kMsgFlagSubmit       = 0x00000004u;  // mfSubmit
constexpr uint32_t kMsgFlagUnsent       = 0x00000008u;  // mfUnsent
constexpr uint32_t kMsgFlagHasAttach    = 0x00000010u;  // mfHasAttach
constexpr uint32_t kMsgFlagFromMe       = 0x00000020u;  // mfFromMe

// ============================================================================
// MailPcBuildContext
//
// Caller-supplied wiring for buildMailPc. The Provider UID is the PST's
// 16-byte identity (same value used by the message store at M6); EntryID
// generation for sender/recipient slots needs it.
//
// `subnodeStart` is the first NID assigned to a subnode-promoted property
// (large body, attachments, recipients). The builder allocates strictly
// monotonic NIDs and reports back which NIDs it consumed via the
// MailPcResult.allocatedSubnodes vector.
// ============================================================================
struct MailPcBuildContext {
    std::array<uint8_t, 16> providerUid {};

    // First NID the builder may use for HN-overflow promotion. Must be a
    // non-HID NID (NidType != HID). Subsequent allocations are
    // subnodeStart, subnodeStart + 4, +8 ... in pidTag-ascending order.
    Nid subnodeStart {0u};

    // Round L+: message's own NID — used to populate PR_LtpRowId on
    // the message PC (matching row's LtpRowId for identity check).
    Nid messageNid  {0u};

    // M12.11+: parent folder's NID — used by contact PCs to populate
    // PR_LTP_PARENT_NID (0x0E2F). Scanpst's "Failed to add row to the
    // FLT" check appears to require this on contact PCs even though
    // libpff/drag-from-mount tolerate its absence. Mail PCs leave it
    // zero (M12 attempts on mail showed no scanpst delta).
    Nid parentFolderNid {0u};

    // M12.20: PR_LtpRowVer (0x67F3) value the PC should carry. MUST match
    // the LtpRowVer set on the Contents TC row that references this
    // message — REF emits non-zero values (mount-time inspection shows
    // Outlook treats LtpRowVer=0 as uncommitted and the Import wizard's
    // full-tree walk skips such rows). Caller (writeM8Pst, writeM7Pst,
    // ...) maintains a monotonic counter and pairs each Contents TC row
    // with the matching messageRowVer here.
    uint32_t messageRowVer {0u};
};

struct MailPcSubnode {
    Nid             nid;
    uint16_t        pidTagId;
    std::vector<uint8_t> bytes;   // value bytes — caller must encode in a data block
};

struct MailPcResult {
    std::vector<uint8_t>       hnBytes;
    std::vector<MailPcSubnode> subnodes;
};

// ============================================================================
// buildMailPc — emit an IPM.Note PC for a single Graph message.
//
// Properties emitted (subset of the M7 mapping table — see
// MILESTONES.md "Graph Message → PST property mapping table"):
//
//   Group A — top-level:
//     0x001A  PidTagMessageClass_W       Unicode    "IPM.Note"
//     0x0037  PidTagSubject_W            Unicode    UTF-16-LE
//     0x1000  PidTagBody_W               Unicode    plain-text body
//     0x10130102 PidTagBodyHtml          Binary     HTML body (Phase C)
//     0x0017  PidTagImportance           Int32
//     0x0E07  PidTagMessageFlags         Int32      bitfield
//     0x0E1B  PidTagHasAttachments       Boolean
//     0x3007  PidTagCreationTime         SystemTime
//     0x3008  PidTagLastModificationTime SystemTime
//     0x0039  PidTagClientSubmitTime     SystemTime
//     0x0E06  PidTagMessageDeliveryTime  SystemTime
//     0x1035  PidTagInternetMessageId_W  Unicode
//     0x0071  PidTagConversationIndex    Binary
//
//   Group B — sender (when GraphMessage has sender / from):
//     0x0C1A  PidTagSenderName_W           Unicode
//     0x0C1F  PidTagSenderEmailAddress_W   Unicode
//     0x0C1E  PidTagSenderAddressType_W    Unicode  "SMTP"
//     0x0C19  PidTagSenderEntryId          Binary   OneOff EntryID
//     0x0C1D  PidTagSenderSearchKey        Binary   16-byte search key
//     0x0042  PidTagSentRepresentingName_W ...      (5 mirror props)
//     etc.
//
// Subnode-promoted properties (body > 3580 bytes, large headers) are
// returned in MailPcResult.subnodes. Caller is responsible for:
//   1. Wrapping each subnode's bytes in a data block (M3 buildDataBlock).
//   2. Building the SLBLOCK for the message PC's bidSub.
//   3. Wiring everything in writeM5Pst.
//
// Throws:
//   * std::invalid_argument — context.subnodeStart has nidType=HID,
//                              or providerUid all-zero.
//   * std::length_error    — total HN body exceeds kMaxHnBodyBytes.
// ============================================================================
MailPcResult buildMailPc(const graph::GraphMessage& msg,
                         const MailPcBuildContext&  ctx);

// ============================================================================
// Recipient TC builder
//
// One row per Graph recipient (To/Cc/Bcc combined). Per [MS-PST]
// "Recipient Table Template" + §3.13 schema, the table has 14 columns.
//
// `rows` are presented in the To/Cc/Bcc concatenation order Outlook
// expects; each row's PidTagRecipientType is set per the source bucket.
//
// Returns a TC HN body. The recipient TC always lives in a subnode of
// the message PC (the message's bidSub references its block).
// ============================================================================
TcResult buildRecipientTc(const std::vector<graph::Recipient>& recipients);

// ============================================================================
// Attachment TC builder
//
// Returns a TC HN body for the attachment row index of one message.
// Attachment data lives in a separate Attachment PC (see
// buildAttachmentPc) — each row's PidTagLtpRowId is the NID of that PC.
// ============================================================================
struct AttachmentTcRow {
    Nid                       attachmentNid;
    const graph::Attachment*  attachment;
};

TcResult buildAttachmentTc(const std::vector<AttachmentTcRow>& rows);

// ============================================================================
// Attachment PC builder
//
// Per-attachment PC carrying:
//   * PidTagAttachMethod (1 = afByValue, 5 = afEmbeddedMessage)
//   * PidTagAttachFilenameW / PidTagAttachLongFilename_W / PidTagDisplayName_W
//   * PidTagAttachMimeTag_W (optional)
//   * PidTagAttachContentId_W (optional, inline images)
//   * PidTagRenderingPosition (-1 = no inline)
//   * PidTagAttachSize
//   * PidTagAttachDataBinary  — fileAttachment raw bytes
//     OR
//   * PidTagAttachDataObject  — itemAttachment embedded message PC bytes
//
// File-attachment data > 3580 bytes is subnode-promoted; the returned
// MailPcResult.subnodes list captures each promotion.
// ============================================================================
MailPcResult buildAttachmentPc(const graph::Attachment&   att,
                               const MailPcBuildContext&  ctx);

// ============================================================================
// M7 folder schema — shared between mail / calendar / contact folder PCs.
//
// The PidTagContainerClass that distinguishes folder types is supplied
// by the per-content-type wrappers:
//   * mail     -> buildMailFolderPc      (this header)
//   * calendar -> buildCalendarFolderPc  (event.hpp)
//   * contact  -> buildContactFolderPc   (contact.hpp)
// Each wrapper forwards into the universal envelope builder
// `buildFolderPcExtended` (messaging.cpp) with its own pre-baked
// container-class bytes from the `kContainerClass{Mail,Calendar,Contact}`
// constants below.
// ============================================================================
struct M7FolderSchema {
    const uint8_t* displayNameUtf16le {nullptr};
    size_t         displayNameSize    {0};
    uint32_t       contentCount       {0u};
    uint32_t       contentUnreadCount {0u};
    bool           hasSubfolders      {false};

    // PidTagPstHiddenCount / PidTagPstHiddenUnread (0x6635 / 0x6636).
    uint32_t pstHiddenCount       {0u};
    uint32_t pstHiddenUnreadCount {0u};

    // M12.15: explicit PR_ATTR_HIDDEN (0x10F4). When `emitAttrHidden` is
    // true, emit the Boolean property with `attrHiddenValue`; otherwise
    // omit. REF (real-Outlook contacts.pst, 2026-05-13) emits this on
    // user-visible contact folders (= False) and on the 7 hidden system
    // sub-folders under Contacts (= True). Special root-level folders
    // (IPM Subtree, Root, Deleted Items) omit it entirely.
    bool emitAttrHidden  {false};
    bool attrHiddenValue {false};

    // M12.15: force-emit PR_PstHiddenCount/PstHiddenUnread (0x6635/0x6636)
    // even when both values are 0. Without this, the existing
    // "non-zero only" path leaves them off the PC, which then disagrees
    // with the IPM Subtree Hierarchy TC row's CEB bits 11/12 — REF sets
    // those bits on user contact folder rows so the matching PC tags
    // must be present even with value 0.
    bool emitPstHiddenZero {false};

    // M12.16: PR_PST_FOLDER_DESIGN_CLS (0x7C0F). REF varies this per
    // folder-type: 6 on default IPF.Note user folders, 8 on the user
    // IPF.Contact folder, 0 on hidden contacts-anatomy sub-folders.
    // Default 6 preserves the historical behaviour for mail/calendar
    // callers that don't override it. The Import wizard's "include
    // subfolders" recursive walk appears to gate inclusion on this
    // value matching the expected default class for the folder's
    // container class.
    uint32_t designClass {6u};

    // M12.17: PR_36F8 (0x36F8 Int32). REF carries 0xA00000 on the user
    // visible Contacts folder and 0x300000 on the 7 hidden contacts-
    // anatomy sub-folders. Together with PR_36F7 (CLSID) and PR_3700
    // (Int32=3), this triad appears to mark the folder as an
    // "Outlook-recognised, real folder" for the Import wizard's
    // recursive walk. Default 0xA00000 covers the visible-user-folder
    // case; callers building hidden sub-folders override to 0x300000.
    uint32_t folderClass36F8 {0x00A00000u};
};

// Pre-computed UTF-16-LE bytes for the three Outlook container classes.
//   "IPF.Note"        =  8 chars × 2 = 16 bytes  (mail)
//   "IPF.Appointment" = 15 chars × 2 = 30 bytes  (calendar)
//   "IPF.Contact"     = 11 chars × 2 = 22 bytes  (contacts)
inline constexpr std::array<uint8_t, 16> kContainerClassMail = {
    'I',0,'P',0,'F',0,'.',0,'N',0,'o',0,'t',0,'e',0
};
inline constexpr std::array<uint8_t, 30> kContainerClassCalendar = {
    'I',0,'P',0,'F',0,'.',0,'A',0,'p',0,'p',0,'o',0,
    'i',0,'n',0,'t',0,'m',0,'e',0,'n',0,'t',0
};
inline constexpr std::array<uint8_t, 22> kContainerClassContact = {
    'I',0,'P',0,'F',0,'.',0,'C',0,'o',0,'n',0,'t',0,
    'a',0,'c',0,'t',0
};

// Universal 14-property folder PC envelope builder (defined in
// messaging.cpp). Each per-content-type wrapper supplies its own
// containerClass bytes; everything else is content-agnostic.
PcResult buildFolderPcExtended(const M7FolderSchema& schema,
                               Nid                   firstSubnodeNid,
                               const uint8_t*        containerClassUtf16le,
                               size_t                containerClassSize);

// Build a Mail (IPF.Note) folder PC.
PcResult buildMailFolderPc(const M7FolderSchema& schema,
                           Nid                   firstSubnodeNid);

// ============================================================================
// M7 — Internet headers serialization.
//
// Serialize an array of Graph internet headers back to the RFC 2822
// header block format. Each header rendered as
//   "Name: Value\r\n"
// concatenated. Used to populate PidTagTransportMessageHeaders (UTF-16).
// ============================================================================
std::string serializeInternetHeaders(
    const std::vector<graph::InternetMessageHeader>& headers);

// ============================================================================
// M7 — Folder tree
//
// Flat description of one mail folder under IPM Subtree. M7's folder
// hierarchy is flat: a list of M7Folder entries, each with a parent NID.
// Container class defaults to "IPF.Note".
// ============================================================================
struct M7Folder {
    std::string  displayName;       // UTF-8
    Nid          nid;                // assigned by caller (use M5Allocator)
    Nid          parentNid;          // typically IPM Subtree (0x8022) or another M7Folder
    std::string  containerClass {"IPF.Note"};

    // Optional folder identity for nesting. When ``path`` is non-empty
    // and ``parentPath`` references another M7Folder's ``path`` in the
    // same config, the writer resolves the parent's allocated NID and
    // overrides ``parentNid`` accordingly. Use these when you want
    // Outlook to render the source mailbox's folder tree (e.g.
    // "/Inbox", "/Inbox/Project X") instead of a flat list under IPM
    // Subtree. Empty values fall back to the legacy parentNid wiring.
    std::string  path;
    std::string  parentPath;

    // Messages contained in this folder, in the order they should appear
    // in the Contents TC.
    std::vector<const graph::GraphMessage*> messages;
};

// ============================================================================
// M7 — End-to-end writer.
//
// Produces a PST containing:
//   * The 27 §2.7.1 mandatory nodes (delegated to M6 writer pattern).
//   * For each M7Folder: a Folder PC (NormalFolder type) + Hierarchy /
//     Contents / FAI Contents tables.
//   * For each message in each folder: a Message PC (NormalMessage type),
//     plus a Recipient TC (RecipientTable type) and Attachment TC
//     (AttachmentTable type) when needed, plus Attachment PCs (Attachment
//     type) and data subnodes for binary attachments.
//   * IPM Subtree's Hierarchy TC populated with rows for each top-level
//     M7Folder.
// ============================================================================
struct M7PstConfig {
    std::string             path;
    std::array<uint8_t, 16> providerUid;

    // Display name embedded into the message store + Top of Personal Folders.
    std::string             pstDisplayName  {"M7 Mail PST"};

    // Folder tree. Order significant — first folder is conceptually
    // "Inbox" but the writer doesn't enforce naming.
    std::vector<M7Folder>   folders;
};

WriteResult writeM7Pst(const M7PstConfig& config) noexcept;

} // namespace pstwriter
