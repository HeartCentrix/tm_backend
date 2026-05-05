// pstwriter/src/mail.cpp
//
// M7 — Mail message builders. Implementations.

#include "mail.hpp"

#include "block.hpp"
#include "graph_convert.hpp"
#include "graph_message.hpp"
#include "ltp.hpp"
#include "m5_allocator.hpp"
#include "messaging.hpp"
#include "pst_baseline.hpp"
#include "types.hpp"
#include "writer.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using std::array;
using std::string;
using std::vector;

namespace pstwriter {

namespace {

// ----------------------------------------------------------------------------
// PidTag IDs used by buildMailPc — defined locally to avoid bloating
// types.hpp's pid:: namespace before the rest of the property catalog
// stabilizes.
// ----------------------------------------------------------------------------
namespace pid_local {
// Group A
constexpr uint16_t kImportance               = 0x0017u;
constexpr uint16_t kMessageClass             = 0x001Au;
constexpr uint16_t kSubject                  = 0x0037u;
constexpr uint16_t kClientSubmitTime         = 0x0039u;
constexpr uint16_t kSentRepresentingName     = 0x0042u;
constexpr uint16_t kSentRepresentingAddrType = 0x0064u;
constexpr uint16_t kSentRepresentingEmail    = 0x0065u;
constexpr uint16_t kConversationIndex        = 0x0071u;
constexpr uint16_t kBody                     = 0x1000u;
constexpr uint16_t kInternetMessageId        = 0x1035u;
constexpr uint16_t kBodyHtml                 = 0x1013u;
constexpr uint16_t kCreationTime             = 0x3007u;
constexpr uint16_t kLastModificationTime     = 0x3008u;
constexpr uint16_t kMessageDeliveryTime      = 0x0E06u;
constexpr uint16_t kMessageFlags             = 0x0E07u;
constexpr uint16_t kHasAttachments           = 0x0E1Bu;
constexpr uint16_t kTransportMessageHeaders  = 0x007Du;

// Sender
constexpr uint16_t kSenderName               = 0x0C1Au;
constexpr uint16_t kSenderEntryId            = 0x0C19u;
constexpr uint16_t kSenderSearchKey          = 0x0C1Du;
constexpr uint16_t kSenderAddressType        = 0x0C1Eu;
constexpr uint16_t kSenderEmailAddress       = 0x0C1Fu;
constexpr uint16_t kSentRepresentingEntryId  = 0x0041u;
constexpr uint16_t kSentRepresentingSearchKey= 0x003Bu;

// Recipient row props
constexpr uint16_t kRecipientType            = 0x0C15u;
constexpr uint16_t kDisplayName              = 0x3001u;
constexpr uint16_t kAddressType              = 0x3002u;
constexpr uint16_t kEmailAddress             = 0x3003u;
constexpr uint16_t kSearchKey                = 0x300Bu;
constexpr uint16_t kEntryId                  = 0x0FFFu;
constexpr uint16_t kRecordKey                = 0x0FF9u;
constexpr uint16_t kObjectType               = 0x0FFEu;
constexpr uint16_t kDisplayType              = 0x3900u;
constexpr uint16_t k7BitDisplayName          = 0x39FFu;
constexpr uint16_t kSendRichInfo             = 0x3A40u;
constexpr uint16_t kResponsibility           = 0x0E0Fu;
constexpr uint16_t kLtpRowId                 = 0x67F2u;
constexpr uint16_t kLtpRowVer                = 0x67F3u;

// Attachment row props
constexpr uint16_t kAttachSize               = 0x0E20u;
constexpr uint16_t kAttachFilename           = 0x3704u;
constexpr uint16_t kAttachLongFilename       = 0x3707u;
constexpr uint16_t kAttachMethod             = 0x3705u;
constexpr uint16_t kRenderingPosition        = 0x370Bu;
constexpr uint16_t kAttachDataBinary         = 0x3701u;
constexpr uint16_t kAttachMimeTag            = 0x370Eu;
constexpr uint16_t kAttachContentId          = 0x3712u;

// Folder
constexpr uint16_t kContainerClass           = 0x3613u;
constexpr uint16_t kContentCount             = 0x3602u;
constexpr uint16_t kContentUnreadCount       = 0x3603u;
constexpr uint16_t kSubfolders               = 0x360Au;
constexpr uint16_t kPstHiddenCount           = 0x6635u;
constexpr uint16_t kPstHiddenUnreadCount     = 0x6636u;

// Bulk message-shape properties Outlook expects to surface in the
// reading-pane preview / conversation view. Without these, real
// Outlook tends to display the message as empty or with stale fields
// — even when the underlying body / sender properties are present.
// References: [MS-OXOMSG] §2.2.1.7 (Display* recipient strings),
// [MS-OXCMSG] §2.2.1.5/§2.2.1.10 (Subject decomposition + sizes),
// [MS-OXCMAIL] §2.2.1 (codepage tags).
constexpr uint16_t kDisplayTo                = 0x0E04u;
constexpr uint16_t kDisplayCc                = 0x0E03u;
constexpr uint16_t kDisplayBcc               = 0x0E02u;
constexpr uint16_t kConversationTopic        = 0x0070u;
constexpr uint16_t kNormalizedSubject        = 0x0E1Du;
constexpr uint16_t kSubjectPrefix            = 0x003Du;
constexpr uint16_t kMessageSize              = 0x0E08u;
constexpr uint16_t kInternetCodepage         = 0x3FDEu;
constexpr uint16_t kLocalCommitTime          = 0x6722u;
} // namespace pid_local

// ----------------------------------------------------------------------------
// PropBuilder — accumulates byte buffers + PcProperty descriptors with
// stable storage so the descriptors' valueBytes pointers stay valid until
// buildPropertyContext consumes them.
// ----------------------------------------------------------------------------
class PropBuilder {
public:
    void addInt32(uint16_t tag, uint32_t v)
    {
        scalar_.push_back({});
        auto& slot = scalar_.back();
        detail::writeU32(slot.data(), 0, v);
        addProp(tag, PropType::Int32, slot.data(), 4u);
    }

    void addBoolean(uint16_t tag, bool v)
    {
        scalar_.push_back({});
        auto& slot = scalar_.back();
        slot[0] = v ? 1u : 0u;
        addProp(tag, PropType::Boolean, slot.data(), 1u);
    }

    void addSystemTime(uint16_t tag, uint64_t ticks)
    {
        wide_.push_back({});
        auto& slot = wide_.back();
        detail::writeU64(slot.data(), 0, ticks);
        addProp(tag, PropType::SystemTime, slot.data(), 8u);
    }

    void addUnicodeString(uint16_t tag, const string& utf8)
    {
        if (utf8.empty()) {
            // Skip empty optional strings — Outlook is fine without an
            // empty PtypString here.
            return;
        }
        auto bytes = graph::utf8ToUtf16le(utf8);
        const uint8_t* ptr  = bytes.data();
        const size_t   size = bytes.size();
        bufs_.emplace_back(std::move(bytes));
        addProp(tag, PropType::Unicode, ptr, size);
    }

    void addUnicodeRaw(uint16_t tag, const uint8_t* utf16leBytes, size_t size)
    {
        if (size == 0) return;
        addProp(tag, PropType::Unicode, utf16leBytes, size);
    }

    void addBinary(uint16_t tag, vector<uint8_t> bytes)
    {
        if (bytes.empty()) return;
        const uint8_t* ptr  = bytes.data();
        const size_t   size = bytes.size();
        bufs_.emplace_back(std::move(bytes));
        addProp(tag, PropType::Binary, ptr, size);
    }

    void addBinaryRaw(uint16_t tag, const uint8_t* bytes, size_t size)
    {
        if (size == 0) return;
        addProp(tag, PropType::Binary, bytes, size);
    }

    const vector<PcProperty>& props() const noexcept { return props_; }

private:
    void addProp(uint16_t tag, PropType type, const uint8_t* bytes, size_t size)
    {
        PcProperty p;
        p.pidTagId   = tag;
        p.propType   = type;
        p.valueBytes = bytes;
        p.valueSize  = size;
        p.storage    = PropStorageHint::Auto;
        props_.push_back(p);
    }

    // Owned byte storage. emplace_back returns a stable reference (vector
    // grows; pointers may invalidate). We use deque-style behavior via
    // careful reservation OR use lists instead. Use explicit reservation.
    vector<vector<uint8_t>>     bufs_;
    vector<array<uint8_t, 4>>   scalar_;
    vector<array<uint8_t, 8>>   wide_;
    vector<PcProperty>          props_;

public:
    PropBuilder()
    {
        // Reserve generously so emplace_back doesn't relocate during use.
        // Per PST writer pattern (M4 PC writer); same reservation strategy
        // as buildMessageStorePc — 32 of each kind covers any single
        // message.
        bufs_.reserve(64);
        scalar_.reserve(64);
        wide_.reserve(64);
        props_.reserve(64);
    }
};

// ----------------------------------------------------------------------------
// Allocate stable copies of subnode bytes to return alongside the
// PcResult. The PcSubnodeOut entries point into the caller's memory; we
// must copy them before the caller's storage goes out of scope.
// ----------------------------------------------------------------------------
vector<MailPcSubnode> snapshotSubnodes(const vector<PcSubnodeOut>& src)
{
    vector<MailPcSubnode> out;
    out.reserve(src.size());
    for (const auto& s : src) {
        MailPcSubnode m;
        m.nid      = s.nid;
        m.pidTagId = s.pidTagId;
        m.bytes.assign(s.data, s.data + s.size);
        out.push_back(std::move(m));
    }
    return out;
}

// ----------------------------------------------------------------------------
// Compute PidTagMessageFlags bitfield from Graph fields.
// ----------------------------------------------------------------------------
uint32_t computeMessageFlags(const graph::GraphMessage& msg) noexcept
{
    uint32_t f = 0u;
    if (msg.isRead)         f |= kMsgFlagRead;
    if (msg.isDraft)        f |= kMsgFlagUnsent;
    if (!msg.attachments.empty() || msg.hasAttachments)
                            f |= kMsgFlagHasAttach;
    return f;
}

// ----------------------------------------------------------------------------
// Build the semicolon-separated display string for a recipient bucket.
// Per [MS-OXOMSG] §2.2.1.7, PidTagDisplayTo/Cc/Bcc are flat UTF-16-LE
// strings of recipient display names joined by "; ". Falls back to the
// SMTP address when a recipient has no display name.
// ----------------------------------------------------------------------------
string joinRecipientDisplay(const vector<graph::Recipient>& bucket)
{
    string out;
    for (size_t i = 0; i < bucket.size(); ++i) {
        const auto& r = bucket[i];
        const string& label = !r.emailAddress.name.empty()
                                ? r.emailAddress.name
                                : r.emailAddress.address;
        if (label.empty()) continue;
        if (!out.empty()) out += "; ";
        out += label;
    }
    return out;
}

// ----------------------------------------------------------------------------
// Split a subject into (prefix, normalized) parts per [MS-OXCMSG] §3.2.4.4.
// A subject prefix is up to 3 ASCII letters followed by ": ". When no
// prefix is present, returned prefix is empty and normalized == subject.
// ----------------------------------------------------------------------------
struct SubjectParts {
    string prefix;       // e.g. "RE: " — empty when subject has no prefix
    string normalized;   // subject minus prefix
};

SubjectParts splitSubject(const string& subject)
{
    SubjectParts p;
    p.normalized = subject;

    // Look for "<1..3 letters>: " at the start.
    size_t letters = 0;
    while (letters < subject.size() && letters < 3) {
        const unsigned char c = static_cast<unsigned char>(subject[letters]);
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))
            ++letters;
        else
            break;
    }
    if (letters >= 1 && letters + 1 < subject.size()
        && subject[letters] == ':' && subject[letters + 1] == ' ')
    {
        p.prefix     = subject.substr(0, letters + 2);
        p.normalized = subject.substr(letters + 2);
    }
    return p;
}

// ----------------------------------------------------------------------------
// Compute an estimated PidTagMessageSize value. Outlook treats this as an
// advisory total-bytes counter; an underestimate is harmless. We sum the
// UTF-8 byte sizes of the largest text properties so the value is at
// least proportional to the real on-disk footprint.
// ----------------------------------------------------------------------------
uint32_t estimateMessageSize(const graph::GraphMessage& msg) noexcept
{
    uint64_t total = 0;
    total += msg.subject.size();
    total += msg.body.content.size();
    total += msg.bodyPreview.size();
    for (const auto& h : msg.internetMessageHeaders) {
        total += h.name.size() + h.value.size() + 4;
    }
    for (const auto& a : msg.attachments) {
        total += static_cast<uint64_t>(a.size > 0 ? a.size : 0);
        total += a.name.size() + a.contentType.size();
    }
    if (total > 0xFFFFFFFFull) total = 0xFFFFFFFFull;
    return static_cast<uint32_t>(total);
}

} // namespace

// ============================================================================
// buildMailPc
// ============================================================================
MailPcResult buildMailPc(const graph::GraphMessage& msg,
                         const MailPcBuildContext&  ctx)
{
    if (ctx.subnodeStart.type() == NidType::HID) {
        throw std::invalid_argument(
            "buildMailPc: ctx.subnodeStart must have nidType != HID");
    }

    PropBuilder pb;

    // --- Group A: top-level message properties ---

    // Message class — always "IPM.Note" for M7 mail.
    pb.addUnicodeString(pid_local::kMessageClass, "IPM.Note");

    if (!msg.subject.empty()) {
        pb.addUnicodeString(pid_local::kSubject, msg.subject);

        // PidTagSubjectPrefix / PidTagNormalizedSubject / PidTagConversationTopic.
        // Outlook's message-list view, conversation grouping, and search
        // indexer all read these — without them the message often shows
        // as "(no subject)" in the reading pane even though Subject is set.
        const auto parts = splitSubject(msg.subject);
        if (!parts.prefix.empty())
            pb.addUnicodeString(pid_local::kSubjectPrefix, parts.prefix);
        if (!parts.normalized.empty()) {
            pb.addUnicodeString(pid_local::kNormalizedSubject, parts.normalized);
            pb.addUnicodeString(pid_local::kConversationTopic, parts.normalized);
        }
    }

    // Body: plain-text always emitted when present; HTML body in Phase C
    // adds PidTagBodyHtml (Binary) on top.
    if (msg.body.contentType == graph::BodyType::Text && !msg.body.content.empty()) {
        pb.addUnicodeString(pid_local::kBody, msg.body.content);
    }
    if (msg.body.contentType == graph::BodyType::Html) {
        // Per Decision 2: emit BOTH PidTagBodyHtml (UTF-8 bytes in PtypBinary)
        // AND a plain-text PidTagBody fallback (Outlook displays plain when
        // HTML rendering disabled). Phase C: bodyPreview as the fallback.
        if (!msg.body.content.empty()) {
            const auto& s = msg.body.content;
            vector<uint8_t> bytes(s.begin(), s.end());  // raw UTF-8
            pb.addBinary(pid_local::kBodyHtml, std::move(bytes));
        }
        if (!msg.bodyPreview.empty())
            pb.addUnicodeString(pid_local::kBody, msg.bodyPreview);
    }

    // Importance / message flags
    pb.addInt32(pid_local::kImportance, static_cast<uint32_t>(msg.importance));
    pb.addInt32(pid_local::kMessageFlags, computeMessageFlags(msg));
    pb.addBoolean(pid_local::kHasAttachments,
                  !msg.attachments.empty() || msg.hasAttachments);

    // PidTagObjectType = MAPI_MESSAGE (5). Outlook uses it to discriminate
    // PCs at the protocol level; missing values surface as "unknown item".
    pb.addInt32(pid_local::kObjectType, 5u);
    pb.addInt32(pid_local::kMessageSize, estimateMessageSize(msg));

    // PidTagInternetCodepage — for an HTML body Outlook needs the codepage
    // to render correctly. UTF-8 is 65001 per the IANA charset registry,
    // and is what Graph delivers HTML in.
    if (msg.body.contentType == graph::BodyType::Html) {
        pb.addInt32(pid_local::kInternetCodepage, 65001u);
    }

    // PidTagDisplay{To,Cc,Bcc} — semicolon-joined display names. Outlook's
    // message-list "To" column reads PidTagDisplayTo directly (it does NOT
    // re-derive from the recipient TC at view time); a missing value shows
    // a blank "To" column even when the recipient TC is fully populated.
    pb.addUnicodeString(pid_local::kDisplayTo,
                        joinRecipientDisplay(msg.toRecipients));
    pb.addUnicodeString(pid_local::kDisplayCc,
                        joinRecipientDisplay(msg.ccRecipients));
    pb.addUnicodeString(pid_local::kDisplayBcc,
                        joinRecipientDisplay(msg.bccRecipients));

    // Times
    if (!msg.createdDateTime.empty())
        pb.addSystemTime(pid_local::kCreationTime,
                         graph::isoToFiletimeTicks(msg.createdDateTime));
    if (!msg.lastModifiedDateTime.empty()) {
        const uint64_t ticks =
            graph::isoToFiletimeTicks(msg.lastModifiedDateTime);
        pb.addSystemTime(pid_local::kLastModificationTime, ticks);
        // PidTagLocalCommitTime — Outlook's last-touched timestamp on the
        // local store. Mirroring the modification time matches what real
        // Outlook stores when the item arrived from the server.
        pb.addSystemTime(pid_local::kLocalCommitTime, ticks);
    }
    if (!msg.sentDateTime.empty())
        pb.addSystemTime(pid_local::kClientSubmitTime,
                         graph::isoToFiletimeTicks(msg.sentDateTime));
    if (!msg.receivedDateTime.empty())
        pb.addSystemTime(pid_local::kMessageDeliveryTime,
                         graph::isoToFiletimeTicks(msg.receivedDateTime));

    // Internet message id
    if (!msg.internetMessageId.empty())
        pb.addUnicodeString(pid_local::kInternetMessageId, msg.internetMessageId);

    // Conversation index (raw bytes)
    if (!msg.conversationIndex.empty())
        pb.addBinary(pid_local::kConversationIndex,
                     vector<uint8_t>(msg.conversationIndex));

    // Internet headers (Phase D / always emit when present)
    if (!msg.internetMessageHeaders.empty()) {
        const string headersText = serializeInternetHeaders(msg.internetMessageHeaders);
        if (!headersText.empty())
            pb.addUnicodeString(pid_local::kTransportMessageHeaders, headersText);
    }

    // --- Group B: sender + sent representing ---
    // Use Graph's sender if present, else fall back to from.
    const graph::EmailAddress* sender = nullptr;
    if (msg.hasSender)      sender = &msg.sender;
    else if (msg.hasFrom)   sender = &msg.from;

    if (sender != nullptr) {
        if (!sender->name.empty()) {
            pb.addUnicodeString(pid_local::kSenderName, sender->name);
            pb.addUnicodeString(pid_local::kSentRepresentingName, sender->name);
        }
        if (!sender->address.empty()) {
            pb.addUnicodeString(pid_local::kSenderEmailAddress, sender->address);
            pb.addUnicodeString(pid_local::kSentRepresentingEmail, sender->address);
            pb.addUnicodeString(pid_local::kSenderAddressType, "SMTP");
            pb.addUnicodeString(pid_local::kSentRepresentingAddrType, "SMTP");

            const auto entryId = graph::makeOneOffEntryId(sender->name, sender->address);
            const auto searchK = graph::deriveSearchKey(sender->address);

            pb.addBinary(pid_local::kSenderEntryId, entryId);
            pb.addBinary(pid_local::kSentRepresentingEntryId,
                         vector<uint8_t>(entryId.begin(), entryId.end()));
            pb.addBinary(pid_local::kSenderSearchKey,
                         vector<uint8_t>(searchK.begin(), searchK.end()));
            pb.addBinary(pid_local::kSentRepresentingSearchKey,
                         vector<uint8_t>(searchK.begin(), searchK.end()));
        }
    }

    // ---- Build the PC ----
    const auto& props = pb.props();
    PcResult pc = buildPropertyContext(props.data(), props.size(), ctx.subnodeStart);

    MailPcResult out;
    out.hnBytes  = std::move(pc.hnBytes);
    out.subnodes = snapshotSubnodes(pc.subnodes);
    return out;
}

// ============================================================================
// Recipient TC builder
// ============================================================================
namespace {

// Recipient TC schema — the same 14-column template used by M6's
// recipient template. M7 populates rows; M6 emitted 0 rows.
//
// Per [MS-PST] "Recipient Table Template" + §3.13 schema.
constexpr TcColumn kRecipientCols[14] = {
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

constexpr size_t kRecipientRowSize = 50;  // end1b = 49+1 = 50; cCols=14, ceb=2 -> endBm=52 actually
// computeTcRgib will compute endBm; rowSize == endBm. Recompute statically:
//   end4b = 48 (4-byte cols max ibData+cb = 44+4)
//   end2b = 48 (no 2-byte cols)
//   end1b = 50 (1-byte cols max ibData+cb = 49+1)
//   endBm = 50 + ceil(14/8) = 50 + 2 = 52

// Index in kRecipientCols of the columns that need varlen-cell HID slots:
constexpr size_t kColIdx_RecordKey      = 2;
constexpr size_t kColIdx_EntryId        = 4;
constexpr size_t kColIdx_DisplayName    = 5;
constexpr size_t kColIdx_AddressType    = 6;
constexpr size_t kColIdx_EmailAddress   = 7;
constexpr size_t kColIdx_SearchKey      = 8;
constexpr size_t kColIdx_7BitDisplay    = 10;

constexpr size_t kRecipientEndBm = 52;

// CEB bit pattern for a fully-populated row.
// 14 cols => 2 bytes. Bit layout: byte 0 high-bit-first for iBit 0..7;
// byte 1 high-bit-first for iBit 8..15.
//   iBit 0..7  all set     -> byte 0 = 0xFF
//   iBit 8..13 all set     -> byte 1 = 0b11111100 = 0xFC
constexpr uint8_t kRecipientCebByte0 = 0xFFu;
constexpr uint8_t kRecipientCebByte1 = 0xFCu;

} // namespace

TcResult buildRecipientTc(const vector<graph::Recipient>& recipients)
{
    const size_t rowCount = recipients.size();

    // Stable storage for each row's bytes + varlen cells across the call.
    vector<array<uint8_t, kRecipientEndBm>> rowBuffers(rowCount);
    vector<TcRow>                            tcRows(rowCount);
    vector<vector<TcVarlenCell>>             perRowVarlen(rowCount);

    // Stable storage for per-row varlen byte vectors.
    struct RowVarlenStorage {
        vector<uint8_t> dnBytes;
        vector<uint8_t> atBytes;
        vector<uint8_t> eaBytes;
        vector<uint8_t> entryId;
        array<uint8_t, 16> searchKey {};
        array<uint8_t, 16> recordKey {};
    };
    vector<RowVarlenStorage> store(rowCount);

    for (size_t r = 0; r < rowCount; ++r) {
        const graph::Recipient& src = recipients[r];
        uint8_t* dst = rowBuffers[r].data();
        std::memset(dst, 0, kRecipientEndBm);

        const uint32_t rowId = static_cast<uint32_t>(r + 1);  // sequential 1..N

        // Fixed cells
        detail::writeU32(dst,  0, rowId);
        // LtpRowVer: spec wants monotonic. Mirror rowId so values are
        // strictly ascending with row order — search-index dedup paths
        // distinguish recipient updates by RowVer. (M11-#12.)
        detail::writeU32(dst,  4, rowId);                               // LtpRowVer
        detail::writeU32(dst,  8, static_cast<uint32_t>(src.kind));     // RecipientType
        detail::writeU32(dst, 16, 6u);                                  // ObjectType: MAPI_MAILUSER
        detail::writeU32(dst, 40, 0u);                                  // DisplayType: 0 = MAILUSER
        dst[48] = 0u;  // Responsibility
        dst[49] = 0u;  // SendRichInfo

        // CEB bits
        dst[50] = kRecipientCebByte0;
        dst[51] = kRecipientCebByte1;

        // Varlen cells (DisplayName, AddressType, EmailAddress, EntryId,
        // SearchKey, RecordKey, 7BitDisplayName)
        store[r].dnBytes = graph::utf8ToUtf16le(src.emailAddress.name);
        store[r].atBytes = graph::utf8ToUtf16le("SMTP");
        store[r].eaBytes = graph::utf8ToUtf16le(src.emailAddress.address);
        store[r].entryId = graph::makeOneOffEntryId(src.emailAddress.name,
                                                    src.emailAddress.address);
        store[r].searchKey = graph::deriveSearchKey(src.emailAddress.address);
        // RecordKey: 16 bytes derived from entryId tail (search key works too)
        store[r].recordKey = store[r].searchKey;

        auto& cells = perRowVarlen[r];
        cells.reserve(7);
        if (!store[r].dnBytes.empty())
            cells.push_back({ kColIdx_DisplayName, store[r].dnBytes.data(),
                              store[r].dnBytes.size() });
        if (!store[r].atBytes.empty())
            cells.push_back({ kColIdx_AddressType, store[r].atBytes.data(),
                              store[r].atBytes.size() });
        if (!store[r].eaBytes.empty())
            cells.push_back({ kColIdx_EmailAddress, store[r].eaBytes.data(),
                              store[r].eaBytes.size() });
        if (!store[r].entryId.empty())
            cells.push_back({ kColIdx_EntryId, store[r].entryId.data(),
                              store[r].entryId.size() });
        cells.push_back({ kColIdx_SearchKey, store[r].searchKey.data(),
                          store[r].searchKey.size() });
        cells.push_back({ kColIdx_RecordKey, store[r].recordKey.data(),
                          store[r].recordKey.size() });
        // 7BitDisplayName: emit a duplicate of the display name (ASCII fallback)
        if (!store[r].dnBytes.empty())
            cells.push_back({ kColIdx_7BitDisplay, store[r].dnBytes.data(),
                              store[r].dnBytes.size() });

        tcRows[r].rowId       = rowId;
        tcRows[r].rowBytes    = rowBuffers[r].data();
        tcRows[r].rowSize     = kRecipientEndBm;
        tcRows[r].varlenCells = cells.data();
        tcRows[r].varlenCount = cells.size();
    }

    return buildTableContext(kRecipientCols, 14, tcRows.data(), rowCount);
}

// ============================================================================
// Attachment TC builder
// ============================================================================
namespace {

// 6-column attachment template (matches M6 buildAttachmentTemplateTc).
constexpr TcColumn kAttachmentCols[6] = {
    { 0x0E20u, PropType::Int32,    8, 4,  2 },  // AttachSize
    { 0x3704u, PropType::Unicode, 12, 4,  3 },  // AttachFilenameW (HID)
    { 0x3705u, PropType::Int32,   16, 4,  4 },  // AttachMethod
    { 0x370Bu, PropType::Int32,   20, 4,  5 },  // RenderingPosition
    { 0x67F2u, PropType::Int32,    0, 4,  0 },  // LtpRowId
    { 0x67F3u, PropType::Int32,    4, 4,  1 },  // LtpRowVer
};

constexpr size_t kAttachColIdx_FilenameW = 1;

// rgib: end4b=24 (RenderingPosition at 20+4), end2b=24, end1b=24, endBm=24+1=25.
constexpr size_t kAttachmentEndBm = 25;

// 6 cols -> 1 CEB byte. Bits 0..5 set.
constexpr uint8_t kAttachmentCebByte0 = 0xFCu;

} // namespace

TcResult buildAttachmentTc(const vector<AttachmentTcRow>& rows)
{
    const size_t rowCount = rows.size();

    vector<array<uint8_t, kAttachmentEndBm>> rowBuffers(rowCount);
    vector<TcRow>                            tcRows(rowCount);
    vector<vector<TcVarlenCell>>             perRowVarlen(rowCount);
    vector<vector<uint8_t>>                  filenameStore(rowCount);

    for (size_t r = 0; r < rowCount; ++r) {
        const auto& src = rows[r];
        if (src.attachment == nullptr)
            throw std::invalid_argument("buildAttachmentTc: null attachment ptr");

        const graph::Attachment& att = *src.attachment;
        uint8_t* dst = rowBuffers[r].data();
        std::memset(dst, 0, kAttachmentEndBm);

        const uint32_t rowId = src.attachmentNid.value;

        detail::writeU32(dst,  0, rowId);
        detail::writeU32(dst,  4, 0u);                              // LtpRowVer
        detail::writeU32(dst,  8, static_cast<uint32_t>(att.size)); // AttachSize
        // Bytes 12..15 = filename HID (filled by writer)
        detail::writeU32(dst, 16, att.kind == graph::AttachmentKind::Item ? 5u : 1u);
        detail::writeU32(dst, 20, static_cast<uint32_t>(-1));       // RenderingPosition

        dst[24] = kAttachmentCebByte0;

        filenameStore[r] = graph::utf8ToUtf16le(att.name);
        if (!filenameStore[r].empty()) {
            perRowVarlen[r].push_back({ kAttachColIdx_FilenameW,
                                        filenameStore[r].data(),
                                        filenameStore[r].size() });
        }

        tcRows[r].rowId       = rowId;
        tcRows[r].rowBytes    = rowBuffers[r].data();
        tcRows[r].rowSize     = kAttachmentEndBm;
        tcRows[r].varlenCells = perRowVarlen[r].data();
        tcRows[r].varlenCount = perRowVarlen[r].size();
    }

    return buildTableContext(kAttachmentCols, 6, tcRows.data(), rowCount);
}

// ============================================================================
// buildAttachmentPc
// ============================================================================
MailPcResult buildAttachmentPc(const graph::Attachment&  att,
                               const MailPcBuildContext& ctx)
{
    if (ctx.subnodeStart.type() == NidType::HID) {
        throw std::invalid_argument(
            "buildAttachmentPc: ctx.subnodeStart must have nidType != HID");
    }

    PropBuilder pb;

    const uint32_t method = (att.kind == graph::AttachmentKind::Item) ? 5u : 1u;
    pb.addInt32(pid_local::kAttachMethod, method);
    pb.addInt32(pid_local::kAttachSize, static_cast<uint32_t>(att.size));
    pb.addInt32(pid_local::kRenderingPosition, static_cast<uint32_t>(-1));

    if (!att.name.empty()) {
        pb.addUnicodeString(pid_local::kAttachFilename, att.name);
        pb.addUnicodeString(pid_local::kAttachLongFilename, att.name);
        pb.addUnicodeString(pid_local::kDisplayName, att.name);
    }
    if (!att.contentType.empty())
        pb.addUnicodeString(pid_local::kAttachMimeTag, att.contentType);
    if (!att.contentId.empty())
        pb.addUnicodeString(pid_local::kAttachContentId, att.contentId);

    if (att.kind == graph::AttachmentKind::File) {
        if (!att.contentBytes.empty()) {
            pb.addBinary(pid_local::kAttachDataBinary,
                         vector<uint8_t>(att.contentBytes));
        }
    } else if (att.kind == graph::AttachmentKind::Item) {
        // Item attachment: serialize the embedded message as PC bytes,
        // store as PtypBinary at PidTagAttachDataObject (same tag id 0x3701
        // semantically — Outlook switches on PidTagAttachMethod).
        if (att.item) {
            MailPcBuildContext nestedCtx = ctx;
            // Bump subnodeStart so embedded subnodes don't collide with parent.
            nestedCtx.subnodeStart = Nid{ctx.subnodeStart.value + 0x4000u};
            const MailPcResult nested = buildMailPc(*att.item, nestedCtx);
            // Stash the embedded PC bytes as the AttachDataBinary value.
            pb.addBinary(pid_local::kAttachDataBinary,
                         vector<uint8_t>(nested.hnBytes));
            // NOTE: nested.subnodes are dropped here. M7 itemAttachment
            // simplification: only the embedded PC's HN body travels into
            // the parent attachment PC; nested subnodes (large embedded
            // bodies, embedded attachments-of-attachments) are M10 work.
            // [KNOWN_UNVERIFIED M7-4]
        }
    }

    const auto& props = pb.props();
    PcResult pc = buildPropertyContext(props.data(), props.size(), ctx.subnodeStart);

    MailPcResult out;
    out.hnBytes  = std::move(pc.hnBytes);
    out.subnodes = snapshotSubnodes(pc.subnodes);
    return out;
}

// ============================================================================
// buildMailFolderPc — extends M6 folder PC with PidTagContainerClass +
// PidTagPstHidden* properties for full Outlook compat.
// ============================================================================
PcResult buildMailFolderPc(const M7FolderSchema& schema,
                           Nid                   firstSubnodeNid)
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

    vector<PcProperty> props;
    props.reserve(7);

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
    if (schema.containerClassSize > 0)
        props.push_back({ 0x3613u, PropType::Unicode,
                          schema.containerClassUtf16le, schema.containerClassSize,
                          PropStorageHint::Auto });
    props.push_back({ 0x6635u, PropType::Int32,
                      hiddenCountBytes.data(), 4u, PropStorageHint::Auto });
    props.push_back({ 0x6636u, PropType::Int32,
                      hiddenUnreadBytes.data(), 4u, PropStorageHint::Auto });

    return buildPropertyContext(props.data(), props.size(), firstSubnodeNid);
}

// ============================================================================
// serializeInternetHeaders
// ============================================================================
string serializeInternetHeaders(
    const vector<graph::InternetMessageHeader>& headers)
{
    string out;
    for (const auto& h : headers) {
        out += h.name;
        out += ": ";
        out += h.value;
        out += "\r\n";
    }
    return out;
}

// ============================================================================
// writeM7Pst — Phase E end-to-end PST writer.
//
// Layout:
//   * 27 §2.7.1 mandatory nodes (subset of M6 layout, rebuilt per-call).
//   * For each M7Folder: PC + Hierarchy/Contents/FAI sibling tables.
//   * For each message: PC + Recipient TC subnode + Attachment TC subnode
//     (when present) + per-attachment PC subnodes + SLBLOCK.
//
// Subnode NID convention (within each message's subnode tree):
//   * 0x692 (NID_RECIPIENT_TABLE)  — recipient TC
//   * 0x671 (NID_ATTACHMENT_TABLE) — attachment TC
//   * (NidType::Attachment, idx=K)  — Kth attachment PC
// ============================================================================
namespace {

// Encode a UTF-8 string as UTF-16-LE bytes (no terminator). Used for
// folder display names + container class strings.
vector<uint8_t> u16le(const string& s)
{
    return graph::utf8ToUtf16le(s);
}

// One node we want to land in the final PST.
struct M7Node {
    Nid             nid;
    Nid             nidParent;
    Bid             bidData;
    Bid             bidSub;       // 0 if no subnode tree
    vector<uint8_t> bodyBytes;    // pre-encryption HN bytes / payload
};

// A scheduled data block (HN body or subnode payload).
struct M7Block {
    Bid             bid;
    vector<uint8_t> bodyBytes;
};

// A scheduled SLBLOCK (subnode index for one node).
struct M7SlBlock {
    Bid                bid;
    vector<SlEntry>    entries;
};

// A scheduled X/XXBLOCK (BID-list block chaining data blocks). cLevel=1
// produces XBLOCK pointing to data blocks; cLevel=2 produces XXBLOCK
// pointing to XBLOCKs. lcbTotal is the sum of indirected payload bytes.
struct M7XBlock {
    Bid              bid;
    uint8_t          cLevel;
    uint32_t         lcbTotal;
    vector<Bid>      childBids;
};

// XBLOCK / XXBLOCK header is 8 bytes; each child entry is 8 bytes.
// kMaxBlockPayload = 8176, so one XBLOCK can index up to (8176-8)/8 =
// 1021 children. One XXBLOCK indirecting through XBLOCKs can index up
// to 1021 × 1021 ≈ 1.04M data blocks ≈ 8.5 GB raw payload.
constexpr size_t kXBlockHeaderSize  = 8;
constexpr size_t kXBlockMaxChildren =
    (kMaxBlockPayload - kXBlockHeaderSize) / kXBlockEntrySize;

} // namespace

WriteResult writeM7Pst(const M7PstConfig& config) noexcept
{
    try {
        // ---- BID/NID allocators ----
        // Data blocks for HN bodies + subnode payloads share the data-BID
        // counter. Internal blocks (SLBLOCKs) use the internal-BID counter.
        uint64_t nextDataBidIdx     = 1u;
        uint64_t nextInternalBidIdx = 1u;

        auto allocDataBid = [&]() noexcept {
            return Bid::makeData(nextDataBidIdx++);
        };
        auto allocInternalBid = [&]() noexcept {
            return Bid::makeInternal(nextInternalBidIdx++);
        };

        M5Allocator alloc;

        // Storage for per-folder UTF-16-LE display names (must outlive
        // schema struct usage). Each folder iteration pushes TWO
        // entries (display name + container class), so reserve 2× the
        // folder count to keep raw pointers stable across the loop —
        // a smaller reserve risks reallocation under push_back which
        // would invalidate previously-captured `data()` pointers and
        // produce garbled folder PCs / hierarchy rows. (M11-#6.)
        vector<vector<uint8_t>> folderNameStore;
        folderNameStore.reserve(config.folders.size() * 2);

        const Nid kDummySub{0x00000041u};

        // Output collections
        vector<M7Node>    nodes;
        vector<M7Block>   dataBlocks;
        vector<M7SlBlock> slBlocks;
        vector<M7XBlock>  xBlocks;

        // Helper: schedule a data block & node.
        auto scheduleNode = [&](Nid nid, Nid parent,
                                vector<uint8_t> body,
                                Bid             bidSub = Bid{0u}) {
            M7Block b;
            b.bid       = allocDataBid();
            b.bodyBytes = body;
            const Bid bidData = b.bid;
            dataBlocks.push_back(std::move(b));

            M7Node n;
            n.nid       = nid;
            n.nidParent = parent;
            n.bidData   = bidData;
            n.bidSub    = bidSub;
            n.bodyBytes = std::move(body);  // unused after this; informational
            nodes.push_back(std::move(n));
        };

        auto scheduleDataBlock = [&](vector<uint8_t> body) -> Bid {
            M7Block b;
            b.bid       = allocDataBid();
            b.bodyBytes = std::move(body);
            const Bid out = b.bid;
            dataBlocks.push_back(std::move(b));
            return out;
        };

        // schedulePayload — schedule an arbitrary-size byte sequence as
        // either one data block (≤ 8176 B), one XBLOCK chaining N data
        // blocks (≤ ~8 MB), or one XXBLOCK chaining XBLOCKs chaining
        // data blocks (up to ~8.5 GB). Returns the BID that should land
        // in the SLENTRY's bidData / PC HID-slot for the consumer.
        //
        // Used wherever a promoted-subnode payload (large body, large
        // attachment data, large TC row matrix) is bigger than a single
        // data block can hold. Per [MS-PST] §2.2.2.8.3.2.{1,2}.
        auto schedulePayload = [&](vector<uint8_t> body) -> Bid {
            const size_t total = body.size();
            if (total <= kMaxBlockPayload) {
                return scheduleDataBlock(std::move(body));
            }

            // Step 1: chunk into data blocks of ≤ kMaxBlockPayload each.
            const size_t nChunks =
                (total + kMaxBlockPayload - 1u) / kMaxBlockPayload;
            vector<Bid>      chunkBids;     chunkBids.reserve(nChunks);
            vector<uint32_t> chunkSizes;    chunkSizes.reserve(nChunks);
            size_t off = 0;
            while (off < total) {
                const size_t cz = (total - off > kMaxBlockPayload)
                                    ? kMaxBlockPayload : (total - off);
                vector<uint8_t> chunk(body.begin() + off,
                                      body.begin() + off + cz);
                chunkBids.push_back(scheduleDataBlock(std::move(chunk)));
                chunkSizes.push_back(static_cast<uint32_t>(cz));
                off += cz;
            }

            // Step 2: build XBLOCK(s) over the data chunks. If we need
            // more than one XBLOCK (>1021 chunks), wrap them under an
            // XXBLOCK.
            auto makeBlock = [&](uint8_t cLevel, vector<Bid> children,
                                 uint32_t lcbTotal) -> Bid {
                M7XBlock x;
                x.bid       = allocInternalBid();
                x.cLevel    = cLevel;
                x.childBids = std::move(children);
                x.lcbTotal  = lcbTotal;
                const Bid out = x.bid;
                xBlocks.push_back(std::move(x));
                return out;
            };

            if (chunkBids.size() <= kXBlockMaxChildren) {
                return makeBlock(/*cLevel=*/1u,
                                 std::move(chunkBids),
                                 static_cast<uint32_t>(total));
            }

            // Need XXBLOCK over multiple XBLOCKs.
            vector<Bid> xblockBids;
            const size_t nXBlocks =
                (chunkBids.size() + kXBlockMaxChildren - 1u)
                    / kXBlockMaxChildren;
            xblockBids.reserve(nXBlocks);
            size_t xbOff = 0;
            while (xbOff < chunkBids.size()) {
                const size_t take = (chunkBids.size() - xbOff
                                       > kXBlockMaxChildren)
                                      ? kXBlockMaxChildren
                                      : (chunkBids.size() - xbOff);
                uint32_t lcb = 0;
                vector<Bid> children;
                children.reserve(take);
                for (size_t i = 0; i < take; ++i) {
                    children.push_back(chunkBids[xbOff + i]);
                    lcb += chunkSizes[xbOff + i];
                }
                xblockBids.push_back(makeBlock(/*cLevel=*/1u,
                                               std::move(children), lcb));
                xbOff += take;
            }
            return makeBlock(/*cLevel=*/2u,
                             std::move(xblockBids),
                             static_cast<uint32_t>(total));
        };

        // 1. Mandatory baseline nodes (excludes 0x802D/0x802E/0x802F).
        for (auto& e : buildPstBaselineEntries(config.providerUid,
                                                config.pstDisplayName))
        {
            scheduleNode(e.nid, e.nidParent, std::move(e.body));
        }

        // 2. Pre-register reserved NIDs into the allocator.
        registerBaselineReservedNids(alloc);

        // Walk folders, allocate NIDs.
        struct FolderRecord {
            const M7Folder* src;
            Nid             folderNid;
            Nid             hierarchyNid;
            Nid             contentsNid;
            Nid             faiNid;
            // Per-folder state populated as messages are added.
            uint32_t        contentCount       {0u};
            uint32_t        contentUnreadCount {0u};
        };
        vector<FolderRecord> folderRecs;
        folderRecs.reserve(config.folders.size());

        for (const auto& f : config.folders) {
            FolderRecord rec;
            rec.src         = &f;
            rec.folderNid   = alloc.allocate(NidType::NormalFolder);
            const uint32_t idx = rec.folderNid.index();
            rec.hierarchyNid = Nid(NidType::HierarchyTable,     idx);
            rec.contentsNid  = Nid(NidType::ContentsTable,      idx);
            rec.faiNid       = Nid(NidType::AssocContentsTable, idx);
            // Register sibling NIDs so they don't collide later.
            alloc.registerExternal(rec.hierarchyNid);
            alloc.registerExternal(rec.contentsNid);
            alloc.registerExternal(rec.faiNid);
            folderRecs.push_back(rec);
        }

        // ============================================================
        // 3. Build each user folder's structures + messages.
        // ============================================================
        // We need to first compute each folder's contents row count so
        // the folder PC can carry the right value. Walk messages in two
        // passes: first count, then build.

        for (auto& rec : folderRecs) {
            rec.contentCount       = static_cast<uint32_t>(rec.src->messages.size());
            // We don't know "unread" without scanning; treat all as read.
            rec.contentUnreadCount = 0u;
            for (const auto* m : rec.src->messages) {
                if (m && !m->isRead) rec.contentUnreadCount++;
            }
        }

        // For each folder: build PC + sibling tables. Schedule them.
        for (auto& rec : folderRecs) {
            // Display name buffer must outlive the schema struct
            folderNameStore.push_back(u16le(rec.src->displayName));
            const auto& nameBuf = folderNameStore.back();

            const auto containerClassBuf = u16le(rec.src->containerClass);
            // containerClassBuf is a temporary; we need stable storage too.
            folderNameStore.push_back(containerClassBuf);
            const auto& ccBuf = folderNameStore.back();

            M7FolderSchema schema{};
            schema.displayNameUtf16le    = nameBuf.data();
            schema.displayNameSize       = nameBuf.size();
            schema.contentCount          = rec.contentCount;
            schema.contentUnreadCount    = rec.contentUnreadCount;
            schema.hasSubfolders         = false;
            schema.containerClassUtf16le = ccBuf.data();
            schema.containerClassSize    = ccBuf.size();

            auto pc = buildMailFolderPc(schema, kDummySub);
            scheduleNode(rec.folderNid, rec.src->parentNid, std::move(pc.hnBytes));

            // Hierarchy TC (0 rows — M7 folders are flat; sub-folder
            // support is left to writeM7Pst callers via parentNid wiring).
            // Sibling tables (HIER/CONTENTS/FAI) NBTENTRYs carry
            // nidParent = 0 per [MS-PST] §3.12, confirmed via Aspose
            // oracle. See KNOWN_UNVERIFIED.md M11-D.
            scheduleNode(rec.hierarchyNid, Nid{0u},
                         buildFolderHierarchyTc(nullptr, 0).hnBytes);

            // Contents TC — populated rows below.
            // (Defer to message-building loop; we'll patch the slot.)

            // FAI contents TC (0 rows)
            scheduleNode(rec.faiNid, Nid{0u},
                         buildFolderFaiContentsTc().hnBytes);
        }

        // Build each folder's Contents TC with rows for messages.
        // First, allocate message NIDs so each row's PidTagLtpRowId is
        // the message NID.
        struct MessageRecord {
            const graph::GraphMessage* src;
            FolderRecord*              folder;
            Nid                        messageNid;
        };
        vector<MessageRecord> msgRecs;
        for (auto& rec : folderRecs) {
            for (const auto* m : rec.src->messages) {
                if (!m) continue;
                MessageRecord mr;
                mr.src        = m;
                mr.folder     = &rec;
                mr.messageNid = alloc.allocate(NidType::NormalMessage);
                msgRecs.push_back(mr);
            }
        }

        // Build per-folder Contents TC with one row per message.
        //
        // Real-Outlook contract: a folder's CONTENTS_TABLE TC must
        // contain a populated row for each message the folder owns —
        // Outlook reads the row's display columns (Subject, DisplayTo,
        // MessageDeliveryTime, ...) directly when building its
        // message-list view; an empty Contents TC produces "error
        // reading folder" even though the message NBT entries are
        // intact. Per [MS-OXCFOLD] §2.2.1.7 the row-cell columns are
        // copied from the corresponding message PC properties.
        //
        // Stable storage: we reserve `folderMsgCount` row buffers up
        // front so push_back() never reallocates — ContentsTcRow holds
        // raw pointers into the buffer entries.
        struct MsgRowBuffers {
            vector<uint8_t> messageClass;
            vector<uint8_t> subject;
            vector<uint8_t> sentRepresentingName;
            vector<uint8_t> conversationTopic;
            vector<uint8_t> displayTo;
            vector<uint8_t> displayCc;
        };

        for (auto& rec : folderRecs) {
            size_t folderMsgCount = 0;
            for (const auto& mr : msgRecs) {
                if (mr.folder == &rec) ++folderMsgCount;
            }

            vector<MsgRowBuffers> bufs;       bufs.reserve(folderMsgCount);
            vector<ContentsTcRow> contentRows; contentRows.reserve(folderMsgCount);

            for (const auto& mr : msgRecs) {
                if (mr.folder != &rec) continue;
                const graph::GraphMessage& m = *mr.src;

                bufs.push_back({});
                auto& b = bufs.back();

                b.messageClass = u16le("IPM.Note");

                if (!m.subject.empty()) {
                    b.subject = u16le(m.subject);
                    const auto parts = splitSubject(m.subject);
                    if (!parts.normalized.empty()) {
                        b.conversationTopic = u16le(parts.normalized);
                    }
                }

                if (m.hasFrom && !m.from.name.empty()) {
                    b.sentRepresentingName = u16le(m.from.name);
                } else if (m.hasSender && !m.sender.name.empty()) {
                    b.sentRepresentingName = u16le(m.sender.name);
                }

                const string displayToStr = joinRecipientDisplay(m.toRecipients);
                if (!displayToStr.empty()) b.displayTo = u16le(displayToStr);
                const string displayCcStr = joinRecipientDisplay(m.ccRecipients);
                if (!displayCcStr.empty()) b.displayCc = u16le(displayCcStr);

                ContentsTcRow row;
                row.rowId        = mr.messageNid;
                row.rowVer       = 1u;
                row.importance   = static_cast<int32_t>(m.importance);
                row.messageFlags =
                    static_cast<int32_t>(computeMessageFlags(m));
                row.messageSize  =
                    static_cast<int32_t>(estimateMessageSize(m));
                if (!m.receivedDateTime.empty()) {
                    row.messageDeliveryTime =
                        graph::isoToFiletimeTicks(m.receivedDateTime);
                }
                if (!m.sentDateTime.empty()) {
                    row.clientSubmitTime =
                        graph::isoToFiletimeTicks(m.sentDateTime);
                }
                if (!m.lastModifiedDateTime.empty()) {
                    row.lastModificationTime =
                        graph::isoToFiletimeTicks(m.lastModifiedDateTime);
                }
                if (!b.messageClass.empty()) {
                    row.messageClassUtf16le = b.messageClass.data();
                    row.messageClassSize    = b.messageClass.size();
                }
                if (!b.subject.empty()) {
                    row.subjectUtf16le = b.subject.data();
                    row.subjectSize    = b.subject.size();
                }
                if (!b.sentRepresentingName.empty()) {
                    row.sentRepresentingNameUtf16le = b.sentRepresentingName.data();
                    row.sentRepresentingNameSize    = b.sentRepresentingName.size();
                }
                if (!b.conversationTopic.empty()) {
                    row.conversationTopicUtf16le = b.conversationTopic.data();
                    row.conversationTopicSize    = b.conversationTopic.size();
                }
                if (!b.displayTo.empty()) {
                    row.displayToUtf16le = b.displayTo.data();
                    row.displayToSize    = b.displayTo.size();
                }
                if (!b.displayCc.empty()) {
                    row.displayCcUtf16le = b.displayCc.data();
                    row.displayCcSize    = b.displayCc.size();
                }
                if (!m.conversationIndex.empty()) {
                    row.conversationIndexBytes = m.conversationIndex.data();
                    row.conversationIndexSize  = m.conversationIndex.size();
                }
                contentRows.push_back(row);
            }

            // Subnode NID for the (possibly promoted) row matrix. Lives
            // in the contentsNid's own subnode-tree namespace, so it
            // doesn't collide with top-level NBT NIDs. NidType ≠ HID
            // is required for HNID NID-branch decoding per §2.3.3.2.
            const Nid kRowMatrixSubnode{
                (static_cast<uint32_t>(NidType::Internal)) | (1u << 5)};

            auto tc = buildFolderContentsTc(
                contentRows.empty() ? nullptr : contentRows.data(),
                contentRows.size(),
                kRowMatrixSubnode);

            Bid contentsBidSub{0u};
            if (!tc.subnodes.empty()) {
                // Row matrix overflowed HN — schedule its bytes (with
                // XBLOCK chaining if rowMatrixSize > 8176) and emplace
                // an SLENTRY pointing at the resulting BID. Build a
                // single-entry SLBLOCK and use it as the contentsNid's
                // bidSub.
                vector<SlEntry> contentsSl;
                contentsSl.reserve(tc.subnodes.size());
                for (auto& s : tc.subnodes) {
                    vector<uint8_t> bytes(s.data, s.data + s.size);
                    const Bid sBid = schedulePayload(std::move(bytes));
                    contentsSl.emplace_back(s.nid, sBid, Bid{0u});
                }
                std::sort(contentsSl.begin(), contentsSl.end(),
                          [](const SlEntry& a, const SlEntry& b) {
                              return a.nid.value < b.nid.value;
                          });
                M7SlBlock slb;
                slb.bid     = allocInternalBid();
                slb.entries = std::move(contentsSl);
                contentsBidSub = slb.bid;
                slBlocks.push_back(std::move(slb));
            }
            scheduleNode(rec.contentsNid, Nid{0u},
                         std::move(tc.hnBytes), contentsBidSub);
        }

        // ============================================================
        // 4. IPM Subtree Hierarchy TC: 1 row per user folder.
        // ============================================================
        {
            // Need stable storage for HierarchyTcRow's display-name
            // pointers — the rows array references into folderNameStore.
            vector<HierarchyTcRow> ipmHierRows;
            ipmHierRows.reserve(folderRecs.size());
            for (size_t i = 0; i < folderRecs.size(); ++i) {
                HierarchyTcRow row;
                row.rowId = folderRecs[i].folderNid;
                // folderNameStore stores [name, containerClass, name,
                // containerClass, ...]. Display name is at index 2*i.
                const auto& nameBuf = folderNameStore[2 * i];
                row.displayNameUtf16le = nameBuf.data();
                row.displayNameSize    = nameBuf.size();
                row.contentCount       = folderRecs[i].contentCount;
                row.contentUnreadCount = folderRecs[i].contentUnreadCount;
                row.hasSubfolders      = false;
                ipmHierRows.push_back(row);
            }
            const HierarchyTcRow* rowsPtr =
                ipmHierRows.empty() ? nullptr : ipmHierRows.data();
            auto tc = buildFolderHierarchyTc(rowsPtr, ipmHierRows.size());
            scheduleNode(Nid{0x0000802Du}, Nid{0u}, std::move(tc.hnBytes));
        }

        // ============================================================
        // 5. Message PCs + subnodes.
        // ============================================================
        // Each message: build mailPc, recipientTc (if recipients),
        // attachmentTc (if attachments), per-attachment PCs, then SLBLOCK.
        for (auto& mr : msgRecs) {
            const graph::GraphMessage& m = *mr.src;

            MailPcBuildContext ctx;
            ctx.providerUid  = config.providerUid;
            // subnodeStart for body promotion — pick high NID values
            // unlikely to collide with subnode-tree NIDs (0x671, 0x692,
            // attachment NIDs).
            ctx.subnodeStart = Nid{(mr.messageNid.value & ~uint32_t{0x1Fu}) + 0x10000u + 0x1u};

            // Build mail PC
            MailPcResult pc = buildMailPc(m, ctx);

            // Build SLBLOCK entries from PC's subnodes (large body etc.)
            vector<SlEntry> slEntries;

            // 5a. Promoted body subnodes (from MailPcResult).
            // Bodies > 8176 bytes (HTML newsletters, large RFC headers)
            // are chained through XBLOCK / XXBLOCK by schedulePayload.
            for (auto& s : pc.subnodes) {
                const Bid sBid = schedulePayload(std::move(s.bytes));
                slEntries.emplace_back(s.nid, sBid, Bid{0u});
            }

            // 5b. Recipient TC (combined To/Cc/Bcc).
            //
            // Per [MS-OXCMSG] §2.2.1.47.1 every message MUST own a recipient
            // table at NID 0x692, even an outgoing autoreply with no
            // recipients. A missing 0x692 subnode causes Outlook to flag
            // the message as malformed at open time and may suppress the
            // entire reading-pane preview.
            vector<graph::Recipient> allRecipients;
            allRecipients.reserve(m.toRecipients.size() + m.ccRecipients.size()
                                + m.bccRecipients.size());
            for (const auto& r : m.toRecipients)  allRecipients.push_back(r);
            for (const auto& r : m.ccRecipients)  allRecipients.push_back(r);
            for (const auto& r : m.bccRecipients) allRecipients.push_back(r);

            {
                auto recipTc = buildRecipientTc(allRecipients);
                const Bid recipBid = scheduleDataBlock(std::move(recipTc.hnBytes));
                slEntries.emplace_back(Nid{0x00000692u}, recipBid, Bid{0u});
            }

            // 5c. Attachments
            if (!m.attachments.empty()) {
                vector<AttachmentTcRow> attRows;
                attRows.reserve(m.attachments.size());

                // First, pre-allocate attachment subnode NIDs so the
                // attachment TC rows can carry them.
                vector<Nid> attNids;
                attNids.reserve(m.attachments.size());
                for (size_t i = 0; i < m.attachments.size(); ++i) {
                    // Subnode NID: NidType::Attachment, idx=(i+1).
                    // These NIDs live in the message's subnode tree
                    // namespace; they don't collide with top-level NBT.
                    attNids.push_back(Nid(NidType::Attachment,
                                          static_cast<uint32_t>(i + 1)));
                }

                for (size_t i = 0; i < m.attachments.size(); ++i) {
                    AttachmentTcRow row;
                    row.attachmentNid = attNids[i];
                    row.attachment    = &m.attachments[i];
                    attRows.push_back(row);
                }
                auto attTc = buildAttachmentTc(attRows);
                const Bid attTcBid = scheduleDataBlock(std::move(attTc.hnBytes));
                slEntries.emplace_back(Nid{0x00000671u}, attTcBid, Bid{0u});

                // Per-attachment PC + (optionally) data subnode.
                for (size_t i = 0; i < m.attachments.size(); ++i) {
                    auto attPc = buildAttachmentPc(m.attachments[i], ctx);
                    const Bid attPcBid = scheduleDataBlock(std::move(attPc.hnBytes));
                    slEntries.emplace_back(attNids[i], attPcBid, Bid{0u});

                    // Attachment PC subnodes (large data) — schedule each
                    // through schedulePayload so multi-MB attachment
                    // contentBytes are chained via XBLOCK/XXBLOCK rather
                    // than truncated at the 8176-byte single-block cap.
                    for (auto& s : attPc.subnodes) {
                        const Bid sBid = schedulePayload(std::move(s.bytes));
                        slEntries.emplace_back(s.nid, sBid, Bid{0u});
                    }
                }
            }

            // 5d. Schedule the message PC's data block.
            const Bid msgPcBid = scheduleDataBlock(std::move(pc.hnBytes));

            // 5e. Build SLBLOCK if there are any subnode entries.
            Bid bidSub{0u};
            if (!slEntries.empty()) {
                // SLENTRY array must be sorted ascending by NID per
                // [MS-PST] §2.2.2.8.3.3.1.
                std::sort(slEntries.begin(), slEntries.end(),
                          [](const SlEntry& a, const SlEntry& b) {
                              return a.nid.value < b.nid.value;
                          });
                M7SlBlock slb;
                slb.bid     = allocInternalBid();
                slb.entries = std::move(slEntries);
                bidSub      = slb.bid;
                slBlocks.push_back(std::move(slb));
            }

            // 5f. NBT entry for the message.
            M7Node node;
            node.nid       = mr.messageNid;
            node.nidParent = mr.folder->folderNid;
            node.bidData   = msgPcBid;
            node.bidSub    = bidSub;
            nodes.push_back(node);
        }

        // ============================================================
        // 6. Encode all blocks and assemble M5DataBlockSpec list.
        // ============================================================
        // Block order: data blocks first (in the order scheduled), then
        // SLBLOCKs. IBs assigned sequentially with 64-byte alignment.
        // 0x4600 = 0x4400 (kIbAMap, M11-G) + 0x200 (AMap page).
        constexpr uint64_t kBlocksStart = 0x4600u;

        vector<M5DataBlockSpec> m5Blocks;
        vector<M5Node>          m5Nodes;
        m5Blocks.reserve(dataBlocks.size() + slBlocks.size() + xBlocks.size());
        m5Nodes.reserve(nodes.size());

        uint64_t cursorIb = kBlocksStart;

        for (const auto& blk : dataBlocks) {
            const auto encoded = buildDataBlock(
                blk.bodyBytes.data(), blk.bodyBytes.size(),
                blk.bid, Ib{cursorIb}, CryptMethod::Permute);
            M5DataBlockSpec spec;
            spec.bid          = blk.bid;
            spec.encodedBlock = encoded;
            spec.cb           = static_cast<uint16_t>(blk.bodyBytes.size());
            m5Blocks.push_back(std::move(spec));
            cursorIb += encoded.size();
        }

        // X/XXBLOCKs are written before SLBLOCKs (no inter-dependency,
        // ordering is just for deterministic file layout).
        for (const auto& xb : xBlocks) {
            vector<uint8_t> encoded;
            if (xb.cLevel == 1u) {
                encoded = buildXBlock(xb.childBids.data(),
                                      xb.childBids.size(),
                                      xb.lcbTotal, xb.bid, Ib{cursorIb});
            } else {
                encoded = buildXXBlock(xb.childBids.data(),
                                       xb.childBids.size(),
                                       xb.lcbTotal, xb.bid, Ib{cursorIb});
            }
            M5DataBlockSpec spec;
            spec.bid          = xb.bid;
            spec.encodedBlock = encoded;
            // XBLOCK / XXBLOCK structured-body size:
            //   kXBlockHeaderSize (= 8) + cEnt * kXBlockEntrySize (= 8)
            spec.cb = static_cast<uint16_t>(
                kXBlockHeaderSize + xb.childBids.size() * kXBlockEntrySize);
            m5Blocks.push_back(std::move(spec));
            cursorIb += encoded.size();
        }

        for (const auto& sl : slBlocks) {
            const auto encoded = buildSlBlock(
                sl.entries.data(), sl.entries.size(),
                sl.bid, Ib{cursorIb});
            M5DataBlockSpec spec;
            spec.bid          = sl.bid;
            spec.encodedBlock = encoded;
            // SLBLOCK structured-body size:
            //   8-byte header + cEnt * 24
            spec.cb = static_cast<uint16_t>(8 + sl.entries.size() * 24);
            m5Blocks.push_back(std::move(spec));
            cursorIb += encoded.size();
        }

        for (const auto& n : nodes) {
            M5Node mn;
            mn.nid       = n.nid;
            mn.bidData   = n.bidData;
            mn.bidSub    = n.bidSub;
            mn.nidParent = n.nidParent;
            m5Nodes.push_back(mn);
        }

        return writeM5Pst(config.path, m5Blocks, m5Nodes);
    } catch (const std::exception& e) {
        return { false, std::string("writeM7Pst: ") + e.what() };
    } catch (...) {
        return { false, "writeM7Pst: unknown exception" };
    }
}

} // namespace pstwriter
