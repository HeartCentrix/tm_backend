// pstwriter/src/event.cpp
//
// M9 — Calendar/event builders. Implementations.

#include "event.hpp"

#include "block.hpp"
#include "graph_convert.hpp"
#include "graph_event.hpp"
#include "ltp.hpp"
#include "m5_allocator.hpp"
#include "mail.hpp"
#include "messaging.hpp"
#include "pst_baseline.hpp"
#include "types.hpp"
#include "writer.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <string>
#include <utility>
#include <vector>

using std::array;
using std::string;
using std::vector;

namespace pstwriter {

namespace {

// ----------------------------------------------------------------------------
// Local PidTag catalog used by buildEventPc.
// ----------------------------------------------------------------------------
namespace pid_event {

constexpr uint16_t kImportance               = 0x0017u;
constexpr uint16_t kMessageClass             = 0x001Au;
constexpr uint16_t kSensitivity              = 0x0036u;
constexpr uint16_t kSubject                  = 0x0037u;
constexpr uint16_t kStartDate                = 0x0060u;
constexpr uint16_t kEndDate                  = 0x0061u;
constexpr uint16_t kSentRepresentingName     = 0x0042u;
constexpr uint16_t kSentRepresentingAddrType = 0x0064u;
constexpr uint16_t kSentRepresentingEmail    = 0x0065u;
constexpr uint16_t kSentRepresentingEntryId  = 0x0041u;
constexpr uint16_t kSentRepresentingSearchKey= 0x003Bu;

constexpr uint16_t kSenderName               = 0x0C1Au;
constexpr uint16_t kSenderEntryId            = 0x0C19u;
constexpr uint16_t kSenderSearchKey          = 0x0C1Du;
constexpr uint16_t kSenderAddressType        = 0x0C1Eu;
constexpr uint16_t kSenderEmailAddress       = 0x0C1Fu;

constexpr uint16_t kBody                     = 0x1000u;
constexpr uint16_t kBodyHtml                 = 0x1013u;

constexpr uint16_t kCreationTime             = 0x3007u;
constexpr uint16_t kLastModificationTime     = 0x3008u;

constexpr uint16_t kHasAttachments           = 0x0E1Bu;
constexpr uint16_t kMessageFlags             = 0x0E07u;

} // namespace pid_event

// ----------------------------------------------------------------------------
// PropBuilder — same shape as mail.cpp / contact.cpp.
// ----------------------------------------------------------------------------
class PropBuilder {
public:
    PropBuilder()
    {
        bufs_.reserve(64);
        scalar_.reserve(64);
        wide_.reserve(64);
        props_.reserve(64);
    }

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
        if (utf8.empty()) return;
        auto bytes = graph::utf8ToUtf16le(utf8);
        const uint8_t* ptr  = bytes.data();
        const size_t   size = bytes.size();
        bufs_.emplace_back(std::move(bytes));
        addProp(tag, PropType::Unicode, ptr, size);
    }

    void addBinary(uint16_t tag, vector<uint8_t> bytes)
    {
        if (bytes.empty()) return;
        const uint8_t* ptr  = bytes.data();
        const size_t   size = bytes.size();
        bufs_.emplace_back(std::move(bytes));
        addProp(tag, PropType::Binary, ptr, size);
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

    vector<vector<uint8_t>>     bufs_;
    vector<array<uint8_t, 4>>   scalar_;
    vector<array<uint8_t, 8>>   wide_;
    vector<PcProperty>          props_;
};

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

vector<uint8_t> u16le(const string& s)
{
    return graph::utf8ToUtf16le(s);
}

// Compose a Graph dateTimeTimeZone into an ISO-8601 string suitable for
// graph::isoToFiletimeTicks. Graph's `dateTime` field comes WITHOUT a
// trailing 'Z' or offset. When `timeZone == "UTC"`, append 'Z'; for any
// non-UTC zone we treat as UTC (the spec's offset would require a tz
// database — out of scope for M9).
string toIso(const graph::DateTimeTimeZone& dtz)
{
    if (dtz.dateTime.empty()) return "";
    if (dtz.timeZone == "UTC") return dtz.dateTime + "Z";
    // Non-UTC: assume UTC; KNOWN_UNVERIFIED M9-2 (timezone handling).
    return dtz.dateTime + "Z";
}

} // namespace

// ============================================================================
// buildEventPc
// ============================================================================
MailPcResult buildEventPc(const graph::GraphEvent&  e,
                          const MailPcBuildContext& ctx)
{
    if (ctx.subnodeStart.type() == NidType::HID) {
        throw std::invalid_argument(
            "buildEventPc: ctx.subnodeStart must have nidType != HID");
    }

    PropBuilder pb;

    // Message class
    pb.addUnicodeString(pid_event::kMessageClass, "IPM.Appointment");

    // Subject + body
    if (!e.subject.empty())
        pb.addUnicodeString(pid_event::kSubject, e.subject);

    if (e.body.contentType == graph::BodyType::Text && !e.body.content.empty()) {
        pb.addUnicodeString(pid_event::kBody, e.body.content);
    } else if (e.body.contentType == graph::BodyType::Html && !e.body.content.empty()) {
        const auto& s = e.body.content;
        vector<uint8_t> bytes(s.begin(), s.end());  // raw UTF-8
        pb.addBinary(pid_event::kBodyHtml, std::move(bytes));
        if (!e.bodyPreview.empty())
            pb.addUnicodeString(pid_event::kBody, e.bodyPreview);
    }

    // Importance / Sensitivity
    pb.addInt32(pid_event::kImportance,
                static_cast<uint32_t>(e.importance));
    // Sensitivity: Graph doesn't expose directly; emit 0 (normal) by
    // default. M10 hardening could read Graph's `sensitivity` field
    // (when added to the Graph schema).
    pb.addInt32(pid_event::kSensitivity, 0u);

    // Times — emit both the legacy PidTag mirrors and the canonical
    // named appointment properties. Outlook's Calendar UI reads only
    // the named variants (PidLidAppointmentStartWhole / EndWhole); the
    // legacy 0x0060 / 0x0061 tags are kept for tools that follow
    // [MS-OXPROPS] property aliasing.
    //
    // Local IDs come from messaging.hpp::kLid* (assigned in the
    // Name-to-Id Map registered at NID 0x61 by buildNameToIdMapPc).
    uint64_t startTicks = 0;
    uint64_t endTicks   = 0;
    if (!e.start.dateTime.empty()) {
        startTicks = graph::isoToFiletimeTicks(toIso(e.start));
        pb.addSystemTime(pid_event::kStartDate, startTicks);
        pb.addSystemTime(kLidAppointmentStartWhole, startTicks);
    }
    if (!e.end.dateTime.empty()) {
        endTicks = graph::isoToFiletimeTicks(toIso(e.end));
        pb.addSystemTime(pid_event::kEndDate, endTicks);
        pb.addSystemTime(kLidAppointmentEndWhole, endTicks);
    }

    // PidLidAppointmentDuration (minutes between start and end).
    // [MS-OXOCAL] §2.2.1.7 — integer minutes; readers fall back to
    // computing from start/end when absent but populating it lets
    // Outlook's grid render duration columns without re-deriving.
    if (startTicks != 0 && endTicks > startTicks) {
        // 100-ns ticks → minutes: 1 minute = 60s = 600_000_000 ticks.
        const uint32_t durationMinutes = static_cast<uint32_t>(
            (endTicks - startTicks) / 600000000ull);
        pb.addInt32(kLidAppointmentDuration, durationMinutes);
    }

    // PidLidAppointmentSubType — boolean "is all-day event".
    pb.addBoolean(kLidAppointmentSubType, e.isAllDay);

    // PidLidLocation — single-line location text. Prefer the canonical
    // `location.displayName`; some Graph payloads only carry
    // `locations[0]` so fall back to that.
    {
        const std::string* locStr = nullptr;
        if (!e.location.displayName.empty()) {
            locStr = &e.location.displayName;
        } else if (!e.locations.empty() && !e.locations.front().displayName.empty()) {
            locStr = &e.locations.front().displayName;
        }
        if (locStr) {
            pb.addUnicodeString(kLidLocation, *locStr);
        }
    }

    // Server-time bookkeeping — always emit. scanpst rejects messages
    // missing PR_CREATION_TIME / PR_LAST_MODIFICATION_TIME (see
    // M11/M12 contact fix). Stub with FILETIME 0 when Graph didn't
    // supply a value.
    if (!e.createdDateTime.empty()) {
        pb.addSystemTime(pid_event::kCreationTime,
                         graph::isoToFiletimeTicks(e.createdDateTime));
    } else {
        pb.addSystemTime(pid_event::kCreationTime, 0ull);
    }
    if (!e.lastModifiedDateTime.empty()) {
        pb.addSystemTime(pid_event::kLastModificationTime,
                         graph::isoToFiletimeTicks(e.lastModifiedDateTime));
    } else {
        pb.addSystemTime(pid_event::kLastModificationTime, 0ull);
    }

    // PR_SEARCH_KEY — scanpst rejects "Missing PR_SEARCH_KEY" AND
    // raises "row doesn't match sub-object" if the value differs from
    // the Contents-TC-row's. Derive from the event identity so both
    // sides produce the same bytes. iCalUId is the canonical event
    // identity in Graph; falls back to subject + start.
    {
        std::string seed = e.iCalUId;
        if (seed.empty()) seed = e.subject + "|" + e.start.dateTime;
        const auto sk = graph::deriveMessageSearchKey(seed);
        pb.addBinary(0x300Bu, vector<uint8_t>(sk.begin(), sk.end()));
    }

    // Has-attachments
    pb.addBoolean(pid_event::kHasAttachments, e.hasAttachments);
    // MessageFlags: minimal — read flag = 1 when not draft.
    {
        uint32_t flags = 0u;
        if (!e.isDraft) flags |= 0x00000001u; // mfRead
        pb.addInt32(pid_event::kMessageFlags, flags);
    }

    // Organizer (treated as sender / sent representing)
    if (e.hasOrganizer) {
        if (!e.organizer.name.empty()) {
            pb.addUnicodeString(pid_event::kSenderName, e.organizer.name);
            pb.addUnicodeString(pid_event::kSentRepresentingName, e.organizer.name);
        }
        if (!e.organizer.address.empty()) {
            pb.addUnicodeString(pid_event::kSenderEmailAddress,    e.organizer.address);
            pb.addUnicodeString(pid_event::kSentRepresentingEmail, e.organizer.address);
            pb.addUnicodeString(pid_event::kSenderAddressType,        "SMTP");
            pb.addUnicodeString(pid_event::kSentRepresentingAddrType, "SMTP");

            const auto entryId = graph::makeOneOffEntryId(e.organizer.name,
                                                          e.organizer.address);
            const auto searchK = graph::deriveSearchKey(e.organizer.address);
            pb.addBinary(pid_event::kSenderEntryId, entryId);
            pb.addBinary(pid_event::kSentRepresentingEntryId,
                         vector<uint8_t>(entryId.begin(), entryId.end()));
            pb.addBinary(pid_event::kSenderSearchKey,
                         vector<uint8_t>(searchK.begin(), searchK.end()));
            pb.addBinary(pid_event::kSentRepresentingSearchKey,
                         vector<uint8_t>(searchK.begin(), searchK.end()));
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
// writeM9Pst — Phase C end-to-end PST writer for events.
//
// Mirrors writeM8Pst structure (per-item simple PC, no subnodes per
// event for M9 minimum). 27-node baseline duplicated; M10 refactor.
// ============================================================================
namespace {

struct M9NodeBuild {
    Nid             nid;
    Nid             nidParent;
    Bid             bidData;
    Bid             bidSub;
    vector<uint8_t> bodyBytes;
};

struct M9DataBlock {
    Bid             bid;
    vector<uint8_t> bodyBytes;
};

// A scheduled SLBLOCK (subnode index for one node) — same shape as
// M7SlBlock in mail.cpp. Folder bidSubs reference these so the import
// wizard can recurse into Hierarchy/Contents/FAI tables.
struct M9SlBlock {
    Bid                bid;
    vector<SlEntry>    entries;
};

} // namespace

WriteResult writeM9Pst(const M9PstConfig& config) noexcept
{
    try {
        uint64_t nextDataBidIdx     = 1u;
        uint64_t nextInternalBidIdx = 1u;
        auto allocDataBid = [&]() noexcept {
            return Bid::makeData(nextDataBidIdx++);
        };
        auto allocInternalBid = [&]() noexcept {
            return Bid::makeInternal(nextInternalBidIdx++);
        };

        M5Allocator alloc;

        vector<vector<uint8_t>> folderBufStore;
        folderBufStore.reserve(config.folders.size() * 2);

        const Nid kDummySub{0x00000041u};

        vector<M9NodeBuild> nodes;
        vector<M9DataBlock> dataBlocks;
        vector<M9SlBlock>   slBlocks;
        // nid → bidData lookup for the folder-bidSub post-pass below.
        std::unordered_map<uint32_t, Bid> nidToBid;

        auto scheduleNode = [&](Nid nid, Nid parent,
                                vector<uint8_t> body,
                                Bid bidSub = Bid{0u}) {
            M9DataBlock b;
            b.bid       = allocDataBid();
            b.bodyBytes = body;
            const Bid bidData = b.bid;
            dataBlocks.push_back(std::move(b));

            M9NodeBuild n;
            n.nid       = nid;
            n.nidParent = parent;
            n.bidData   = bidData;
            n.bidSub    = bidSub;
            n.bodyBytes = std::move(body);
            nodes.push_back(std::move(n));
            nidToBid[nid.value] = bidData;
        };

        // Round B: empty basic queue node — bidData=0, no block. See mail.cpp.
        auto scheduleEmptyQueue = [&](Nid nid, Nid parent) {
            M9NodeBuild n;
            n.nid       = nid;
            n.nidParent = parent;
            n.bidData   = Bid{0u};
            n.bidSub    = Bid{0u};
            nodes.push_back(std::move(n));
        };

        // 1. Mandatory baseline nodes (excludes 0x802D/0x802E/0x802F).
        for (auto& e : buildPstBaselineEntries(config.providerUid,
                                                config.pstDisplayName))
        {
            if (e.isEmptyQueue) {
                scheduleEmptyQueue(e.nid, e.nidParent);
            } else {
                scheduleNode(e.nid, e.nidParent, std::move(e.body));
            }
        }

        // 2. Pre-register reserved NIDs into the allocator.
        registerBaselineReservedNids(alloc);

        // ============================================================
        // 3. Allocate user-folder NIDs.
        // ============================================================
        struct FolderRecord {
            const M9CalendarFolder* src;
            Nid                     folderNid;
            Nid                     hierarchyNid;
            Nid                     contentsNid;
            Nid                     faiNid;
            uint32_t                contentCount {0u};
        };
        vector<FolderRecord> folderRecs;
        folderRecs.reserve(config.folders.size());

        // path → assigned NID (mirrors writeM7Pst). Used to resolve
        // M9CalendarFolder.parentPath back to the parent folder's
        // allocated NID after this loop, so nested calendar folders
        // ("Calendar/Default", "Calendar/Holidays") render under the
        // right parent in Outlook instead of flat under IPM Subtree.
        std::unordered_map<std::string, Nid> pathToFolderNid;

        for (const auto& f : config.folders) {
            FolderRecord rec;
            rec.src       = &f;
            rec.folderNid = alloc.allocate(NidType::NormalFolder);
            const uint32_t idx = rec.folderNid.index();
            rec.hierarchyNid = Nid(NidType::HierarchyTable,     idx);
            rec.contentsNid  = Nid(NidType::ContentsTable,      idx);
            rec.faiNid       = Nid(NidType::AssocContentsTable, idx);
            alloc.registerExternal(rec.hierarchyNid);
            alloc.registerExternal(rec.contentsNid);
            alloc.registerExternal(rec.faiNid);
            rec.contentCount = static_cast<uint32_t>(f.events.size());
            folderRecs.push_back(rec);
            if (!f.path.empty()) {
                pathToFolderNid[f.path] = rec.folderNid;
            }
        }

        // Pre-allocate NIDs for every event up front so each folder's
        // Contents TC can carry one populated row per event (with
        // PidTagLtpRowId = event NID). Outlook's calendar view
        // enumerates contents from the Contents TC and reads each row's
        // display columns; an empty Contents TC produces a blank
        // calendar even when the message PCs exist in the NBT.
        struct EventRecord {
            const graph::GraphEvent* src;
            FolderRecord*            folder;
            Nid                      eventNid;
        };
        vector<EventRecord> eventRecs;
        for (auto& rec : folderRecs) {
            for (const auto* ev : rec.src->events) {
                if (!ev) continue;
                EventRecord er;
                er.src      = ev;
                er.folder   = &rec;
                er.eventNid = alloc.allocate(NidType::NormalMessage);
                eventRecs.push_back(er);
            }
        }

        // ============================================================
        // 4. Per folder: PC + sibling tables.
        // ============================================================
        for (auto& rec : folderRecs) {
            folderBufStore.push_back(u16le(rec.src->displayName));
            const auto& nameBuf = folderBufStore.back();
            folderBufStore.push_back(u16le(rec.src->containerClass));
            const auto& ccBuf = folderBufStore.back();

            // hasSubfolders flag must reflect reality so Outlook's
            // tree control draws the expand-arrow on calendar
            // containers. Walk siblings once per folder.
            bool anyChild = false;
            if (!rec.src->path.empty()) {
                for (const auto& g : config.folders) {
                    if (&g == rec.src) continue;
                    if (g.parentPath == rec.src->path) {
                        anyChild = true; break;
                    }
                }
            }

            M7FolderSchema schema{};
            schema.displayNameUtf16le    = nameBuf.data();
            schema.displayNameSize       = nameBuf.size();
            schema.contentCount          = rec.contentCount;
            schema.contentUnreadCount    = 0u;
            schema.hasSubfolders         = anyChild;
            schema.containerClassUtf16le = ccBuf.data();
            schema.containerClassSize    = ccBuf.size();

            // Resolve effective parent via parentPath lookup. Empty
            // parentPath falls back to the explicit parentNid the
            // caller supplied (legacy flat layout under IPM Subtree).
            Nid effectiveParentNid = rec.src->parentNid;
            if (!rec.src->parentPath.empty()) {
                auto it = pathToFolderNid.find(rec.src->parentPath);
                if (it != pathToFolderNid.end()) {
                    effectiveParentNid = it->second;
                }
            }

            auto pc = buildMailFolderPc(schema, kDummySub);
            scheduleNode(rec.folderNid, effectiveParentNid, std::move(pc.hnBytes));

            // Per-folder Hierarchy TC: list rows for any child folders
            // (those whose parentPath references this folder's path).
            // Empty TC for leaves. Without populated child rows, the
            // import wizard sees "Include subfolders" with no children
            // to recurse and silently skips them.
            std::vector<std::vector<uint8_t>> childNameBuffers;
            std::vector<HierarchyTcRow> childRows;
            if (!rec.src->path.empty()) {
                childNameBuffers.reserve(folderRecs.size() * 2);
                childRows.reserve(folderRecs.size());
                for (const auto& other : folderRecs) {
                    if (other.src == rec.src) continue;
                    if (other.src->parentPath != rec.src->path) continue;
                    childNameBuffers.push_back(u16le(other.src->displayName));
                    childNameBuffers.push_back(u16le(other.src->containerClass));
                    HierarchyTcRow row{};
                    row.rowId                 = other.folderNid;
                    row.displayNameUtf16le    = childNameBuffers[childNameBuffers.size() - 2].data();
                    row.displayNameSize       = childNameBuffers[childNameBuffers.size() - 2].size();
                    const auto& cc = childNameBuffers.back();
                    row.containerClassUtf16le = cc.empty() ? nullptr : cc.data();
                    row.containerClassSize    = cc.size();
                    row.contentCount          = other.contentCount;
                    row.contentUnreadCount    = 0u;
                    row.hasSubfolders         = false;
                    childRows.push_back(row);
                }
            }
            const HierarchyTcRow* childRowsPtr =
                childRows.empty() ? nullptr : childRows.data();
            scheduleNode(rec.hierarchyNid, rec.folderNid,
                         buildFolderHierarchyTc(childRowsPtr,
                                                childRows.size()).hnBytes);

            // Per-folder Contents TC: one populated row per event the
            // folder owns. Outlook's calendar view enumerates this TC
            // and reads each row's display columns (Subject, organizer,
            // LastModificationTime) to render the calendar grid. An
            // empty TC = "no appointments" even when the event PCs
            // exist below in the NBT.
            //
            // Storage outlives the schedule call so raw pointers in
            // ContentsTcRow stay valid until buildFolderContentsTc has
            // emitted the row payloads. We reserve upfront so push_back
            // never reallocates and invalidates pointers we already
            // captured.
            struct EvtRowBuffers {
                vector<uint8_t> messageClass;
                vector<uint8_t> subject;
                vector<uint8_t> sentRepresentingName;
                vector<uint8_t> searchKey;
                vector<uint8_t> changeKey;
            };
            size_t folderEventCount = 0;
            for (const auto& er : eventRecs) {
                if (er.folder == &rec) ++folderEventCount;
            }
            vector<EvtRowBuffers>  evtBufs;       evtBufs.reserve(folderEventCount);
            vector<ContentsTcRow>  evtRows;       evtRows.reserve(folderEventCount);
            for (const auto& er : eventRecs) {
                if (er.folder != &rec) continue;
                const graph::GraphEvent& e = *er.src;

                evtBufs.push_back({});
                auto& b = evtBufs.back();

                b.messageClass = u16le("IPM.Appointment");
                if (!e.subject.empty()) b.subject = u16le(e.subject);
                if (e.hasOrganizer && !e.organizer.name.empty()) {
                    b.sentRepresentingName = u16le(e.organizer.name);
                }
                // PR_SEARCH_KEY: derive deterministically from the
                // same seed buildEventPc uses, so the row's value and
                // the event PC's PR_SEARCH_KEY match byte-for-byte
                // (scanpst flags "row doesn't match sub-object" when
                // they diverge).
                {
                    std::string seed = e.iCalUId;
                    if (seed.empty()) seed = e.subject + "|" + e.start.dateTime;
                    const auto sk = graph::deriveMessageSearchKey(seed);
                    b.searchKey.assign(sk.begin(), sk.end());
                }
                // PR_CHANGE_KEY: 22-byte XID stub (Outlook accepts).
                b.changeKey.assign(22, 0u);

                ContentsTcRow row{};
                row.rowId        = er.eventNid;
                row.rowVer       = 0u;
                row.importance   = static_cast<int32_t>(e.importance);
                row.sensitivity  = 0;
                row.messageStatus= 0;
                row.messageFlags = e.isDraft ? 0u : 0x00000001u;   // mfRead
                row.messageSize  = 0;
                row.messageToMe  = false;
                row.messageCcMe  = false;
                if (!e.lastModifiedDateTime.empty()) {
                    row.lastModificationTime =
                        graph::isoToFiletimeTicks(e.lastModifiedDateTime);
                }
                row.messageClassUtf16le         = b.messageClass.data();
                row.messageClassSize            = b.messageClass.size();
                row.subjectUtf16le              = b.subject.empty() ? nullptr : b.subject.data();
                row.subjectSize                 = b.subject.size();
                row.sentRepresentingNameUtf16le = b.sentRepresentingName.empty()
                                                  ? nullptr : b.sentRepresentingName.data();
                row.sentRepresentingNameSize    = b.sentRepresentingName.size();
                row.searchKeyBytes              = b.searchKey.data();
                row.searchKeySize               = b.searchKey.size();
                row.changeKeyBytes              = b.changeKey.data();
                row.changeKeySize               = b.changeKey.size();
                evtRows.push_back(row);
            }
            // Row matrix subnode NID — same convention as mail.cpp uses
            // for the contents TC.
            const Nid kEvtRowMatrixSubnode{
                (static_cast<uint32_t>(NidType::LtpReserved)) | (1u << 5)};
            auto evtTc = buildFolderContentsTc(
                evtRows.empty() ? nullptr : evtRows.data(),
                evtRows.size(),
                kEvtRowMatrixSubnode);

            Bid evtContentsBidSub{0u};
            if (!evtTc.subnodes.empty()) {
                vector<SlEntry> contentsSl;
                contentsSl.reserve(evtTc.subnodes.size());
                for (auto& s : evtTc.subnodes) {
                    M9DataBlock db;
                    db.bid       = allocDataBid();
                    db.bodyBytes.assign(s.data, s.data + s.size);
                    const Bid sBid = db.bid;
                    dataBlocks.push_back(std::move(db));
                    contentsSl.emplace_back(s.nid, sBid, Bid{0u});
                }
                std::sort(contentsSl.begin(), contentsSl.end(),
                          [](const SlEntry& a, const SlEntry& b) {
                              return a.nid.value < b.nid.value;
                          });
                M9SlBlock slb;
                slb.bid     = allocInternalBid();
                slb.entries = std::move(contentsSl);
                evtContentsBidSub = slb.bid;
                slBlocks.push_back(std::move(slb));
            }
            // scheduleNode helper auto-allocates a data BID; manually
            // schedule here so we can attach the bidSub for the row
            // matrix subnode.
            {
                M9DataBlock b;
                b.bid       = allocDataBid();
                b.bodyBytes = evtTc.hnBytes;
                const Bid bidData = b.bid;
                dataBlocks.push_back(std::move(b));
                M9NodeBuild n;
                n.nid       = rec.contentsNid;
                n.nidParent = rec.folderNid;
                n.bidData   = bidData;
                n.bidSub    = evtContentsBidSub;
                n.bodyBytes = std::move(evtTc.hnBytes);
                nodes.push_back(std::move(n));
                nidToBid[rec.contentsNid.value] = bidData;
            }

            scheduleNode(rec.faiNid, rec.folderNid,
                         buildFolderFaiContentsTc().hnBytes);
        }

        // ============================================================
        // 5. IPM Subtree Hierarchy / Contents / FAI tables.
        // ============================================================
        {
            vector<HierarchyTcRow> ipmHier;
            ipmHier.reserve(folderRecs.size());
            for (size_t i = 0; i < folderRecs.size(); ++i) {
                // Nested folders appear in their parent's Hierarchy TC,
                // not in IPM Subtree's. Listing them here would make
                // Outlook show every folder as a direct child of "Top
                // of Personal Folders", losing the per-calendar grouping.
                if (!folderRecs[i].src->parentPath.empty()) continue;

                HierarchyTcRow row{};
                row.rowId              = folderRecs[i].folderNid;
                const auto& nameBuf    = folderBufStore[2 * i];
                const auto& ccBuf      = folderBufStore[2 * i + 1];
                row.displayNameUtf16le = nameBuf.data();
                row.displayNameSize    = nameBuf.size();
                row.containerClassUtf16le = ccBuf.empty() ? nullptr : ccBuf.data();
                row.containerClassSize    = ccBuf.size();
                row.contentCount       = folderRecs[i].contentCount;
                row.contentUnreadCount = 0u;
                bool anyChild = false;
                if (!folderRecs[i].src->path.empty()) {
                    for (const auto& g : folderRecs) {
                        if (g.src == folderRecs[i].src) continue;
                        if (g.src->parentPath == folderRecs[i].src->path) {
                            anyChild = true; break;
                        }
                    }
                }
                row.hasSubfolders = anyChild;
                ipmHier.push_back(row);
            }
            const HierarchyTcRow* rowsPtr = ipmHier.empty() ? nullptr : ipmHier.data();
            auto tc = buildFolderHierarchyTc(rowsPtr, ipmHier.size());
            scheduleNode(Nid{0x0000802Du}, Nid{0u}, std::move(tc.hnBytes));

            // IPM Subtree Contents (0x802E) + FAI (0x802F). The import
            // wizard walks IPM Subtree's bidSub looking for all three
            // when "Include subfolders" is checked at store root —
            // missing siblings make it return 0 imports even though
            // the user folders below are otherwise valid.
            scheduleNode(Nid{0x0000802Eu}, Nid{0u},
                         buildFolderContentsTc().hnBytes);
            scheduleNode(Nid{0x0000802Fu}, Nid{0u},
                         buildFolderFaiContentsTc().hnBytes);
        }

        // ============================================================
        // 6. Per event: event PC. Reuses the NIDs we pre-allocated
        //    when building the per-folder Contents TC rows so each
        //    row's PidTagLtpRowId resolves to the event PC's NBT entry.
        // ============================================================
        for (auto& er : eventRecs) {
            const graph::GraphEvent& ev = *er.src;

            MailPcBuildContext ctx;
            ctx.providerUid  = config.providerUid;
            ctx.subnodeStart = Nid{(er.eventNid.value & ~uint32_t{0x1Fu}) + 0x10000u + 0x1u};

            MailPcResult pc = buildEventPc(ev, ctx);
            scheduleNode(er.eventNid, er.folder->folderNid, std::move(pc.hnBytes));
        }

        // ============================================================
        // 6.5  Folder bidSub wiring (mirror writeM7Pst). Every folder
        // NID needs a subnode tree pointing to its Hierarchy/Contents/FAI
        // tables — otherwise the import wizard's "Include subfolders"
        // recursion at the store root sees the folder PC but can't
        // resolve its child tables and reports zero imports. Root
        // (0x122) is skipped; user folders + IPM Subtree (0x8022) get
        // wired here.
        // ============================================================
        for (auto& fnode : nodes) {
            if (fnode.nid.type() != NidType::NormalFolder) continue;
            if (fnode.nid.value == 0x00000122u) continue;
            const uint32_t idx = fnode.nid.index();
            const Nid hierNid{(idx << 5) | static_cast<uint32_t>(NidType::HierarchyTable)};
            const Nid contNid{(idx << 5) | static_cast<uint32_t>(NidType::ContentsTable)};
            const Nid faiNid {(idx << 5) | static_cast<uint32_t>(NidType::AssocContentsTable)};
            vector<SlEntry> entries;
            entries.reserve(3);
            if (auto it = nidToBid.find(hierNid.value); it != nidToBid.end()) {
                entries.emplace_back(hierNid, it->second, Bid{0u});
            }
            if (auto it = nidToBid.find(contNid.value); it != nidToBid.end()) {
                entries.emplace_back(contNid, it->second, Bid{0u});
            }
            if (auto it = nidToBid.find(faiNid.value); it != nidToBid.end()) {
                entries.emplace_back(faiNid, it->second, Bid{0u});
            }
            if (entries.empty()) continue;
            std::sort(entries.begin(), entries.end(),
                      [](const SlEntry& a, const SlEntry& b) {
                          return a.nid.value < b.nid.value;
                      });
            M9SlBlock fslb;
            fslb.bid     = allocInternalBid();
            fslb.entries = std::move(entries);
            fnode.bidSub = fslb.bid;
            slBlocks.push_back(std::move(fslb));
        }

        // ============================================================
        // 7. Encode all blocks + assemble M5DataBlockSpec list.
        // ============================================================
        // Blocks live AFTER AMap[0] @ 0x4400 + PMap[0] @ 0x4600 (Round-A).
        // Must match writer.cpp::kBlocksStart — block-trailer wSig
        // (= bid XOR ib at build time) must equal what Outlook computes
        // from the file offset.
        constexpr uint64_t kBlocksStart = 0x4800u;

        vector<M5DataBlockSpec> m5Blocks;
        vector<M5Node>          m5Nodes;
        m5Blocks.reserve(dataBlocks.size() + slBlocks.size());
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

        // SLBLOCKs after data blocks. Ordering is for deterministic
        // layout; the M5 layer indexes by BID so file position is free.
        for (const auto& sl : slBlocks) {
            const auto encoded = buildSlBlock(
                sl.entries.data(), sl.entries.size(),
                sl.bid, Ib{cursorIb});
            M5DataBlockSpec spec;
            spec.bid          = sl.bid;
            spec.encodedBlock = encoded;
            // SLBLOCK structured-body size: 8-byte header + 24-byte entries.
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
        return { false, std::string("writeM9Pst: ") + e.what() };
    } catch (...) {
        return { false, "writeM9Pst: unknown exception" };
    }
}

} // namespace pstwriter
