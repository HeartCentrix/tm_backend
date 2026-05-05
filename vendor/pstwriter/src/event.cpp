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

    // Times — non-named PidTag mirrors of named appointment props.
    if (!e.start.dateTime.empty()) {
        pb.addSystemTime(pid_event::kStartDate,
                         graph::isoToFiletimeTicks(toIso(e.start)));
    }
    if (!e.end.dateTime.empty()) {
        pb.addSystemTime(pid_event::kEndDate,
                         graph::isoToFiletimeTicks(toIso(e.end)));
    }

    // Server-time bookkeeping
    if (!e.createdDateTime.empty())
        pb.addSystemTime(pid_event::kCreationTime,
                         graph::isoToFiletimeTicks(e.createdDateTime));
    if (!e.lastModifiedDateTime.empty())
        pb.addSystemTime(pid_event::kLastModificationTime,
                         graph::isoToFiletimeTicks(e.lastModifiedDateTime));

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

} // namespace

WriteResult writeM9Pst(const M9PstConfig& config) noexcept
{
    try {
        uint64_t nextDataBidIdx = 1u;
        auto allocDataBid = [&]() noexcept {
            return Bid::makeData(nextDataBidIdx++);
        };

        M5Allocator alloc;

        vector<vector<uint8_t>> folderBufStore;
        folderBufStore.reserve(config.folders.size() * 2);

        const Nid kDummySub{0x00000041u};

        vector<M9NodeBuild> nodes;
        vector<M9DataBlock> dataBlocks;

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
        };

        // 1. Mandatory baseline nodes (excludes 0x802D/0x802E/0x802F).
        for (auto& e : buildPstBaselineEntries(config.providerUid,
                                                config.pstDisplayName))
        {
            scheduleNode(e.nid, e.nidParent, std::move(e.body));
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
        }

        // ============================================================
        // 4. Per folder: PC + sibling tables.
        // ============================================================
        for (auto& rec : folderRecs) {
            folderBufStore.push_back(u16le(rec.src->displayName));
            const auto& nameBuf = folderBufStore.back();
            folderBufStore.push_back(u16le(rec.src->containerClass));
            const auto& ccBuf = folderBufStore.back();

            M7FolderSchema schema{};
            schema.displayNameUtf16le    = nameBuf.data();
            schema.displayNameSize       = nameBuf.size();
            schema.contentCount          = rec.contentCount;
            schema.contentUnreadCount    = 0u;
            schema.hasSubfolders         = false;
            schema.containerClassUtf16le = ccBuf.data();
            schema.containerClassSize    = ccBuf.size();

            auto pc = buildMailFolderPc(schema, kDummySub);
            scheduleNode(rec.folderNid, rec.src->parentNid, std::move(pc.hnBytes));

            // Sibling tables (HIER/CONTENTS/FAI) NBTENTRYs carry
            // nidParent = 0 per [MS-PST] §3.12, confirmed via Aspose
            // oracle. See KNOWN_UNVERIFIED.md M11-D.
            scheduleNode(rec.hierarchyNid, Nid{0u},
                         buildFolderHierarchyTc(nullptr, 0).hnBytes);
            scheduleNode(rec.contentsNid, Nid{0u},
                         buildFolderContentsTc().hnBytes);
            scheduleNode(rec.faiNid, Nid{0u},
                         buildFolderFaiContentsTc().hnBytes);
        }

        // ============================================================
        // 5. IPM Subtree Hierarchy TC.
        // ============================================================
        {
            vector<HierarchyTcRow> ipmHier;
            ipmHier.reserve(folderRecs.size());
            for (size_t i = 0; i < folderRecs.size(); ++i) {
                HierarchyTcRow row;
                row.rowId              = folderRecs[i].folderNid;
                const auto& nameBuf    = folderBufStore[2 * i];
                row.displayNameUtf16le = nameBuf.data();
                row.displayNameSize    = nameBuf.size();
                row.contentCount       = folderRecs[i].contentCount;
                row.contentUnreadCount = 0u;
                row.hasSubfolders      = false;
                ipmHier.push_back(row);
            }
            const HierarchyTcRow* rowsPtr = ipmHier.empty() ? nullptr : ipmHier.data();
            auto tc = buildFolderHierarchyTc(rowsPtr, ipmHier.size());
            scheduleNode(Nid{0x0000802Du}, Nid{0u}, std::move(tc.hnBytes));
        }

        // ============================================================
        // 6. Per event: event PC. No subnodes for M9 Phase C.
        // ============================================================
        for (auto& rec : folderRecs) {
            for (const auto* ev : rec.src->events) {
                if (ev == nullptr) continue;
                const Nid eventNid = alloc.allocate(NidType::NormalMessage);

                MailPcBuildContext ctx;
                ctx.providerUid  = config.providerUid;
                ctx.subnodeStart = Nid{(eventNid.value & ~uint32_t{0x1Fu}) + 0x10000u + 0x1u};

                MailPcResult pc = buildEventPc(*ev, ctx);
                scheduleNode(eventNid, rec.folderNid, std::move(pc.hnBytes));
            }
        }

        // ============================================================
        // 7. Encode all blocks + assemble M5DataBlockSpec list.
        // ============================================================
        constexpr uint64_t kBlocksStart = 0x4600u;  // M11-G: blocks live after AMap[0] @ 0x4400

        vector<M5DataBlockSpec> m5Blocks;
        vector<M5Node>          m5Nodes;
        m5Blocks.reserve(dataBlocks.size());
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
