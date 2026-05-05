// pstwriter/src/contact.cpp
//
// M8 — Contact builders. Implementations.

#include "contact.hpp"

#include "block.hpp"
#include "graph_contact.hpp"
#include "graph_convert.hpp"
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
// Local PidTag catalog used by buildContactPc. Values verified against
// [MS-OXPROPS] (canonical names + tag IDs). PropTypes follow the _W
// (Unicode) convention for string-bearing tags.
// ----------------------------------------------------------------------------
namespace pid_contact {

constexpr uint16_t kMessageClass            = 0x001Au;
constexpr uint16_t kSubject                 = 0x0037u;   // Display-name fallback
constexpr uint16_t kCreationTime            = 0x3007u;
constexpr uint16_t kLastModificationTime    = 0x3008u;

// Personal-info props
constexpr uint16_t kDisplayName             = 0x3001u;
constexpr uint16_t kAddressType             = 0x3002u;
constexpr uint16_t kEmailAddress            = 0x3003u;

constexpr uint16_t kGeneration              = 0x3A05u;
constexpr uint16_t kGivenName               = 0x3A06u;
constexpr uint16_t kBusinessTelephone       = 0x3A08u;
constexpr uint16_t kHomeTelephone           = 0x3A09u;
constexpr uint16_t kInitials                = 0x3A0Au;
constexpr uint16_t kSurname                 = 0x3A11u;
constexpr uint16_t kPostalAddress           = 0x3A15u;
constexpr uint16_t kCompanyName             = 0x3A16u;
constexpr uint16_t kJobTitle                = 0x3A17u;   // PR_TITLE
constexpr uint16_t kDepartmentName          = 0x3A18u;
constexpr uint16_t kOfficeLocation          = 0x3A19u;
constexpr uint16_t kMobileTelephone         = 0x3A1Cu;
constexpr uint16_t kBusinessFax             = 0x3A24u;
constexpr uint16_t kBusinessAddrCountry     = 0x3A26u;
constexpr uint16_t kBusinessAddrCity        = 0x3A27u;
constexpr uint16_t kBusinessAddrState       = 0x3A28u;
constexpr uint16_t kBusinessAddrStreet      = 0x3A29u;
constexpr uint16_t kBusinessAddrPostalCode  = 0x3A2Au;
constexpr uint16_t kWeddingAnniversary      = 0x3A41u;
constexpr uint16_t kBirthday                = 0x3A42u;
constexpr uint16_t kMiddleName              = 0x3A44u;
constexpr uint16_t kDisplayNamePrefix       = 0x3A45u;   // Graph 'title'
constexpr uint16_t kProfession              = 0x3A46u;
constexpr uint16_t kNickname                = 0x3A4Fu;
constexpr uint16_t kHomeAddrCity            = 0x3A59u;
constexpr uint16_t kHomeAddrCountry         = 0x3A5Au;
constexpr uint16_t kHomeAddrPostalCode      = 0x3A5Bu;
constexpr uint16_t kHomeAddrState           = 0x3A5Cu;
constexpr uint16_t kHomeAddrStreet          = 0x3A5Du;
constexpr uint16_t kBusinessHomePage        = 0x3A51u;
constexpr uint16_t kPersonalNotes           = 0x6671u;   // PR_PERSONAL_HOME_PAGE proxy
                                                          // (real MAPI: PidTagBody_W 0x1000)

} // namespace pid_contact

// ----------------------------------------------------------------------------
// PropBuilder — same shape as mail.cpp's PropBuilder. Locally duplicated
// to avoid coupling. M10 refactor can DRY.
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

// Snapshot subnodes (same helper as mail.cpp).
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

// Build a single-line postal-address string from a Graph PhysicalAddress.
// Used to populate PidTagPostalAddress_W when the address has anything.
string formatPostalAddress(const graph::PhysicalAddress& a)
{
    string out;
    auto append = [&](const string& part) {
        if (part.empty()) return;
        if (!out.empty()) out += "\r\n";
        out += part;
    };
    append(a.street);
    {
        string line;
        if (!a.city.empty())       line += a.city;
        if (!a.state.empty())     {
            if (!line.empty()) line += ", ";
            line += a.state;
        }
        if (!a.postalCode.empty()) {
            if (!line.empty()) line += " ";
            line += a.postalCode;
        }
        append(line);
    }
    append(a.countryOrRegion);
    return out;
}

vector<uint8_t> u16le(const string& s)
{
    return graph::utf8ToUtf16le(s);
}

} // namespace

// ============================================================================
// buildContactPc
// ============================================================================
MailPcResult buildContactPc(const graph::GraphContact& c,
                            const MailPcBuildContext&  ctx)
{
    if (ctx.subnodeStart.type() == NidType::HID) {
        throw std::invalid_argument(
            "buildContactPc: ctx.subnodeStart must have nidType != HID");
    }

    PropBuilder pb;

    pb.addUnicodeString(pid_contact::kMessageClass, "IPM.Contact");

    // Display name: prefer Graph's displayName, fall back to "given surname".
    string displayName = c.displayName;
    if (displayName.empty()) {
        if (!c.givenName.empty() && !c.surname.empty())
            displayName = c.givenName + " " + c.surname;
        else if (!c.givenName.empty())
            displayName = c.givenName;
        else
            displayName = c.surname;
    }
    pb.addUnicodeString(pid_contact::kDisplayName, displayName);
    pb.addUnicodeString(pid_contact::kSubject,     displayName);

    // Personal-info props
    pb.addUnicodeString(pid_contact::kGivenName,         c.givenName);
    pb.addUnicodeString(pid_contact::kSurname,           c.surname);
    pb.addUnicodeString(pid_contact::kMiddleName,        c.middleName);
    pb.addUnicodeString(pid_contact::kNickname,          c.nickName);
    pb.addUnicodeString(pid_contact::kInitials,          c.initials);
    pb.addUnicodeString(pid_contact::kGeneration,        c.generation);
    pb.addUnicodeString(pid_contact::kDisplayNamePrefix, c.title);
    pb.addUnicodeString(pid_contact::kJobTitle,          c.jobTitle);
    pb.addUnicodeString(pid_contact::kCompanyName,       c.companyName);
    pb.addUnicodeString(pid_contact::kDepartmentName,    c.department);
    pb.addUnicodeString(pid_contact::kOfficeLocation,    c.officeLocation);
    pb.addUnicodeString(pid_contact::kProfession,        c.profession);
    pb.addUnicodeString(pid_contact::kBusinessHomePage,  c.businessHomePage);

    // Phones
    if (!c.businessPhones.empty())
        pb.addUnicodeString(pid_contact::kBusinessTelephone, c.businessPhones.front());
    if (!c.homePhones.empty())
        pb.addUnicodeString(pid_contact::kHomeTelephone, c.homePhones.front());
    pb.addUnicodeString(pid_contact::kMobileTelephone, c.mobilePhone);

    // Email (first only — see KNOWN_UNVERIFIED M8-1)
    if (!c.emailAddresses.empty()) {
        pb.addUnicodeString(pid_contact::kEmailAddress,
                            c.emailAddresses.front().address);
        pb.addUnicodeString(pid_contact::kAddressType, "SMTP");
    }

    // Birthday / anniversary
    if (!c.birthday.empty())
        pb.addSystemTime(pid_contact::kBirthday, graph::isoToFiletimeTicks(c.birthday));
    if (!c.anniversary.empty())
        pb.addSystemTime(pid_contact::kWeddingAnniversary,
                         graph::isoToFiletimeTicks(c.anniversary));

    // Times
    if (!c.createdDateTime.empty())
        pb.addSystemTime(pid_contact::kCreationTime,
                         graph::isoToFiletimeTicks(c.createdDateTime));
    if (!c.lastModifiedDateTime.empty())
        pb.addSystemTime(pid_contact::kLastModificationTime,
                         graph::isoToFiletimeTicks(c.lastModifiedDateTime));

    // Business address
    pb.addUnicodeString(pid_contact::kBusinessAddrStreet,     c.businessAddress.street);
    pb.addUnicodeString(pid_contact::kBusinessAddrCity,       c.businessAddress.city);
    pb.addUnicodeString(pid_contact::kBusinessAddrState,      c.businessAddress.state);
    pb.addUnicodeString(pid_contact::kBusinessAddrPostalCode, c.businessAddress.postalCode);
    pb.addUnicodeString(pid_contact::kBusinessAddrCountry,    c.businessAddress.countryOrRegion);

    // Home address
    pb.addUnicodeString(pid_contact::kHomeAddrStreet,     c.homeAddress.street);
    pb.addUnicodeString(pid_contact::kHomeAddrCity,       c.homeAddress.city);
    pb.addUnicodeString(pid_contact::kHomeAddrState,      c.homeAddress.state);
    pb.addUnicodeString(pid_contact::kHomeAddrPostalCode, c.homeAddress.postalCode);
    pb.addUnicodeString(pid_contact::kHomeAddrCountry,    c.homeAddress.countryOrRegion);

    // Concatenated postal address for non-MAPI consumers (PR_POSTAL_ADDRESS).
    const string concat = formatPostalAddress(c.businessAddress);
    if (!concat.empty())
        pb.addUnicodeString(pid_contact::kPostalAddress, concat);

    const auto& props = pb.props();
    PcResult pc = buildPropertyContext(props.data(), props.size(), ctx.subnodeStart);

    MailPcResult out;
    out.hnBytes  = std::move(pc.hnBytes);
    out.subnodes = snapshotSubnodes(pc.subnodes);
    return out;
}

// ============================================================================
// writeM8Pst — Phase C end-to-end PST writer for contacts.
//
// Structure mirrors writeM7Pst but with simpler per-item layout
// (contacts don't have recipients, attachments, or message-tree
// subnodes by default).
// ============================================================================
namespace {

// One node we want to land in the final PST.
struct M8NodeBuild {
    Nid             nid;
    Nid             nidParent;
    Bid             bidData;
    Bid             bidSub;
    vector<uint8_t> bodyBytes;
};

struct M8DataBlock {
    Bid             bid;
    vector<uint8_t> bodyBytes;
};

} // namespace

WriteResult writeM8Pst(const M8PstConfig& config) noexcept
{
    try {
        uint64_t nextDataBidIdx = 1u;
        auto allocDataBid = [&]() noexcept {
            return Bid::makeData(nextDataBidIdx++);
        };

        M5Allocator alloc;

        // Storage for per-folder UTF-16-LE buffers (must outlive schema).
        vector<vector<uint8_t>> folderBufStore;
        folderBufStore.reserve(config.folders.size() * 2);

        const Nid kDummySub{0x00000041u};

        vector<M8NodeBuild> nodes;
        vector<M8DataBlock> dataBlocks;

        auto scheduleNode = [&](Nid nid, Nid parent,
                                vector<uint8_t> body,
                                Bid bidSub = Bid{0u}) {
            M8DataBlock b;
            b.bid       = allocDataBid();
            b.bodyBytes = body;
            const Bid bidData = b.bid;
            dataBlocks.push_back(std::move(b));

            M8NodeBuild n;
            n.nid       = nid;
            n.nidParent = parent;
            n.bidData   = bidData;
            n.bidSub    = bidSub;
            n.bodyBytes = std::move(body);
            nodes.push_back(std::move(n));
        };

        // 1. The 24 §2.7.1 mandatory nodes (excluding 0x802D/0x802E/0x802F
        // which are folder-list dependent — added by caller below).
        for (auto& e : buildPstBaselineEntries(config.providerUid,
                                                config.pstDisplayName))
        {
            scheduleNode(e.nid, e.nidParent, std::move(e.body));
        }

        // 2. Pre-register reserved §2.7.1 NIDs into allocator.
        registerBaselineReservedNids(alloc);

        // ============================================================
        // 3. Allocate user-folder NIDs + sibling-table NIDs.
        // ============================================================
        struct FolderRecord {
            const M8ContactFolder* src;
            Nid                    folderNid;
            Nid                    hierarchyNid;
            Nid                    contentsNid;
            Nid                    faiNid;
            uint32_t               contentCount {0u};
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
            rec.contentCount = static_cast<uint32_t>(f.contacts.size());
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
        // 6. Per contact: contact PC. No subnodes for M8 Phase C.
        // ============================================================
        for (auto& rec : folderRecs) {
            for (const auto* c : rec.src->contacts) {
                if (c == nullptr) continue;
                const Nid contactNid = alloc.allocate(NidType::NormalMessage);

                MailPcBuildContext ctx;
                ctx.providerUid  = config.providerUid;
                ctx.subnodeStart = Nid{(contactNid.value & ~uint32_t{0x1Fu}) + 0x10000u + 0x1u};

                MailPcResult pc = buildContactPc(*c, ctx);
                // M8 Phase C: contact PC has no subnodes (we emit only
                // top-level scalar PidTags). Drop pc.subnodes.
                scheduleNode(contactNid, rec.folderNid, std::move(pc.hnBytes));
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
        return { false, std::string("writeM8Pst: ") + e.what() };
    } catch (...) {
        return { false, "writeM8Pst: unknown exception" };
    }
}

} // namespace pstwriter
