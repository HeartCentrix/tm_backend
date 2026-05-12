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
#include <unordered_map>
#include <utility>
#include <vector>

using std::array;
using std::string;
using std::vector;

namespace pstwriter {

// ============================================================================
// buildContactFolderPc — IPF.Contact folder PC wrapper. The universal
// property envelope (DisplayName, content counts, search key, design id,
// etc.) is emitted by buildFolderPcExtended in messaging.cpp; this
// wrapper just supplies the contact-specific PidTagContainerClass bytes.
// See M7FolderSchema in mail.hpp for the input shape.
// ============================================================================
PcResult buildContactFolderPc(const M7FolderSchema& schema,
                              Nid                   firstSubnodeNid)
{
    return buildFolderPcExtended(schema, firstSubnodeNid,
                                 kContainerClassContact.data(),
                                 kContainerClassContact.size());
}

namespace {

// ----------------------------------------------------------------------------
// Local PidTag catalog used by buildContactPc. Values verified against
// [MS-OXPROPS] (canonical names + tag IDs). PropTypes follow the _W
// (Unicode) convention for string-bearing tags.
// ----------------------------------------------------------------------------
namespace pid_contact {

constexpr uint16_t kImportance              = 0x0017u;
constexpr uint16_t kMessageClass            = 0x001Au;
constexpr uint16_t kSensitivity             = 0x0036u;
constexpr uint16_t kSubject                 = 0x0037u;   // Display-name fallback
constexpr uint16_t kMessageFlags            = 0x0E07u;
constexpr uint16_t kMessageSize             = 0x0E08u;
constexpr uint16_t kHasAttachments          = 0x0E1Bu;
constexpr uint16_t kIconIndex               = 0x1080u;   // 512 for default contact icon
constexpr uint16_t kCreationTime            = 0x3007u;
constexpr uint16_t kLastModificationTime    = 0x3008u;
constexpr uint16_t kInternetCpid            = 0x3FDEu;   // 65001 = UTF-8
constexpr uint16_t kMessageLocaleId         = 0x3FF1u;   // 1033 = en-US

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

    // Emit a Unicode string from raw UTF-16LE bytes (no string-to-utf16
    // conversion). Used for PR_SUBJECT where we need to prepend the
    // 2-char `\x01\x01` normalized-subject-prefix marker per
    // [MS-OXCMSG] §3.3.3.1 — addUnicodeString takes UTF-8 and can't
    // express the prefix.
    void addUnicodeRaw(uint16_t tag, vector<uint8_t> utf16leBytes)
    {
        if (utf16leBytes.empty()) return;
        const uint8_t* ptr  = utf16leBytes.data();
        const size_t   size = utf16leBytes.size();
        bufs_.emplace_back(std::move(utf16leBytes));
        addProp(tag, PropType::Unicode, ptr, size);
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

    // PR_SUBJECT for contacts uses the [MS-OXCMSG] §3.3.3.1 normalized-
    // subject format: 2 UTF-16 code units `U+0001 U+0001` followed by
    // the subject text. Outlook-exported contacts.pst byte-diff (this
    // file's reference) shows exactly that layout (`\x01\x01Akshat
    // Verma`). Without the prefix, Outlook treats the subject as
    // malformed and may hide the contact from the People view even
    // though the message lands in the destination Contacts folder.
    {
        auto subjectUtf16 = graph::utf8ToUtf16le(displayName);
        std::vector<uint8_t> prefixed;
        prefixed.reserve(4 + subjectUtf16.size());
        prefixed.push_back(0x01); prefixed.push_back(0x00); // U+0001
        prefixed.push_back(0x01); prefixed.push_back(0x00); // U+0001
        prefixed.insert(prefixed.end(),
                        subjectUtf16.begin(), subjectUtf16.end());
        pb.addUnicodeRaw(pid_contact::kSubject, std::move(prefixed));
    }

    // M12.3 — message-envelope minimum aligned to real-Outlook
    // contacts.pst byte-diff. REF has only 9 of these envelope props
    // on the PC (Importance, Sensitivity, MessageClass, Subject,
    // MessageFlags, MessageSize, DisplayName, plus 0x300B/0x3007/
    // 0x3008/0x3FDE/0x3FF1). Dropped from our PC because REF omits
    // them on IPM.Contact:
    //   * 0x0057 PR_MESSAGE_TO_ME, 0x0058 PR_MESSAGE_CC_ME
    //   * 0x0E17 PR_MESSAGE_STATUS, 0x0E08 PR_MESSAGE_SIZE
    //   * 0x0E1B PR_HASATTACH (mail-style, not used by People view)
    //   * 0x1080 PR_ICON_INDEX (Outlook computes from MessageClass)
    //   * 0x3002 PR_ADDRTYPE, 0x3003 PR_EMAIL_ADDRESS (REF uses
    //     PSETID_Address named props only)
    //   * 0x3013 PR_CHANGE_KEY (REF omits)
    // The Contents TC row paired with this PC sets
    // emitOptionalFixedCells=false to skip CEB bits 2/10/13/14,
    // matching this PC's "absent" state on those tags.
    pb.addInt32  (pid_contact::kImportance,     1u);          // Normal
    pb.addInt32  (pid_contact::kSensitivity,    0u);          // None
    // PR_MESSAGE_FLAGS = 0x09 (mfRead | mfUnsent) per REF; IPM.Contact
    // items are locally-created (never "sent") so mfUnsent applies.
    pb.addInt32  (pid_contact::kMessageFlags,   0x00000009u); // mfRead | mfUnsent
    // PR_INTERNET_CPID = 20127 (US-ASCII) per REF. We used to emit
    // 65001 (UTF-8) for parity with mail; Outlook's contact items
    // canonically carry the US-ASCII codepage.
    pb.addInt32  (pid_contact::kInternetCpid,   20127u);
    pb.addInt32  (pid_contact::kMessageLocaleId, 1033u);      // en-US

    // M12.5 — REVERTED M12.4's "Outlook-canonical envelope" block.
    // The PR_LTP_PARENT_NID=0 emission caused libpff (and presumably
    // Outlook) to treat the contact as having no valid parent and
    // route it to the orphan/recovered tree; pffexport on the M12.4
    // PST showed the contact landing in /tmp/ours.recovered/ instead
    // of under the Contacts folder. The other 11 additions
    // (PR_CREATOR_NAME, PR_MESSAGE_CODEPAGE, etc.) didn't help with
    // Import either, so reverting back to M12.3 minimum.

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

    // M11 named properties under PSETID_Address. The earlier attempt to
    // emit these tripped Outlook's repair dialog, but the actual
    // culprit (revealed by scanpst log) was missing PR_SEARCH_KEY /
    // PR_CREATION_TIME / PR_LAST_MODIFICATION_TIME on the contact PC,
    // not the named props. Re-enabled now that those are stub-emitted
    // below. Outlook's People view reads these to render the contact
    // card / sort order.
    if (!displayName.empty()) {
        pb.addUnicodeString(kLidFileUnder, displayName);
    }
    if (!c.emailAddresses.empty()) {
        const auto& addr = c.emailAddresses.front().address;
        const auto& nm   = c.emailAddresses.front().name;
        const std::string& displayForEmail = nm.empty() ? displayName : nm;
        if (!displayForEmail.empty()) {
            pb.addUnicodeString(kLidEmail1DisplayName,         displayForEmail);
            pb.addUnicodeString(kLidEmail1OriginalDisplayName, displayForEmail);
        }
        if (!addr.empty()) {
            pb.addUnicodeString(kLidEmail1AddressType,  "SMTP");
            pb.addUnicodeString(kLidEmail1EmailAddress, addr);
        }
    }

    // PR_SEARCH_KEY (0x300B) — must match the value the parent
    // folder's Contents TC row carries (scanpst flags "row doesn't
    // match sub-object" otherwise). M12.3 — switched from
    // deriveSearchKey (literal "SMTP:<UPPER(addr)>" truncated to 16B)
    // to deriveMessageSearchKey (FNV-1a 16B hash) so the bytes match
    // what real-Outlook contacts.pst stores (opaque hash, not literal
    // text). Same function as mail/event PCs use; both contact PC and
    // the matching TC row emit the same hash from the same seed.
    {
        std::string seed;
        if (!c.emailAddresses.empty() && !c.emailAddresses.front().address.empty()) {
            seed = c.emailAddresses.front().address;
        } else {
            seed = displayName;
        }
        const auto sk = graph::deriveMessageSearchKey(seed);
        pb.addBinary(0x300Bu, vector<uint8_t>(sk.begin(), sk.end()));
    }

    // Birthday / anniversary
    if (!c.birthday.empty())
        pb.addSystemTime(pid_contact::kBirthday, graph::isoToFiletimeTicks(c.birthday));
    if (!c.anniversary.empty())
        pb.addSystemTime(pid_contact::kWeddingAnniversary,
                         graph::isoToFiletimeTicks(c.anniversary));

    // Times — scanpst log: "Missing PR_CREATION_TIME" and "Missing
    // PR_LAST_MODIFICATION_TIME" are hard errors. Always emit; if
    // Graph didn't supply a value, stub with FILETIME 0 (1601-01-01).
    if (!c.createdDateTime.empty()) {
        pb.addSystemTime(pid_contact::kCreationTime,
                         graph::isoToFiletimeTicks(c.createdDateTime));
    } else {
        pb.addSystemTime(pid_contact::kCreationTime, 0ull);
    }
    if (!c.lastModifiedDateTime.empty()) {
        pb.addSystemTime(pid_contact::kLastModificationTime,
                         graph::isoToFiletimeTicks(c.lastModifiedDateTime));
    } else {
        pb.addSystemTime(pid_contact::kLastModificationTime, 0ull);
    }

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

// A scheduled SLBLOCK (subnode index for one node) — same shape as the
// M9 calendar writer. Folder bidSubs reference these so the import wizard
// can recurse into Hierarchy/Contents/FAI tables.
struct M8SlBlock {
    Bid             bid;
    vector<SlEntry> entries;
};

} // namespace

WriteResult writeM8Pst(const M8PstConfig& config) noexcept
{
    try {
        uint64_t nextDataBidIdx     = 1u;
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
        vector<M8SlBlock>   slBlocks;
        // nid → bidData lookup; retained for future folder-bidSub work
        // (the M11 minimal revert dropped the post-pass; see comments in
        // the per-folder loop). scheduleNode keeps writing to it so we
        // can re-enable wiring without rewiring the helper.
        std::unordered_map<uint32_t, Bid> nidToBid;

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
            nidToBid[nid.value] = bidData;
        };

        // Round B: empty basic queue node — bidData=0, no block. See mail.cpp.
        auto scheduleEmptyQueue = [&](Nid nid, Nid parent) {
            M8NodeBuild n;
            n.nid       = nid;
            n.nidParent = parent;
            n.bidData   = Bid{0u};
            n.bidSub    = Bid{0u};
            nodes.push_back(std::move(n));
        };

        // 1. The 24 §2.7.1 mandatory nodes (excluding 0x802D/0x802E/0x802F
        // which are folder-list dependent — added by caller below).
        for (auto& e : buildPstBaselineEntries(config.providerUid,
                                                config.pstDisplayName))
        {
            if (e.isEmptyQueue) {
                scheduleEmptyQueue(e.nid, e.nidParent);
            } else {
                scheduleNode(e.nid, e.nidParent, std::move(e.body));
            }
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

        // Pre-allocate NIDs for every contact up front so each folder's
        // Contents TC can carry one populated row per contact (with
        // PidTagLtpRowId = contact NID). Outlook's Contacts view
        // enumerates the Contents TC and renders rows from its display
        // columns; an empty Contents TC produces a blank Contacts view
        // even when the contact PCs exist in the NBT.
        struct ContactRecord {
            const graph::GraphContact* src;
            FolderRecord*              folder;
            Nid                        contactNid;
        };
        vector<ContactRecord> contactRecs;
        for (auto& rec : folderRecs) {
            for (const auto* c : rec.src->contacts) {
                if (c == nullptr) continue;
                ContactRecord cr;
                cr.src        = c;
                cr.folder     = &rec;
                cr.contactNid = alloc.allocate(NidType::NormalMessage);
                contactRecs.push_back(cr);
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

            // Container class ("IPF.Contact") is hardcoded by
            // buildContactFolderPc; ccBuf only feeds the IPM Subtree
            // hierarchy TC row built lower down (folderBufStore[2*i+1]).
            (void)ccBuf;

            M7FolderSchema schema{};
            schema.displayNameUtf16le    = nameBuf.data();
            schema.displayNameSize       = nameBuf.size();
            schema.contentCount          = rec.contentCount;
            schema.contentUnreadCount    = 0u;
            schema.hasSubfolders         = false;

            auto pc = buildContactFolderPc(schema, kDummySub);
            scheduleNode(rec.folderNid, rec.src->parentNid, std::move(pc.hnBytes));

            // Hierarchy TC: contacts folder has no children → 0 rows.
            // Sibling-table NBTENTRYs carry nidParent = 0 per
            // [MS-PST] §3.12 (Aspose oracle, see KNOWN_UNVERIFIED M11-D).
            scheduleNode(rec.hierarchyNid, Nid{0u},
                         buildFolderHierarchyTc(nullptr, 0).hnBytes);

            // Per-folder Contents TC: one populated row per contact the
            // folder owns. Outlook's Contacts view enumerates this TC
            // and reads each row's display columns (Subject = display
            // name, LastModificationTime) to render the contact list.
            // An empty TC = "no contacts" even when the contact PCs
            // exist below in the NBT.
            //
            // Storage outlives the schedule call so raw pointers in
            // ContentsTcRow stay valid until buildFolderContentsTc has
            // emitted the row payloads. Reserve upfront so push_back
            // never reallocates and invalidates pointers we captured.
            struct ContactRowBuffers {
                vector<uint8_t> messageClass;
                vector<uint8_t> subject;
                vector<uint8_t> searchKey;
                vector<uint8_t> changeKey;
            };
            size_t folderContactCount = 0;
            for (const auto& cr : contactRecs) {
                if (cr.folder == &rec) ++folderContactCount;
            }
            vector<ContactRowBuffers> ctcBufs;  ctcBufs.reserve(folderContactCount);
            vector<ContentsTcRow>     ctcRows;  ctcRows.reserve(folderContactCount);
            for (const auto& cr : contactRecs) {
                if (cr.folder != &rec) continue;
                const graph::GraphContact& c = *cr.src;

                // Display name fallback chain matches buildContactPc.
                string display = c.displayName;
                if (display.empty()) {
                    if (!c.givenName.empty() && !c.surname.empty())
                        display = c.givenName + " " + c.surname;
                    else if (!c.givenName.empty())
                        display = c.givenName;
                    else
                        display = c.surname;
                }

                ctcBufs.push_back({});
                auto& b = ctcBufs.back();
                b.messageClass = u16le("IPM.Contact");
                // Subject MUST match the contact PC's PR_SUBJECT value
                // byte-for-byte (scanpst flags "row doesn't match
                // sub-object" otherwise). buildContactPc emits PR_SUBJECT
                // with the [MS-OXCMSG] §3.3.3.1 `\x01\x01` normalized-
                // subject prefix; emit the same shape here.
                if (!display.empty()) {
                    auto subjectUtf16 = graph::utf8ToUtf16le(display);
                    b.subject.reserve(4 + subjectUtf16.size());
                    b.subject.push_back(0x01); b.subject.push_back(0x00);
                    b.subject.push_back(0x01); b.subject.push_back(0x00);
                    b.subject.insert(b.subject.end(),
                                     subjectUtf16.begin(), subjectUtf16.end());
                }
                // PR_SEARCH_KEY must match the PC's 0x300B (see
                // buildContactPc). M12.3 — switched to
                // deriveMessageSearchKey to match REF's opaque-hash
                // bytes. Both row and PC use the same seed/function.
                {
                    std::string seed;
                    if (!c.emailAddresses.empty()
                        && !c.emailAddresses.front().address.empty()) {
                        seed = c.emailAddresses.front().address;
                    } else {
                        seed = display;
                    }
                    const auto sk = graph::deriveMessageSearchKey(seed);
                    b.searchKey.assign(sk.begin(), sk.end());
                }
                // PR_CHANGE_KEY (0x3013) — M12.3 — DROP. Outlook-exported
                // contacts.pst byte-diff (2026-05-12) shows real-Outlook
                // IPM.Contact PCs DO NOT carry PR_CHANGE_KEY. Setting
                // the row CEB bit 19 with a 22-byte stub while the PC
                // omits the tag is precisely what triggered "row
                // doesn't match sub-object". b.changeKey stays empty;
                // row.changeKeyBytes = nullptr → CEB bit 19 cleared.

                ContentsTcRow row{};
                row.rowId         = cr.contactNid;
                row.rowVer        = 0u;
                row.importance    = 1;   // Normal
                row.sensitivity   = 0;
                row.messageStatus = 0;
                row.messageFlags  = 0x00000009u;   // mfRead | mfUnsent (matches PC, see buildContactPc)
                row.messageSize   = 0;
                row.messageToMe   = false;
                row.messageCcMe   = false;
                // M12.3 — skip CEB bits 2/10/13/14 for MessageStatus /
                // MessageSize / MessageToMe / MessageCcMe. The contact
                // PC drops the matching tags so the row must too,
                // otherwise scanpst flags "row doesn't match
                // sub-object" + "Failed to add row to the FLT".
                row.emitOptionalFixedCells = false;
                if (!c.lastModifiedDateTime.empty()) {
                    row.lastModificationTime =
                        graph::isoToFiletimeTicks(c.lastModifiedDateTime);
                }
                row.messageClassUtf16le         = b.messageClass.data();
                row.messageClassSize            = b.messageClass.size();
                row.subjectUtf16le              = b.subject.empty() ? nullptr : b.subject.data();
                row.subjectSize                 = b.subject.size();
                row.searchKeyBytes              = b.searchKey.data();
                row.searchKeySize               = b.searchKey.size();
                row.changeKeyBytes              = b.changeKey.data();
                row.changeKeySize               = b.changeKey.size();
                ctcRows.push_back(row);
            }
            // Build Contents TC with row-matrix promotion DISABLED
            // (firstSubnodeNid = 0). For contact volumes the row
            // matrix + varlen HN allocations stay well under the 8176
            // HN block cap (≈118 B/row + ≤200 B varlen), so promotion
            // is unnecessary and the resulting Contents TC has no
            // subnodes. Avoids the bidSub wiring that — in M11 testing
            // — caused Outlook's import wizard to flag "errors
            // detected" even though the same wiring works in M7 mail
            // and M9 calendar (root cause TBD).
            //
            // If buildFolderContentsTc throws std::length_error for a
            // large enough contact set, the caller (pst_export.py's
            // bisect-retry loop) will split the chunk and retry.
            auto ctcTc = buildFolderContentsTc(
                ctcRows.empty() ? nullptr : ctcRows.data(),
                ctcRows.size(),
                Nid{0u});
            scheduleNode(rec.contentsNid, Nid{0u},
                         std::move(ctcTc.hnBytes));

            scheduleNode(rec.faiNid, Nid{0u},
                         buildFolderFaiContentsTc().hnBytes);
        }

        // ============================================================
        // 5. IPM Subtree Hierarchy TC.
        //
        // scanpst log: "Adding folder (nid=8062) back to the database"
        // — Deleted Items (NID 0x8062) is emitted as a baseline folder
        // PC parented to IPM Subtree (0x8022), but the Hierarchy TC at
        // 0x802D listed only the user folders. scanpst then treats
        // Deleted Items as orphaned. Inject a row for it. (M7 mail
        // writer does the same; M11-N on mail.cpp.)
        // ============================================================
        {
            static const auto kDeletedItemsName = u16le("Deleted Items");
            vector<HierarchyTcRow> ipmHier;
            ipmHier.reserve(folderRecs.size() + 1u);

            // Deleted Items first (baseline-emitted folder).
            {
                HierarchyTcRow row{};
                row.rowId                 = Nid{0x00008062u};
                row.displayNameUtf16le    = kDeletedItemsName.data();
                row.displayNameSize       = kDeletedItemsName.size();
                row.containerClassUtf16le = nullptr;  // matches baseline PC
                row.containerClassSize    = 0;
                row.contentCount          = 0u;
                row.contentUnreadCount    = 0u;
                row.hasSubfolders         = false;
                ipmHier.push_back(row);
            }

            // Contact folders. containerClass MUST match the folder PC
            // (we emit "IPF.Contact" on the PC). scanpst log
            // "Hierarchy Table for 8022, row doesn't match sub-object"
            // was raised because the row had CEB-cleared containerClass
            // while the PC carried "IPF.Contact" — mismatch.
            for (size_t i = 0; i < folderRecs.size(); ++i) {
                HierarchyTcRow row{};
                row.rowId                 = folderRecs[i].folderNid;
                const auto& nameBuf       = folderBufStore[2 * i];
                const auto& ccBuf         = folderBufStore[2 * i + 1];
                row.displayNameUtf16le    = nameBuf.data();
                row.displayNameSize       = nameBuf.size();
                row.containerClassUtf16le = ccBuf.empty() ? nullptr : ccBuf.data();
                row.containerClassSize    = ccBuf.size();
                row.contentCount          = folderRecs[i].contentCount;
                row.contentUnreadCount    = 0u;
                row.hasSubfolders         = false;
                ipmHier.push_back(row);
            }
            const HierarchyTcRow* rowsPtr = ipmHier.empty() ? nullptr : ipmHier.data();
            auto tc = buildFolderHierarchyTc(rowsPtr, ipmHier.size());
            scheduleNode(Nid{0x0000802Du}, Nid{0u}, std::move(tc.hnBytes));
        }

        // ============================================================
        // 6. Per contact: contact PC. Reuses the NIDs we pre-allocated
        //    when building the per-folder Contents TC rows so each row's
        //    PidTagLtpRowId resolves to the contact PC's NBT entry.
        // ============================================================
        for (auto& cr : contactRecs) {
            MailPcBuildContext ctx;
            ctx.providerUid  = config.providerUid;
            ctx.subnodeStart = Nid{(cr.contactNid.value & ~uint32_t{0x1Fu}) + 0x10000u + 0x1u};

            MailPcResult pc = buildContactPc(*cr.src, ctx);
            scheduleNode(cr.contactNid, cr.folder->folderNid, std::move(pc.hnBytes));
        }

        // ============================================================
        // 7. Encode all blocks + assemble M5DataBlockSpec list.
        // ============================================================
        // Blocks live AFTER AMap[0] @ 0x4400 + PMap[0] @ 0x4600 (Round-A).
        // Must match writer.cpp::kBlocksStart — wSig (bid XOR ib at build
        // time) must equal what Outlook computes from the file offset.
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
        return { false, std::string("writeM8Pst: ") + e.what() };
    } catch (...) {
        return { false, "writeM8Pst: unknown exception" };
    }
}

} // namespace pstwriter
