// pstwriter/src/pst_baseline.cpp
//
// M10 — Implementation of the shared 27-mandatory-nodes baseline.
// Replaces ~200 lines of identical code that previously lived in
// mail.cpp / contact.cpp / event.cpp.

#include "pst_baseline.hpp"

#include "graph_convert.hpp"
#include "messaging.hpp"
#include "types.hpp"

#include <array>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

using std::array;
using std::string;
using std::vector;

namespace pstwriter {

namespace {

vector<uint8_t> u16le(const string& s)
{
    return graph::utf8ToUtf16le(s);
}

PstBaselineEntry mkEntry(Nid nid, Nid parent, vector<uint8_t> body)
{
    PstBaselineEntry e;
    e.nid       = nid;
    e.nidParent = parent;
    e.body      = std::move(body);
    return e;
}

// Round B: empty queue node ([MS-PST] §2.4.8.3). NBT entry gets
// bidData=0 — no block in BBT. Body is necessarily empty.
PstBaselineEntry mkEmptyQueue(Nid nid, Nid parent)
{
    PstBaselineEntry e;
    e.nid          = nid;
    e.nidParent    = parent;
    e.isEmptyQueue = true;
    return e;
}

} // namespace

vector<PstBaselineEntry>
buildPstBaselineEntries(const array<uint8_t, 16>& providerUid,
                        const string&             pstDisplayName)
{
    vector<PstBaselineEntry> out;
    out.reserve(24);

    // Stable UTF-16-LE buffers used by FolderPcSchema / HierarchyTcRow
    // pointers. These must outlive the build calls below — they're
    // consumed inside this function so local lifetime is fine.
    const auto nameTopOfPersonal = u16le("Top of Personal Folders");
    const auto nameSearchRoot    = u16le("Search Root");
    const auto nameSpamSearch    = u16le("Spam Search Folder");
    const auto nameDeletedItems  = u16le("Deleted Items");
    const auto pstDisplay        = u16le(pstDisplayName);
    // M11-J P3: Root Folder (NID 0x122) per [MS-PST] §2.4.3 must carry
    // PR_DISPLAY_NAME or scanpst flags it missing. Reuse the PST display
    // name when set, falling back to "Outlook Data File" — the conventional
    // root folder name in real-Outlook PSTs.
    const auto nameRootFolder    = pstDisplay.empty()
                                     ? u16le("Outlook Data File")
                                     : pstDisplay;

    // firstSubnodeNid required by buildPropertyContext but unused here
    // (no subnode-promoted props in baseline schemas).
    const Nid kDummySub{0x00000041u};

    // 1. Message Store PC (0x21)
    {
        MessageStoreSchema mss{};
        mss.providerUid        = providerUid;
        mss.displayNameUtf16le = pstDisplay.empty() ? nullptr : pstDisplay.data();
        mss.displayNameSize    = pstDisplay.size();
        auto pc = buildMessageStorePc(mss, kDummySub);
        out.push_back(mkEntry(Nid{0x00000021u}, Nid{0u}, std::move(pc.hnBytes)));
    }

    // 2. NameToIdMap PC (0x61)
    {
        auto pc = buildNameToIdMapPc(kDummySub);
        out.push_back(mkEntry(Nid{0x00000061u}, Nid{0u}, std::move(pc.hnBytes)));
    }

    // 3. Root Folder PC (0x122; nidParent = self)
    {
        FolderPcSchema rs{};
        // M11-J P3: PR_DISPLAY_NAME populated so scanpst doesn't flag
        // "Folder (nid=122): Missing PR_DISPLAY_NAME".
        rs.displayNameUtf16le = nameRootFolder.data();
        rs.displayNameSize    = nameRootFolder.size();
        rs.hasSubfolders      = true;
        auto pc = buildFolderPc(rs, kDummySub);
        out.push_back(mkEntry(Nid{0x00000122u}, Nid{0x00000122u}, std::move(pc.hnBytes)));
    }

    // 4. Root Folder Hierarchy TC (0x12D) — 3 rows for Spam/IPM/Finder
    {
        HierarchyTcRow rootHier[3];
        rootHier[0].rowId              = Nid{0x00002223u};
        rootHier[0].displayNameUtf16le = nameSpamSearch.data();
        rootHier[0].displayNameSize    = nameSpamSearch.size();
        rootHier[0].hasSubfolders      = false;
        rootHier[1].rowId              = Nid{0x00008022u};
        rootHier[1].displayNameUtf16le = nameTopOfPersonal.data();
        rootHier[1].displayNameSize    = nameTopOfPersonal.size();
        rootHier[1].hasSubfolders      = true;
        rootHier[2].rowId              = Nid{0x00008042u};
        rootHier[2].displayNameUtf16le = nameSearchRoot.data();
        rootHier[2].displayNameSize    = nameSearchRoot.size();
        rootHier[2].hasSubfolders      = false;
        auto tc = buildFolderHierarchyTc(rootHier, 3);
        out.push_back(mkEntry(Nid{0x0000012Du}, Nid{0u}, std::move(tc.hnBytes)));
    }

    // 5-6. Root Contents + FAI Contents
    out.push_back(mkEntry(Nid{0x0000012Eu}, Nid{0u}, buildFolderContentsTc().hnBytes));
    out.push_back(mkEntry(Nid{0x0000012Fu}, Nid{0u}, buildFolderFaiContentsTc().hnBytes));

    // 6b. Receive Folder Table (0x0617) — minimal 1-row default-class
    //     mapping per [MS-PST] §2.4.5. Required by scanpst.exe; absent
    //     surfaces as "Receive folder table missing" / "missing default
    //     message class" errors. (Tier 2 ISSUE G.)
    //
    // M11-M (Tier 7) diagnostic toggle: setting the environment variable
    // PSTWRITER_OMIT_RFT=1 skips this emission entirely. Used to bisect
    // which scanpst error is the actual real-Outlook open blocker. Has
    // no effect on production builds (env var is unset by default).
    {
        const char* omitRft = std::getenv("PSTWRITER_OMIT_RFT");
        if (omitRft == nullptr || omitRft[0] == '0' || omitRft[0] == '\0') {
            // ROUND-15 FIX: NID 0x617 has nidType=0x17 which is INVALID
            // per [MS-PST] §2.4.1. Real Outlook (backup.pst byte-diff)
            // puts the Receive Folder Table at NID 0x62B = (idx 0x31 <<
            // 5) | 0x0B (NID_TYPE_RECEIVE_FOLDER_TABLE). We'd been
            // emitting at 0x617 since round 1 — scanpst's persistent
            // "Receive folder table missing" was because it couldn't
            // even parse our NID's type bits as an RFT.
            out.push_back(mkEntry(Nid{0x0000062Bu}, Nid{0u},
                                  buildReceiveFolderTableTc().hnBytes));
        }
    }

    // (Round 22 attempted adding 0x261, 0xC01, 0xE01, 0xE41, 0xEE1,
    //  0xF01, 0xF21 as minimal TCs — caused scanpst Fatal Error
    //  0x80040834 mid-walk. These NIDs need different (non-TC) shapes
    //  in real Outlook PSTs; emitting empty TCs there breaks scanpst's
    //  parser. Reverted.)

    // 6c. NID 0xEC1 (Internal idx=0x76) + sibling SUQ at 0xEC6.
    //
    // Round L (byte-diff vs backup.pst): EC1 is NOT an HN — it's a
    // raw 20-byte structure: { wFlags(2)=0; wSUDType(2)=9; NID(4)=
    // 0x2223 (the active search folder); padding(12)=0 }. We've been
    // emitting EC1 as a TC since round 1, which is why scanpst kept
    // flagging it "corrupt update queue". The structure resembles the
    // first 20 bytes of a SUD (§2.4.8.1.1).
    {
        std::vector<uint8_t> ec1Body(20, 0);
        ec1Body[0] = 0x00;  // wFlags low
        ec1Body[1] = 0x00;  // wFlags high
        ec1Body[2] = 0x09;  // wSUDType low (= 9, SUDT_SRCH_MOD or similar)
        ec1Body[3] = 0x00;  // wSUDType high
        ec1Body[4] = 0x23;  // NID 0x2223 LE byte 0
        ec1Body[5] = 0x22;  // byte 1
        ec1Body[6] = 0x00;  // byte 2
        ec1Body[7] = 0x00;  // byte 3
        // bytes [8..19]: 12 zero padding bytes (already zero)
        out.push_back(mkEntry(Nid{0x00000EC1u}, Nid{0u}, std::move(ec1Body)));
    }
    out.push_back(mkEmptyQueue(Nid{0x00000EC6u}, Nid{0u}));

    // 7-8. Search Management Queue (0x1E1) + Search Activity List (0x201).
    //
    //   SMQ (0x1E1) is a basic queue node ([MS-PST] §2.4.8.3) holding
    //   SUD items. Empty: bidData=0, nidParent=0 (Round B).
    //
    //   SAL (0x201) is NOT a queue — per §2.4.8.4.2 it's "a simple array
    //   of NIDs". Each NID is 4 bytes; cbData = 4 × number_of_entries.
    //   scanpst flags "SAL missing entry (nid=2223)" if the Spam Search
    //   Folder NID isn't in the array. Round D: emit SAL with one
    //   4-byte entry = 0x00002223 LE.
    out.push_back(mkEmptyQueue(Nid{0x000001E1u}, Nid{0u}));
    {
        std::vector<uint8_t> salBody(4, 0);
        salBody[0] = 0x23;
        salBody[1] = 0x22;
        salBody[2] = 0x00;
        salBody[3] = 0x00;
        out.push_back(mkEntry(Nid{0x00000201u}, Nid{0u}, std::move(salBody)));
    }

    // 8b. Search Domain Object (0x261, NID_SEARCH_DOMAIN_OBJECT).
    // Per [MS-PST] §2.4.8.4.3 SDO is "a simple array of NIDs that
    // collectively represent the global search domain of the PST".
    // scanpst flags "?? Deleting SDO" when absent — repair would create
    // or skip the SDO. Emit one entry referencing IPM Subtree (0x8022)
    // — the natural search domain for a minimal PST. Round 22 tried
    // emitting 0x261 as a TC and got Fatal Error 0x80040834 mid-walk;
    // this is the spec-correct shape (NID array, not TC).
    {
        std::vector<uint8_t> sdoBody(4, 0);
        sdoBody[0] = 0x22;
        sdoBody[1] = 0x80;
        sdoBody[2] = 0x00;
        sdoBody[3] = 0x00;
        out.push_back(mkEntry(Nid{0x00000261u}, Nid{0u}, std::move(sdoBody)));
    }

    // 9-14. Templates
    out.push_back(mkEntry(Nid{0x0000060Du}, Nid{0u}, buildFolderHierarchyTc(nullptr, 0).hnBytes));
    out.push_back(mkEntry(Nid{0x0000060Eu}, Nid{0u}, buildFolderContentsTc().hnBytes));
    out.push_back(mkEntry(Nid{0x0000060Fu}, Nid{0u}, buildFolderFaiContentsTc().hnBytes));
    out.push_back(mkEntry(Nid{0x00000610u}, Nid{0u}, buildSearchContentsTemplateTc().hnBytes));
    out.push_back(mkEntry(Nid{0x00000671u}, Nid{0u}, buildAttachmentTemplateTc().hnBytes));
    out.push_back(mkEntry(Nid{0x00000692u}, Nid{0u}, buildRecipientTemplateTc().hnBytes));

    // 14b. Outgoing Queue Table (NID 0x64C). Round 15 fixed the NID;
    //       round 16 adds the column schema scanpst requires (0x0E10
    //       and 0x0E14, both Int32). buildOutgoingQueueTc emits a
    //       0-row TC with these cells plus mandatory LtpRowId/RowVer.
    out.push_back(mkEntry(Nid{0x0000064Cu}, Nid{0u}, buildOutgoingQueueTc().hnBytes));

    // 14c. Additional template NIDs (0x6B6/0x6D7/0x6F8) that scanpst
    //      expects alongside 0x60D–0x610 / 0x671 / 0x692. Each carries
    //      a TC with a specific set of required columns per scanpst's
    //      "missing required column" output:
    //        0x6B6 — 0x0E370102 (Binary)
    //        0x6D7 — 0x0E3E0102 + 0x0E310102 (Binary)
    //        0x6F8 — 0x30070040 (SystemTime) + 0x0E330014 (Int64)
    //      Empty (0-row) TCs are sufficient — scanpst only checks that
    //      the column descriptors are present in TCINFO.
    out.push_back(mkEntry(Nid{0x000006B6u}, Nid{0u}, buildTemplate6B6Tc().hnBytes));
    out.push_back(mkEntry(Nid{0x000006D7u}, Nid{0u}, buildTemplate6D7Tc().hnBytes));
    out.push_back(mkEntry(Nid{0x000006F8u}, Nid{0u}, buildTemplate6F8Tc().hnBytes));

    // 15. Spam Search Folder (0x2223; parent = Root) + its sibling
    //     update queue (0x2226, type 0x06) and criteria object (0x2227,
    //     type 0x07). scanpst flagged "Search folder (nid=2226) missing
    //     update queue" / "Search folder (nid=2227) missing criteria
    //     object" when only the search folder PC was emitted. Both
    //     siblings share nidIndex 0x111 with the search folder.
    {
        FolderPcSchema ss{};
        ss.displayNameUtf16le = nameSpamSearch.data();
        ss.displayNameSize    = nameSpamSearch.size();
        auto pc = buildSearchFolderPc(ss, kDummySub);
        out.push_back(mkEntry(Nid{0x00002223u}, Nid{0x00000122u}, std::move(pc.hnBytes)));
    }
    // Update queue (NidType::SearchUpdateQueue = 0x06). Round B:
    // per [MS-PST] §2.4.8.6 a SUQ is a basic queue node, not a TC.
    // For queue nodes the NBT entry's nidParent field is overloaded as
    // a *byte offset to the head item* (not the logical parent NID).
    // For an empty queue this offset MUST be 0, otherwise scanpst reads
    // the value as a head pointer into a non-existent block and flags
    // "corrupt update queue". The 2226↔2223 search-folder linkage is
    // re-established via shared nidIndex (idx=0x111 for both NIDs), not
    // via nidParent.
    out.push_back(mkEmptyQueue(Nid{0x00002226u}, Nid{0u}));
    // Round 20: NID 0x2230 (idx 0x111 type 0x10 = SearchContentsTable)
    // is emitted by real Outlook (byte-diff vs backup.pst). Without it
    // scanpst flags Spam Search's update queue as "corrupt" because
    // there's no contents table for the search to populate. Use the
    // existing search-contents template shape.
    out.push_back(mkEntry(Nid{0x00002230u}, Nid{0x00002223u},
                          buildSearchContentsTemplateTc().hnBytes));
    // Criteria object (NidType::SearchCriteriaObject = 0x07). Round 5
    // emitted this as a TC (bClientSig=0x7C); scanpst's "missing search
    // flags" finding shows it expects a PC with PR_SEARCH_FOLDER_FLAGS.
    {
        auto pc = buildSearchCriteriaObjectPc(kDummySub);
        out.push_back(mkEntry(Nid{0x00002227u}, Nid{0x00002223u},
                              std::move(pc.hnBytes)));
    }

    // 16. IPM Subtree (0x8022; parent = Root)
    {
        FolderPcSchema ipm{};
        ipm.displayNameUtf16le = nameTopOfPersonal.data();
        ipm.displayNameSize    = nameTopOfPersonal.size();
        ipm.hasSubfolders      = true;
        auto pc = buildFolderPc(ipm, kDummySub);
        out.push_back(mkEntry(Nid{0x00008022u}, Nid{0x00000122u}, std::move(pc.hnBytes)));
    }

    // (NIDs 0x802D, 0x802E, 0x802F are EXCLUDED — caller builds them
    //  with user-folder rows in the Hierarchy TC.)

    // 20. Finder / Search Root (0x8042; parent = Root)
    {
        FolderPcSchema fr{};
        fr.displayNameUtf16le = nameSearchRoot.data();
        fr.displayNameSize    = nameSearchRoot.size();
        auto pc = buildFolderPc(fr, kDummySub);
        out.push_back(mkEntry(Nid{0x00008042u}, Nid{0x00000122u}, std::move(pc.hnBytes)));
    }
    out.push_back(mkEntry(Nid{0x0000804Du}, Nid{0u}, buildFolderHierarchyTc(nullptr, 0).hnBytes));
    out.push_back(mkEntry(Nid{0x0000804Eu}, Nid{0u}, buildFolderContentsTc().hnBytes));
    out.push_back(mkEntry(Nid{0x0000804Fu}, Nid{0u}, buildFolderFaiContentsTc().hnBytes));

    // 24. Deleted Items (0x8062; parent = IPM Subtree)
    {
        FolderPcSchema di{};
        di.displayNameUtf16le = nameDeletedItems.data();
        di.displayNameSize    = nameDeletedItems.size();
        auto pc = buildFolderPc(di, kDummySub);
        out.push_back(mkEntry(Nid{0x00008062u}, Nid{0x00008022u}, std::move(pc.hnBytes)));
    }
    out.push_back(mkEntry(Nid{0x0000806Du}, Nid{0u}, buildFolderHierarchyTc(nullptr, 0).hnBytes));
    out.push_back(mkEntry(Nid{0x0000806Eu}, Nid{0u}, buildFolderContentsTc().hnBytes));
    out.push_back(mkEntry(Nid{0x0000806Fu}, Nid{0u}, buildFolderFaiContentsTc().hnBytes));

    return out;
}

void registerBaselineReservedNids(M5Allocator& alloc)
{
    constexpr uint32_t kReserved[] = {
        0x0000012Du, 0x0000012Eu, 0x0000012Fu,
        0x00000261u,                          // Search Domain Object (Round D)
        0x0000060Du, 0x0000060Eu, 0x0000060Fu, 0x00000610u,
        0x0000062Bu,                          // ReceiveFolderTable (M11-N round 15: was 0x617)
        0x0000064Cu,                          // OutgoingQueueTable (M11-N round 15)
        0x00000671u, 0x00000692u,
        0x000006B6u, 0x000006D7u, 0x000006F8u,// Additional templates (M11-N)
        0x00000EC1u, 0x00000EC6u,             // Procmon-confirmed extra search nodes
        0x00002223u, 0x00002226u, 0x00002227u, 0x00002230u,  // Spam search family
        0x00008022u, 0x0000802Du, 0x0000802Eu, 0x0000802Fu,
        0x00008042u, 0x0000804Du, 0x0000804Eu, 0x0000804Fu,
        0x00008062u, 0x0000806Du, 0x0000806Eu, 0x0000806Fu,
    };
    for (uint32_t v : kReserved) {
        if (M5Allocator::isValidNidType(Nid{v}.type())) {
            if (!alloc.isAllocated(Nid{v})) {
                alloc.registerExternal(Nid{v});
            }
        }
    }
}

} // namespace pstwriter
