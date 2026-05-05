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
    out.push_back(mkEntry(Nid{0x00000617u}, Nid{0u},
                          buildReceiveFolderTableTc().hnBytes));

    // 7-8. Bare nodes
    out.push_back(mkEntry(Nid{0x000001E1u}, Nid{0u}, buildEmptyNodePayload()));
    out.push_back(mkEntry(Nid{0x00000201u}, Nid{0u}, buildEmptyNodePayload()));

    // 9-14. Templates
    out.push_back(mkEntry(Nid{0x0000060Du}, Nid{0u}, buildFolderHierarchyTc(nullptr, 0).hnBytes));
    out.push_back(mkEntry(Nid{0x0000060Eu}, Nid{0u}, buildFolderContentsTc().hnBytes));
    out.push_back(mkEntry(Nid{0x0000060Fu}, Nid{0u}, buildFolderFaiContentsTc().hnBytes));
    out.push_back(mkEntry(Nid{0x00000610u}, Nid{0u}, buildSearchContentsTemplateTc().hnBytes));
    out.push_back(mkEntry(Nid{0x00000671u}, Nid{0u}, buildAttachmentTemplateTc().hnBytes));
    out.push_back(mkEntry(Nid{0x00000692u}, Nid{0u}, buildRecipientTemplateTc().hnBytes));

    // 15. Spam Search Folder (0x2223; parent = Root)
    {
        FolderPcSchema ss{};
        ss.displayNameUtf16le = nameSpamSearch.data();
        ss.displayNameSize    = nameSpamSearch.size();
        auto pc = buildSearchFolderPc(ss, kDummySub);
        out.push_back(mkEntry(Nid{0x00002223u}, Nid{0x00000122u}, std::move(pc.hnBytes)));
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
        0x0000060Du, 0x0000060Eu, 0x0000060Fu, 0x00000610u,
        0x00000617u,                          // ReceiveFolderTable (Tier 2 G)
        0x00000671u, 0x00000692u,
        0x00002223u,
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
