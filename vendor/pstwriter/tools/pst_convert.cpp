// pstwriter/tools/pst_convert.cpp
//
// CLI: Graph JSON -> PST.
//
// Usage:
//   pst_convert <kind> <input.json> <output.pst>
//     kind ∈ {mail, contacts, calendar}
//
// Input JSON accepts:
//   * single object:   {"id": "...", "subject": "...", ...}
//   * bare array:      [ {...}, {...}, ... ]
//   * Graph envelope:  {"value":[ {...}, ... ], "@odata.context": "..."}
//
// All converters produce a single user folder under IPM Subtree:
//   * mail     -> "Inbox" (containerClass = "IPF.Note")
//   * contacts -> "Contacts" (containerClass = "IPF.Contact")
//   * calendar -> "Calendar" (containerClass = "IPF.Appointment")
//
// Provider UID is fixed (deterministic — same input -> same PST bytes).
// For multi-folder PSTs, drive the M7/M8/M9 writers programmatically
// from your own code (see README "API at a glance").

#include "contact.hpp"
#include "event.hpp"
#include "graph_contact.hpp"
#include "graph_event.hpp"
#include "graph_message.hpp"
#include "mail.hpp"
#include "types.hpp"
#include "writer.hpp"

#include <array>
#include <cstdio>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

using namespace pstwriter;
using std::string;
using std::vector;

namespace {

constexpr std::array<uint8_t, 16> kDefaultProviderUid = {{
    0x22, 0x9D, 0xB5, 0x0A, 0xDC, 0xD9, 0x94, 0x43,
    0x85, 0xDE, 0x90, 0xAE, 0xB0, 0x7D, 0x12, 0x70,
}};

string slurpFile(const string& path)
{
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        throw std::runtime_error("cannot open input file: " + path);
    }
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

void usage(const char* argv0)
{
    std::cerr <<
        "pst_convert — Graph JSON -> Outlook PST\n"
        "\n"
        "Usage:\n"
        "  " << argv0 << " <kind> <input.json> <output.pst>\n"
        "\n"
        "Kinds:\n"
        "  mail       Graph 'message' resource(s) -> IPM.Note PST\n"
        "  contacts   Graph 'contact' resource(s) -> IPM.Contact PST\n"
        "  calendar   Graph 'event'   resource(s) -> IPM.Appointment PST\n"
        "\n"
        "Input accepts a single Graph object, a bare JSON array, or a\n"
        "Graph list response of the form {\"value\":[...]}. \n"
        "\n"
        "After conversion, validate the output:\n"
        "  pst_info <output.pst>\n";
}

// Try to parse as list first; fall back to single-object mode.
template <class Single, class List>
vector<typename std::result_of<Single(const string&)>::type>
parseAny(const string& json, Single parseSingle, List parseList)
{
    using Item = typename std::result_of<Single(const string&)>::type;
    try {
        return parseList(json);
    } catch (const std::exception& e) {
        // Fall back: maybe it's a single object.
        try {
            return vector<Item>{ parseSingle(json) };
        } catch (const std::exception& e2) {
            throw std::runtime_error(
                string("JSON parse failed: list-form said \"") + e.what() +
                "\" and single-form said \"" + e2.what() + "\"");
        }
    }
}

// Normalise a folder path: trim leading/trailing slashes/backslashes,
// collapse repeats, and turn "\" into "/". Empty input → "Inbox" so
// messages with no recorded folder still land somewhere visible.
string normalizeFolderPath(const string& raw)
{
    string s = raw;
    for (auto& c : s) if (c == '\\') c = '/';
    // Trim leading and trailing slashes.
    size_t start = 0;
    while (start < s.size() && s[start] == '/') ++start;
    size_t end = s.size();
    while (end > start && s[end - 1] == '/') --end;
    s = s.substr(start, end - start);
    // Collapse repeated slashes.
    string out;
    out.reserve(s.size());
    bool prevSlash = false;
    for (char c : s) {
        if (c == '/') {
            if (prevSlash) continue;
            prevSlash = true;
        } else {
            prevSlash = false;
        }
        out.push_back(c);
    }
    if (out.empty()) out = "Inbox";
    return out;
}

// Split "A/B/C" → ["A", "B", "C"]. Caller has already normalised.
vector<string> splitFolderPath(const string& norm)
{
    vector<string> parts;
    string cur;
    for (char c : norm) {
        if (c == '/') {
            if (!cur.empty()) { parts.push_back(cur); cur.clear(); }
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) parts.push_back(cur);
    return parts;
}

// ----------------------------------------------------------------------------
// Per-folder Contents TC budget — hot-fix for the HN body cap.
//
// Each folder's Contents TC packs row data + per-row varlen payloads (subject,
// conversation topic, display-to, conv-index, change-key, search-key, …) into
// a single 8176-byte Heap-On-Node body. The pstwriter promotes the row matrix
// to a subnode but still inlines varlen payloads in the parent HN, so a folder
// of ~25–50 messages overflows once the cumulative varlen total exceeds the
// cap.  Until the proper varlen-subnode-promotion lands ([MS-PST] §2.3.4.3.1),
// we split overlarge buckets here so the user still gets a single PST with
// every message — just under "Original Folder/Part k of N" containers.
//
// estimateContentsRowVarlen over-estimates UTF-16 size as 2 × UTF-8 bytes plus
// a small null-terminator pad and accounts for DisplayTo/DisplayCc synthesised
// from recipient name lists.
// ----------------------------------------------------------------------------
constexpr size_t kHnBodyCap        = 8176;
constexpr size_t kHnSafetyMargin   = 256;     // leave room for estimator drift
constexpr size_t kHnHdrSize        = 12;
constexpr size_t kContentsTcFixed  = 8 + 254; // RowIdx BTHHEADER + TCINFO/TCOLDESC (29 cols)
constexpr size_t kContentsBaseAlloc = 3;       // RowIdx hdr + TCINFO + leaf
constexpr size_t kContentsVarlenColCount = 9;  // varlen columns per row
constexpr size_t kContentsBytesPerRowFixed = 8;       // BTH leaf record
constexpr size_t kContentsExtraBytesPerNamePart = 4;  // "; " separator in UTF-16

size_t estimateContentsRowVarlen(const graph::GraphMessage& m)
{
    auto utf16Bytes = [](const string& s) noexcept -> size_t {
        // 2 bytes per UTF-8 byte is a safe upper bound (UTF-16 BMP is 2B
        // per code point, UTF-8 ASCII is 1B per code point).  Add 2 for a
        // null terminator.
        return s.size() * 2u + 2u;
    };

    size_t v = 0;
    v += 18u;                              // MessageClass_W "IPM.Note" (16) + null
    v += utf16Bytes(m.subject);            // Subject_W
    v += utf16Bytes(m.subject);            // ConversationTopic_W (~ subject)
    const string& senderName = m.hasSender ? m.sender.name
                              : (m.hasFrom ? m.from.name : string());
    v += utf16Bytes(senderName);           // SentRepresentingName_W

    auto displaySize = [&](const std::vector<graph::Recipient>& rs) {
        size_t s = 0;
        for (const auto& r : rs) {
            s += utf16Bytes(r.emailAddress.name);
            s += kContentsExtraBytesPerNamePart;
        }
        return s == 0 ? 2u : s;
    };
    v += displaySize(m.toRecipients);      // DisplayTo_W
    v += displaySize(m.ccRecipients);      // DisplayCc_W

    v += m.conversationIndex.empty() ? 22u : m.conversationIndex.size();
    v += 22u;                              // ChangeKey
    v += 16u;                              // PR_SEARCH_KEY
    return v;
}

size_t estimateContentsHnBody(size_t rowCount, size_t totalVarlenBytes)
{
    const size_t total = kContentsTcFixed
                       + kContentsBytesPerRowFixed * rowCount
                       + totalVarlenBytes;
    size_t allocCount = kContentsBaseAlloc
                      + kContentsVarlenColCount * rowCount;
    const size_t cursorRaw = kHnHdrSize + total;
    const size_t cursorAligned = (cursorRaw + 3u) & ~size_t{3u};
    if (cursorRaw != cursorAligned) allocCount += 1u;
    const size_t hnpm = 4u + 2u * (allocCount + 1u);
    return cursorAligned + hnpm;
}

// Greedy split: returns one bucket per Part. Each part's HN-body estimate
// stays below kHnBodyCap - kHnSafetyMargin. Single oversize messages still
// land in their own bucket (the writer will throw at build time but at least
// the smaller buckets won't be lost).
vector<vector<const graph::GraphMessage*>>
splitBucketByHnBudget(const vector<const graph::GraphMessage*>& msgs)
{
    vector<vector<const graph::GraphMessage*>> parts;
    vector<const graph::GraphMessage*> cur;
    size_t curV = 0;
    const size_t cap = kHnBodyCap - kHnSafetyMargin;
    for (auto* m : msgs) {
        const size_t addV = estimateContentsRowVarlen(*m);
        if (!cur.empty() &&
            estimateContentsHnBody(cur.size() + 1u, curV + addV) > cap) {
            parts.push_back(std::move(cur));
            cur.clear();
            curV = 0;
        }
        cur.push_back(m);
        curV += addV;
    }
    if (!cur.empty()) parts.push_back(std::move(cur));
    return parts;
}

int runMail(const string& jsonPath, const string& pstPath)
{
    const auto jsonText = slurpFile(jsonPath);
    auto messages = parseAny(jsonText,
        graph::parseGraphMessage, graph::parseGraphMessageList);

    std::cout << "  parsed " << messages.size() << " mail message(s)\n";

    // Folder synthesis. The exporter stamps each message's source
    // folder onto m.folderPath ("/Inbox", "/Sent Items/Project X").
    // Group by full path and synthesize one M7Folder per unique node
    // so Outlook renders the source mailbox's tree instead of a flat
    // dump under "Inbox". Path components map 1:1 to M7Folders;
    // intermediate path levels (e.g. "Sent Items" when only "Sent
    // Items/Project X" appears) are inserted as empty container
    // folders so Outlook's tree control draws the right ancestry.
    //
    // Single-folder corpora (no folderPath set on any message, or
    // every message landing in "/Inbox") are equivalent to the old
    // "everything under Inbox" behaviour.
    std::vector<string> folderOrder;            // unique folder paths in insertion order
    std::unordered_map<string, size_t> pathToIdx;
    std::vector<vector<const graph::GraphMessage*>> bucketByIdx;

    auto ensureFolder = [&](const string& fullPath) -> size_t {
        auto it = pathToIdx.find(fullPath);
        if (it != pathToIdx.end()) return it->second;
        const size_t idx = folderOrder.size();
        folderOrder.push_back(fullPath);
        pathToIdx.emplace(fullPath, idx);
        bucketByIdx.emplace_back();
        return idx;
    };

    for (const auto& m : messages) {
        const string norm = normalizeFolderPath(m.folderPath);
        // Pre-create every ancestor so the path "/A/B" produces both
        // "A" and "A/B" folders. Without this, the wizard would see
        // a child whose parentPath references a folder that doesn't
        // exist in the M7PstConfig and the mail.cpp parent-resolver
        // would fall back to IPM Subtree, flattening the hierarchy.
        const auto parts = splitFolderPath(norm);
        string accumulated;
        for (size_t i = 0; i < parts.size(); ++i) {
            if (!accumulated.empty()) accumulated.push_back('/');
            accumulated += parts[i];
            ensureFolder(accumulated);
        }
        const size_t leafIdx = ensureFolder(norm);
        bucketByIdx[leafIdx].push_back(&m);
    }

    // Hot-fix: split any leaf bucket whose Contents TC would overflow
    // the 8176-byte HN body cap. Replace the original folder's
    // messages with an empty container + child "Part k of N"
    // sub-folders so the user still gets a single PST. The proper fix
    // (varlen subnode promotion per [MS-PST] §2.3.4.3.1) tracked
    // separately. Two-phase to keep references stable while we mutate
    // the bucket vector.
    struct SplitDecision {
        size_t origIdx;
        size_t origVarlenBytes;
        std::vector<std::vector<const graph::GraphMessage*>> parts;
    };
    std::vector<SplitDecision> splits;
    for (size_t i = 0; i < folderOrder.size(); ++i) {
        const auto& msgs = bucketByIdx[i];
        if (msgs.size() <= 1) continue;
        size_t totalV = 0;
        for (auto* m : msgs) totalV += estimateContentsRowVarlen(*m);
        if (estimateContentsHnBody(msgs.size(), totalV)
            <= kHnBodyCap - kHnSafetyMargin) {
            continue;
        }
        auto parts = splitBucketByHnBudget(msgs);
        if (parts.size() <= 1) continue;  // single oversized message
        splits.push_back({i, totalV, std::move(parts)});
    }
    for (auto& d : splits) {
        bucketByIdx[d.origIdx].clear();   // original becomes empty container
        const string parentPath = folderOrder[d.origIdx];   // copy: ensureFolder may invalidate
        std::cout << "  split folder \"" << parentPath << "\" ("
                  << d.parts.size() << " parts, ~" << d.origVarlenBytes
                  << " B varlen)\n";
        for (size_t pi = 0; pi < d.parts.size(); ++pi) {
            std::ostringstream child;
            child << parentPath << "/Part " << (pi + 1) << " of " << d.parts.size();
            const size_t cidx = ensureFolder(child.str());
            bucketByIdx[cidx] = std::move(d.parts[pi]);
        }
    }

    // Build M7Folder records in the same order. Leading folders are
    // ancestors so the writer's pathToFolderNid map is populated
    // before children reference it via parentPath. We're already
    // depth-first by virtue of the ancestor pre-creation loop above
    // (each ensureFolder call adds entries in parent-then-leaf order).
    std::vector<M7Folder> folders;
    folders.reserve(folderOrder.size());
    for (size_t i = 0; i < folderOrder.size(); ++i) {
        const string& full = folderOrder[i];
        const auto parts = splitFolderPath(full);
        M7Folder f;
        f.displayName = parts.empty() ? string("Inbox") : parts.back();
        f.path        = full;
        // parentPath = the path with the last component stripped.
        // Empty for top-level folders → falls back to parentNid (IPM
        // Subtree) in the writer.
        if (parts.size() > 1) {
            f.parentPath.clear();
            for (size_t j = 0; j + 1 < parts.size(); ++j) {
                if (!f.parentPath.empty()) f.parentPath.push_back('/');
                f.parentPath += parts[j];
            }
        }
        f.parentNid = Nid{0x00008022u};   // IPM Subtree, used when parentPath is empty
        f.containerClass = "IPF.Note";
        f.messages.reserve(bucketByIdx[i].size());
        for (auto* mp : bucketByIdx[i]) f.messages.push_back(mp);
        folders.push_back(std::move(f));
    }

    if (folders.empty()) {
        // Defensive: parseGraphMessageList returned zero entries.
        // Emit a placeholder Inbox so writeM7Pst still produces a
        // valid PST rather than aborting on the empty-folders branch.
        M7Folder placeholder;
        placeholder.displayName = "Inbox";
        placeholder.path        = "Inbox";
        placeholder.parentNid   = Nid{0x00008022u};
        folders.push_back(std::move(placeholder));
    }

    std::cout << "  synthesised " << folders.size() << " folder(s) from "
              << messages.size() << " message(s)\n";

    M7PstConfig cfg;
    cfg.path           = pstPath;
    cfg.providerUid    = kDefaultProviderUid;
    cfg.pstDisplayName = "PST Conversion (mail)";
    cfg.folders        = std::move(folders);

    const auto r = writeM7Pst(cfg);
    if (!r.ok) {
        std::cerr << "  writeM7Pst failed: " << r.message << "\n";
        return 1;
    }
    std::cout << "  wrote " << pstPath << "\n";
    return 0;
}

int runContacts(const string& jsonPath, const string& pstPath)
{
    const auto jsonText = slurpFile(jsonPath);
    auto contacts = parseAny(jsonText,
        graph::parseGraphContact, graph::parseGraphContactList);

    std::cout << "  parsed " << contacts.size() << " contact(s)\n";

    M8ContactFolder folder;
    folder.displayName = "Contacts";
    folder.parentNid   = Nid{0x00008022u};
    folder.contacts.reserve(contacts.size());
    for (const auto& c : contacts) folder.contacts.push_back(&c);

    M8PstConfig cfg;
    cfg.path           = pstPath;
    cfg.providerUid    = kDefaultProviderUid;
    cfg.pstDisplayName = "PST Conversion (contacts)";
    cfg.folders        = { folder };

    const auto r = writeM8Pst(cfg);
    if (!r.ok) {
        std::cerr << "  writeM8Pst failed: " << r.message << "\n";
        return 1;
    }
    std::cout << "  wrote " << pstPath << "\n";
    return 0;
}

int runCalendar(const string& jsonPath, const string& pstPath)
{
    const auto jsonText = slurpFile(jsonPath);
    auto events = parseAny(jsonText,
        graph::parseGraphEvent, graph::parseGraphEventList);

    std::cout << "  parsed " << events.size() << " event(s)\n";

    M9CalendarFolder folder;
    folder.displayName = "Calendar";
    folder.parentNid   = Nid{0x00008022u};
    folder.events.reserve(events.size());
    for (const auto& e : events) folder.events.push_back(&e);

    M9PstConfig cfg;
    cfg.path           = pstPath;
    cfg.providerUid    = kDefaultProviderUid;
    cfg.pstDisplayName = "PST Conversion (calendar)";
    cfg.folders        = { folder };

    const auto r = writeM9Pst(cfg);
    if (!r.ok) {
        std::cerr << "  writeM9Pst failed: " << r.message << "\n";
        return 1;
    }
    std::cout << "  wrote " << pstPath << "\n";
    return 0;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 4) {
        usage(argv[0]);
        return 2;
    }
    const string kind     = argv[1];
    const string jsonPath = argv[2];
    const string pstPath  = argv[3];

    std::cout << "pst_convert: kind=" << kind
              << " input=" << jsonPath
              << " output=" << pstPath << "\n";

    try {
        if (kind == "mail")     return runMail(jsonPath, pstPath);
        if (kind == "contacts") return runContacts(jsonPath, pstPath);
        if (kind == "calendar") return runCalendar(jsonPath, pstPath);

        std::cerr << "unknown kind: " << kind
                  << " (expected mail|contacts|calendar)\n";
        usage(argv[0]);
        return 2;
    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }
}
