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

int runMail(const string& jsonPath, const string& pstPath)
{
    const auto jsonText = slurpFile(jsonPath);
    auto messages = parseAny(jsonText,
        graph::parseGraphMessage, graph::parseGraphMessageList);

    std::cout << "  parsed " << messages.size() << " mail message(s)\n";

    M7Folder inbox;
    inbox.displayName = "Inbox";
    inbox.parentNid   = Nid{0x00008022u};
    inbox.messages.reserve(messages.size());
    for (const auto& m : messages) inbox.messages.push_back(&m);

    M7PstConfig cfg;
    cfg.path           = pstPath;
    cfg.providerUid    = kDefaultProviderUid;
    cfg.pstDisplayName = "PST Conversion (mail)";
    cfg.folders        = { inbox };

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
