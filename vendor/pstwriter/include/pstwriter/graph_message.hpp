// pstwriter/graph_message.hpp
//
// M7 Phase A — Microsoft Graph "message" resource model + minimal JSON
// parser tuned to it.
//
// Reference: learn.microsoft.com/en-us/graph/api/resources/message
//
// Design constraints:
//   * No external JSON library (project rule: no deps beyond Catch2 in
//     tests). Hand-rolled parser is sufficient — Graph JSON is well-formed
//     and we only need the subset of fields M7/M8/M9 consume.
//   * Parser preserves UTF-8 bytes verbatim for string values; conversion
//     to UTF-16-LE happens at PC-build time via graph_convert::utf8ToUtf16le.
//   * Unknown fields are ignored (Graph adds new fields over time; M7
//     should not break when one appears).

#pragma once

#include "graph_convert.hpp"
#include "types.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace pstwriter {
namespace graph {

// ============================================================================
// EmailAddress / Recipient
// ============================================================================
struct EmailAddress {
    std::string name;     // UTF-8 display name
    std::string address;  // UTF-8 SMTP address
};

enum class RecipientKind : uint8_t {
    To  = 1,
    Cc  = 2,
    Bcc = 3,
};

struct Recipient {
    RecipientKind kind {RecipientKind::To};
    EmailAddress  emailAddress;
};

// ============================================================================
// InternetMessageHeader — RFC 2822 name/value pair.
// ============================================================================
struct InternetMessageHeader {
    std::string name;
    std::string value;
};

// ============================================================================
// Body content type.
// ============================================================================
enum class BodyType : uint8_t {
    Text = 0,
    Html = 1,
};

struct Body {
    BodyType    contentType {BodyType::Text};
    std::string content;     // UTF-8 (HTML or plain text)
};

// ============================================================================
// Attachment
//
// Two main flavors:
//   * fileAttachment   — has contentBytes (base64) + contentType + name
//   * itemAttachment   — has item (an embedded GraphMessage)
//
// Discriminator is the Graph @odata.type field. We store both flavors
// in one struct: `kind` selects which sub-fields are valid.
// ============================================================================
enum class AttachmentKind : uint8_t {
    File = 0,
    Item = 1,
};

struct GraphMessage;  // forward decl for itemAttachment

struct Attachment {
    AttachmentKind kind {AttachmentKind::File};

    std::string name;            // attachment.name
    std::string contentType;     // MIME type (e.g. "image/png")
    std::string contentId;       // optional inline-image content-id
    bool        isInline {false};
    int32_t     size     {0};    // attachment.size (Graph reports bytes)

    // File-attachment: raw bytes (already base64-decoded).
    std::vector<uint8_t> contentBytes;

    // Item-attachment: nested message. Only valid when kind == Item.
    std::shared_ptr<GraphMessage> item;
};

// ============================================================================
// GraphMessage — the root object. Mirrors the Graph "message" resource;
// fields we care about for M7. Anything not consumed by the writer is
// dropped at parse time.
// ============================================================================
enum class FlagStatus : uint8_t {
    NotFlagged = 0,
    Complete   = 1,
    Flagged    = 2,
};

enum class Importance : uint8_t {
    Low    = 0,
    Normal = 1,
    High   = 2,
};

struct GraphMessage {
    // Identity / threading
    std::string id;                  // Graph internal id
    std::string internetMessageId;   // RFC 2822 Message-ID
    std::string conversationId;
    std::vector<uint8_t> conversationIndex;  // already base64-decoded

    // Subject / preview / body
    std::string subject;
    std::string bodyPreview;
    Body        body;

    // Times (UTC ISO 8601)
    std::string createdDateTime;
    std::string lastModifiedDateTime;
    std::string sentDateTime;
    std::string receivedDateTime;

    // Sender + From (Graph distinguishes; equal in non-delegation cases)
    EmailAddress sender;
    bool         hasSender {false};
    EmailAddress from;
    bool         hasFrom   {false};

    // Recipients
    std::vector<Recipient> toRecipients;
    std::vector<Recipient> ccRecipients;
    std::vector<Recipient> bccRecipients;
    EmailAddress           replyTo;       // Graph's replyTo[0] if present
    bool                   hasReplyTo {false};

    // Flags
    bool        isRead       {false};
    bool        isDraft      {false};
    bool        hasAttachments {false};
    Importance  importance   {Importance::Normal};
    FlagStatus  flagStatus   {FlagStatus::NotFlagged};

    // Categories (string array)
    std::vector<std::string> categories;

    // Internet headers (when Graph $select includes them)
    std::vector<InternetMessageHeader> internetMessageHeaders;

    // Attachments
    std::vector<Attachment> attachments;

    // Folder containment
    std::string parentFolderId;  // Graph folder id (mailFolders/<id>)
};

// ============================================================================
// parseGraphMessage(json)
//
// Parses a single Graph message JSON object. Throws std::runtime_error
// on syntax errors with a 1-line description. Tolerates unknown fields.
//
// Top-level array form (e.g. `{"value": [ ... ]}`) is NOT accepted here —
// callers should iterate the array themselves and pass each element.
// ============================================================================
GraphMessage parseGraphMessage(const std::string& json);

// Parse a top-level array of messages: accepts either a bare array
// `[ {...}, {...} ]` or a `{"value":[...]}` envelope. Returns the list.
std::vector<GraphMessage> parseGraphMessageList(const std::string& json);

} // namespace graph
} // namespace pstwriter
