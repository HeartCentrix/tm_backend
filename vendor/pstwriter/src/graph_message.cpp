// pstwriter/src/graph_message.cpp
//
// M7 Phase A — Graph message JSON parser.
//
// Hand-rolled recursive-descent JSON parser. Two stages:
//   1. parseValue(text) -> JsonValue tree
//   2. Walk the tree, picking out the fields we care about into
//      GraphMessage. Unknown keys are ignored.
//
// The parser is conformant to RFC 8259 in scope, with these constraints:
//   * Numbers are stored as double + raw text; we read the int64 form
//     from the text where required.
//   * Strings handle \", \\, \/, \b, \f, \n, \r, \t, and \uXXXX (with
//     surrogate pair handling).
//   * Surrogate pairs decode to UTF-8 in the resulting std::string.

#include "graph_message.hpp"

#include "graph_convert.hpp"

#include <cctype>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using std::map;
using std::shared_ptr;
using std::string;
using std::vector;

namespace pstwriter {
namespace graph {

// ============================================================================
// JsonValue — minimal generic tree. Used internally only.
// ============================================================================
namespace {

struct JsonValue;
using JsonObject = map<string, JsonValue>;
using JsonArray  = vector<JsonValue>;

enum class JsonType : uint8_t {
    Null    = 0,
    Bool    = 1,
    Number  = 2,
    String  = 3,
    Object  = 4,
    Array   = 5,
};

struct JsonValue {
    JsonType  type {JsonType::Null};
    bool      b    {false};
    double    n    {0.0};
    string    s;       // for strings AND raw number text
    JsonObject obj;
    JsonArray  arr;
};

// ----------------------------------------------------------------------------
// Parser
// ----------------------------------------------------------------------------
class Parser {
public:
    explicit Parser(const string& src) : src_(src), pos_(0) {}

    JsonValue parseTopLevel()
    {
        skipWs();
        JsonValue v = parseValue();
        skipWs();
        if (pos_ != src_.size())
            fail("trailing data after JSON value");
        return v;
    }

private:
    const string& src_;
    size_t        pos_;

    [[noreturn]] void fail(const char* msg)
    {
        throw std::runtime_error(string("graph_message JSON: ") + msg
                                 + " at offset " + std::to_string(pos_));
    }

    void skipWs() noexcept
    {
        while (pos_ < src_.size()) {
            const char c = src_[pos_];
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') ++pos_;
            else break;
        }
    }

    char peek()
    {
        if (pos_ >= src_.size()) fail("unexpected end of input");
        return src_[pos_];
    }

    bool consume(char c)
    {
        if (pos_ < src_.size() && src_[pos_] == c) { ++pos_; return true; }
        return false;
    }

    void expect(char c)
    {
        if (!consume(c)) fail("expected character not found");
    }

    JsonValue parseValue()
    {
        skipWs();
        if (pos_ >= src_.size()) fail("expected value");
        const char c = src_[pos_];
        if (c == '{') return parseObject();
        if (c == '[') return parseArray();
        if (c == '"') return parseString();
        if (c == 't' || c == 'f') return parseBool();
        if (c == 'n') return parseNull();
        if (c == '-' || (c >= '0' && c <= '9')) return parseNumber();
        fail("unexpected character at start of value");
    }

    JsonValue parseObject()
    {
        JsonValue v;
        v.type = JsonType::Object;
        expect('{');
        skipWs();
        if (consume('}')) return v;

        while (true) {
            skipWs();
            if (peek() != '"') fail("expected string key in object");
            JsonValue key = parseString();
            skipWs();
            expect(':');
            skipWs();
            JsonValue val = parseValue();
            v.obj.emplace(std::move(key.s), std::move(val));
            skipWs();
            if (consume(',')) continue;
            if (consume('}')) break;
            fail("expected ',' or '}' in object");
        }
        return v;
    }

    JsonValue parseArray()
    {
        JsonValue v;
        v.type = JsonType::Array;
        expect('[');
        skipWs();
        if (consume(']')) return v;

        while (true) {
            skipWs();
            v.arr.push_back(parseValue());
            skipWs();
            if (consume(',')) continue;
            if (consume(']')) break;
            fail("expected ',' or ']' in array");
        }
        return v;
    }

    JsonValue parseBool()
    {
        JsonValue v;
        v.type = JsonType::Bool;
        if (pos_ + 4 <= src_.size() && src_.compare(pos_, 4, "true") == 0) {
            pos_ += 4; v.b = true;
        } else if (pos_ + 5 <= src_.size() && src_.compare(pos_, 5, "false") == 0) {
            pos_ += 5; v.b = false;
        } else {
            fail("expected true/false");
        }
        return v;
    }

    JsonValue parseNull()
    {
        if (pos_ + 4 <= src_.size() && src_.compare(pos_, 4, "null") == 0) {
            pos_ += 4;
            return JsonValue{};
        }
        fail("expected null");
    }

    JsonValue parseNumber()
    {
        const size_t start = pos_;
        if (consume('-')) {}
        if (pos_ < src_.size() && src_[pos_] == '0') ++pos_;
        else {
            while (pos_ < src_.size() && std::isdigit(static_cast<unsigned char>(src_[pos_]))) ++pos_;
        }
        if (pos_ < src_.size() && src_[pos_] == '.') {
            ++pos_;
            while (pos_ < src_.size() && std::isdigit(static_cast<unsigned char>(src_[pos_]))) ++pos_;
        }
        if (pos_ < src_.size() && (src_[pos_] == 'e' || src_[pos_] == 'E')) {
            ++pos_;
            if (pos_ < src_.size() && (src_[pos_] == '+' || src_[pos_] == '-')) ++pos_;
            while (pos_ < src_.size() && std::isdigit(static_cast<unsigned char>(src_[pos_]))) ++pos_;
        }
        JsonValue v;
        v.type = JsonType::Number;
        v.s    = src_.substr(start, pos_ - start);
        try {
            v.n = std::stod(v.s);
        } catch (...) {
            fail("malformed number");
        }
        return v;
    }

    // ------------------------------------------------------------
    // String parsing with full escape handling.
    // ------------------------------------------------------------
    static void appendUtf8Codepoint(string& dst, uint32_t cp)
    {
        if (cp <= 0x7Fu) {
            dst.push_back(static_cast<char>(cp));
        } else if (cp <= 0x7FFu) {
            dst.push_back(static_cast<char>(0xC0u | (cp >> 6)));
            dst.push_back(static_cast<char>(0x80u | (cp & 0x3Fu)));
        } else if (cp <= 0xFFFFu) {
            dst.push_back(static_cast<char>(0xE0u | (cp >> 12)));
            dst.push_back(static_cast<char>(0x80u | ((cp >> 6) & 0x3Fu)));
            dst.push_back(static_cast<char>(0x80u | (cp & 0x3Fu)));
        } else {
            dst.push_back(static_cast<char>(0xF0u | (cp >> 18)));
            dst.push_back(static_cast<char>(0x80u | ((cp >> 12) & 0x3Fu)));
            dst.push_back(static_cast<char>(0x80u | ((cp >> 6) & 0x3Fu)));
            dst.push_back(static_cast<char>(0x80u | (cp & 0x3Fu)));
        }
    }

    uint32_t parseHex4()
    {
        if (pos_ + 4 > src_.size()) fail("truncated \\u escape");
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            const char c = src_[pos_++];
            v <<= 4;
            if (c >= '0' && c <= '9') v |= static_cast<uint32_t>(c - '0');
            else if (c >= 'a' && c <= 'f') v |= static_cast<uint32_t>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') v |= static_cast<uint32_t>(c - 'A' + 10);
            else fail("bad hex digit in \\u escape");
        }
        return v;
    }

    JsonValue parseString()
    {
        JsonValue v;
        v.type = JsonType::String;
        expect('"');
        while (true) {
            if (pos_ >= src_.size()) fail("unterminated string");
            const char c = src_[pos_++];
            if (c == '"') break;
            if (c == '\\') {
                if (pos_ >= src_.size()) fail("truncated escape");
                const char e = src_[pos_++];
                switch (e) {
                case '"':  v.s.push_back('"'); break;
                case '\\': v.s.push_back('\\'); break;
                case '/':  v.s.push_back('/'); break;
                case 'b':  v.s.push_back('\b'); break;
                case 'f':  v.s.push_back('\f'); break;
                case 'n':  v.s.push_back('\n'); break;
                case 'r':  v.s.push_back('\r'); break;
                case 't':  v.s.push_back('\t'); break;
                case 'u': {
                    uint32_t cp = parseHex4();
                    if (cp >= 0xD800u && cp <= 0xDBFFu) {
                        // High surrogate; expect low surrogate next.
                        if (pos_ + 2 > src_.size() || src_[pos_] != '\\' || src_[pos_ + 1] != 'u')
                            fail("expected low surrogate after high surrogate");
                        pos_ += 2;
                        const uint32_t lo = parseHex4();
                        if (lo < 0xDC00u || lo > 0xDFFFu)
                            fail("invalid low surrogate value");
                        cp = 0x10000u + ((cp - 0xD800u) << 10) + (lo - 0xDC00u);
                    } else if (cp >= 0xDC00u && cp <= 0xDFFFu) {
                        fail("unexpected low surrogate");
                    }
                    appendUtf8Codepoint(v.s, cp);
                    break;
                }
                default: fail("bad escape character");
                }
            } else {
                v.s.push_back(c);
            }
        }
        return v;
    }
};

// ----------------------------------------------------------------------------
// Lookup helpers
// ----------------------------------------------------------------------------
const JsonValue* find(const JsonValue& obj, const string& key) noexcept
{
    if (obj.type != JsonType::Object) return nullptr;
    auto it = obj.obj.find(key);
    return it == obj.obj.end() ? nullptr : &it->second;
}

string getStr(const JsonValue& obj, const string& key)
{
    const JsonValue* v = find(obj, key);
    if (v == nullptr) return "";
    if (v->type != JsonType::String) return "";
    return v->s;
}

bool getBool(const JsonValue& obj, const string& key, bool deflt = false)
{
    const JsonValue* v = find(obj, key);
    if (v == nullptr || v->type != JsonType::Bool) return deflt;
    return v->b;
}

int32_t getInt(const JsonValue& obj, const string& key, int32_t deflt = 0)
{
    const JsonValue* v = find(obj, key);
    if (v == nullptr || v->type != JsonType::Number) return deflt;
    return static_cast<int32_t>(v->n);
}

// ----------------------------------------------------------------------------
// Field extraction
// ----------------------------------------------------------------------------
EmailAddress extractEmailAddress(const JsonValue& obj)
{
    EmailAddress a;
    a.name    = getStr(obj, "name");
    a.address = getStr(obj, "address");
    return a;
}

// Graph wraps email addresses one level deeper:
//   {"emailAddress": {"name": "...", "address": "..."}}
EmailAddress extractRecipientEmail(const JsonValue& wrapper)
{
    const JsonValue* ea = find(wrapper, "emailAddress");
    if (ea == nullptr) return {};
    return extractEmailAddress(*ea);
}

void extractRecipients(const JsonValue& parent, const string& key,
                       RecipientKind kind,
                       vector<Recipient>& out)
{
    const JsonValue* arr = find(parent, key);
    if (arr == nullptr || arr->type != JsonType::Array) return;
    for (const auto& el : arr->arr) {
        if (el.type != JsonType::Object) continue;
        Recipient r;
        r.kind         = kind;
        r.emailAddress = extractRecipientEmail(el);
        out.push_back(std::move(r));
    }
}

void extractBody(const JsonValue& parent, Body& out)
{
    const JsonValue* b = find(parent, "body");
    if (b == nullptr) return;
    out.content = getStr(*b, "content");
    const string ct = getStr(*b, "contentType");
    if (ct == "html" || ct == "Html" || ct == "HTML") out.contentType = BodyType::Html;
    else                                              out.contentType = BodyType::Text;
}

Importance importanceFrom(const string& s) noexcept
{
    if (s == "low")  return Importance::Low;
    if (s == "high") return Importance::High;
    return Importance::Normal;
}

FlagStatus flagStatusFrom(const string& s) noexcept
{
    if (s == "complete") return FlagStatus::Complete;
    if (s == "flagged")  return FlagStatus::Flagged;
    return FlagStatus::NotFlagged;
}

GraphMessage extractMessage(const JsonValue& obj);   // fwd

void extractAttachments(const JsonValue& parent, vector<Attachment>& out)
{
    const JsonValue* arr = find(parent, "attachments");
    if (arr == nullptr || arr->type != JsonType::Array) return;
    for (const auto& el : arr->arr) {
        if (el.type != JsonType::Object) continue;
        Attachment a;
        a.name        = getStr(el, "name");
        a.contentType = getStr(el, "contentType");
        a.contentId   = getStr(el, "contentId");
        a.isInline    = getBool(el, "isInline");
        a.size        = getInt (el, "size");

        // Discriminate via @odata.type when present, otherwise via fields.
        const string odata = getStr(el, "@odata.type");
        const bool   hasItem  = (find(el, "item") != nullptr);
        const bool   hasBytes = (find(el, "contentBytes") != nullptr);

        if (odata == "#microsoft.graph.itemAttachment" || hasItem) {
            a.kind = AttachmentKind::Item;
            const JsonValue* it = find(el, "item");
            if (it != nullptr && it->type == JsonType::Object) {
                a.item = std::make_shared<GraphMessage>(extractMessage(*it));
            }
        } else if (odata == "#microsoft.graph.fileAttachment" || hasBytes) {
            a.kind = AttachmentKind::File;
            const string b64 = getStr(el, "contentBytes");
            if (!b64.empty()) {
                try { a.contentBytes = base64DecodeBinary(b64); }
                catch (...) { a.contentBytes.clear(); }
            }
        } else {
            a.kind = AttachmentKind::File;  // default
        }

        out.push_back(std::move(a));
    }
}

void extractInternetHeaders(const JsonValue& parent, vector<InternetMessageHeader>& out)
{
    const JsonValue* arr = find(parent, "internetMessageHeaders");
    if (arr == nullptr || arr->type != JsonType::Array) return;
    for (const auto& el : arr->arr) {
        if (el.type != JsonType::Object) continue;
        InternetMessageHeader h;
        h.name  = getStr(el, "name");
        h.value = getStr(el, "value");
        out.push_back(std::move(h));
    }
}

void extractCategories(const JsonValue& parent, vector<string>& out)
{
    const JsonValue* arr = find(parent, "categories");
    if (arr == nullptr || arr->type != JsonType::Array) return;
    for (const auto& el : arr->arr) {
        if (el.type == JsonType::String) out.push_back(el.s);
    }
}

GraphMessage extractMessage(const JsonValue& obj)
{
    GraphMessage m;
    m.id                   = getStr(obj, "id");
    m.internetMessageId    = getStr(obj, "internetMessageId");
    m.conversationId       = getStr(obj, "conversationId");
    {
        const string b64 = getStr(obj, "conversationIndex");
        if (!b64.empty()) {
            try { m.conversationIndex = base64DecodeBinary(b64); }
            catch (...) { m.conversationIndex.clear(); }
        }
    }
    m.subject              = getStr(obj, "subject");
    m.bodyPreview          = getStr(obj, "bodyPreview");
    extractBody(obj, m.body);

    m.createdDateTime      = getStr(obj, "createdDateTime");
    m.lastModifiedDateTime = getStr(obj, "lastModifiedDateTime");
    m.sentDateTime         = getStr(obj, "sentDateTime");
    m.receivedDateTime     = getStr(obj, "receivedDateTime");

    if (const JsonValue* sender = find(obj, "sender")) {
        m.sender    = extractRecipientEmail(*sender);
        m.hasSender = !m.sender.address.empty() || !m.sender.name.empty();
    }
    if (const JsonValue* fr = find(obj, "from")) {
        m.from    = extractRecipientEmail(*fr);
        m.hasFrom = !m.from.address.empty() || !m.from.name.empty();
    }

    extractRecipients(obj, "toRecipients",  RecipientKind::To,  m.toRecipients);
    extractRecipients(obj, "ccRecipients",  RecipientKind::Cc,  m.ccRecipients);
    extractRecipients(obj, "bccRecipients", RecipientKind::Bcc, m.bccRecipients);

    if (const JsonValue* rt = find(obj, "replyTo")) {
        if (rt->type == JsonType::Array && !rt->arr.empty()
            && rt->arr.front().type == JsonType::Object)
        {
            m.replyTo    = extractRecipientEmail(rt->arr.front());
            m.hasReplyTo = !m.replyTo.address.empty();
        }
    }

    m.isRead         = getBool(obj, "isRead");
    m.isDraft        = getBool(obj, "isDraft");
    m.hasAttachments = getBool(obj, "hasAttachments");
    m.importance     = importanceFrom(getStr(obj, "importance"));

    if (const JsonValue* fl = find(obj, "flag")) {
        m.flagStatus = flagStatusFrom(getStr(*fl, "flagStatus"));
    }

    extractCategories(obj, m.categories);
    extractInternetHeaders(obj, m.internetMessageHeaders);
    extractAttachments(obj, m.attachments);

    m.parentFolderId = getStr(obj, "parentFolderId");

    return m;
}

} // namespace

// ============================================================================
// Public API
// ============================================================================
GraphMessage parseGraphMessage(const string& json)
{
    Parser p(json);
    JsonValue v = p.parseTopLevel();
    if (v.type != JsonType::Object)
        throw std::runtime_error("graph_message JSON: expected top-level object");
    return extractMessage(v);
}

vector<GraphMessage> parseGraphMessageList(const string& json)
{
    Parser p(json);
    JsonValue v = p.parseTopLevel();

    const JsonArray* arr = nullptr;
    if (v.type == JsonType::Array) {
        arr = &v.arr;
    } else if (v.type == JsonType::Object) {
        const JsonValue* val = find(v, "value");
        if (val != nullptr && val->type == JsonType::Array) arr = &val->arr;
    }
    if (arr == nullptr)
        throw std::runtime_error("graph_message JSON: expected array or {value:[...]}");

    vector<GraphMessage> out;
    out.reserve(arr->size());
    for (const auto& el : *arr) {
        if (el.type != JsonType::Object) continue;
        out.push_back(extractMessage(el));
    }
    return out;
}

} // namespace graph
} // namespace pstwriter
