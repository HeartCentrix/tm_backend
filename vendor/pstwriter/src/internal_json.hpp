// pstwriter/src/internal_json.hpp
//
// Minimal JSON parser shared by graph_message.cpp + graph_contact.cpp +
// (future) graph_event.cpp. Header-only so each translation unit gets
// its own copy of the parser body — compile cost is ~200 lines per TU,
// acceptable for the M7-M9 scope.
//
// NOT a public API. Lives under src/ rather than include/pstwriter/ on
// purpose.

#pragma once

#include <cctype>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace pstwriter {
namespace graph {
namespace json_detail {

struct JsonValue;
using JsonObject = std::map<std::string, JsonValue>;
using JsonArray  = std::vector<JsonValue>;

enum class JsonType : uint8_t {
    Null    = 0,
    Bool    = 1,
    Number  = 2,
    String  = 3,
    Object  = 4,
    Array   = 5,
};

struct JsonValue {
    JsonType    type {JsonType::Null};
    bool        b    {false};
    double      n    {0.0};
    std::string s;
    JsonObject  obj;
    JsonArray   arr;
};

class Parser {
public:
    explicit Parser(const std::string& src) : src_(src), pos_(0) {}

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
    const std::string& src_;
    size_t             pos_;

    [[noreturn]] void fail(const char* msg)
    {
        throw std::runtime_error(std::string("graph JSON: ") + msg
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

    static void appendUtf8Codepoint(std::string& dst, uint32_t cp)
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
inline const JsonValue* find(const JsonValue& obj, const std::string& key) noexcept
{
    if (obj.type != JsonType::Object) return nullptr;
    auto it = obj.obj.find(key);
    return it == obj.obj.end() ? nullptr : &it->second;
}

inline std::string getStr(const JsonValue& obj, const std::string& key)
{
    const JsonValue* v = find(obj, key);
    if (v == nullptr) return "";
    if (v->type != JsonType::String) return "";
    return v->s;
}

inline bool getBool(const JsonValue& obj, const std::string& key, bool deflt = false)
{
    const JsonValue* v = find(obj, key);
    if (v == nullptr || v->type != JsonType::Bool) return deflt;
    return v->b;
}

inline int32_t getInt(const JsonValue& obj, const std::string& key, int32_t deflt = 0)
{
    const JsonValue* v = find(obj, key);
    if (v == nullptr || v->type != JsonType::Number) return deflt;
    return static_cast<int32_t>(v->n);
}

inline std::vector<std::string> getStringArray(const JsonValue& obj, const std::string& key)
{
    std::vector<std::string> out;
    const JsonValue* v = find(obj, key);
    if (v == nullptr || v->type != JsonType::Array) return out;
    for (const auto& el : v->arr) {
        if (el.type == JsonType::String) out.push_back(el.s);
    }
    return out;
}

} // namespace json_detail
} // namespace graph
} // namespace pstwriter
