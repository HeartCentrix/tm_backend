// pstwriter/src/graph_convert.cpp
//
// M7 Phase A — Graph-JSON conversion utility implementations.

#include "graph_convert.hpp"

#include "types.hpp"

#include <cctype>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

using std::string;
using std::vector;

namespace pstwriter {
namespace graph {

// ============================================================================
// utf8ToUtf16le
// ============================================================================
namespace {

// Append a UTF-16 code unit (LE) to dst.
void appendU16Le(vector<uint8_t>& dst, uint32_t u16)
{
    dst.push_back(static_cast<uint8_t>(u16 & 0xFFu));
    dst.push_back(static_cast<uint8_t>((u16 >> 8) & 0xFFu));
}

} // namespace

vector<uint8_t> utf8ToUtf16le(const string& s)
{
    vector<uint8_t> out;
    out.reserve(s.size() * 2);

    size_t i = 0;
    while (i < s.size()) {
        const uint8_t b0 = static_cast<uint8_t>(s[i]);
        uint32_t cp = 0;
        size_t   n  = 0;

        if ((b0 & 0x80u) == 0u) {
            cp = b0;
            n  = 1;
        } else if ((b0 & 0xE0u) == 0xC0u) {
            if (i + 1 >= s.size()) throw std::invalid_argument("utf8ToUtf16le: truncated 2-byte seq");
            const uint8_t b1 = static_cast<uint8_t>(s[i + 1]);
            if ((b1 & 0xC0u) != 0x80u) throw std::invalid_argument("utf8ToUtf16le: bad continuation");
            cp = (static_cast<uint32_t>(b0 & 0x1Fu) << 6)
               |  static_cast<uint32_t>(b1 & 0x3Fu);
            n  = 2;
        } else if ((b0 & 0xF0u) == 0xE0u) {
            if (i + 2 >= s.size()) throw std::invalid_argument("utf8ToUtf16le: truncated 3-byte seq");
            const uint8_t b1 = static_cast<uint8_t>(s[i + 1]);
            const uint8_t b2 = static_cast<uint8_t>(s[i + 2]);
            if ((b1 & 0xC0u) != 0x80u || (b2 & 0xC0u) != 0x80u)
                throw std::invalid_argument("utf8ToUtf16le: bad continuation");
            cp = (static_cast<uint32_t>(b0 & 0x0Fu) << 12)
               | (static_cast<uint32_t>(b1 & 0x3Fu) << 6)
               |  static_cast<uint32_t>(b2 & 0x3Fu);
            n  = 3;
        } else if ((b0 & 0xF8u) == 0xF0u) {
            if (i + 3 >= s.size()) throw std::invalid_argument("utf8ToUtf16le: truncated 4-byte seq");
            const uint8_t b1 = static_cast<uint8_t>(s[i + 1]);
            const uint8_t b2 = static_cast<uint8_t>(s[i + 2]);
            const uint8_t b3 = static_cast<uint8_t>(s[i + 3]);
            if ((b1 & 0xC0u) != 0x80u || (b2 & 0xC0u) != 0x80u || (b3 & 0xC0u) != 0x80u)
                throw std::invalid_argument("utf8ToUtf16le: bad continuation");
            cp = (static_cast<uint32_t>(b0 & 0x07u) << 18)
               | (static_cast<uint32_t>(b1 & 0x3Fu) << 12)
               | (static_cast<uint32_t>(b2 & 0x3Fu) << 6)
               |  static_cast<uint32_t>(b3 & 0x3Fu);
            n  = 4;
        } else {
            throw std::invalid_argument("utf8ToUtf16le: invalid leading byte");
        }

        if (cp <= 0xFFFFu) {
            // Disallow lone surrogates in source.
            if (cp >= 0xD800u && cp <= 0xDFFFu)
                throw std::invalid_argument("utf8ToUtf16le: lone surrogate codepoint");
            appendU16Le(out, cp);
        } else if (cp <= 0x10FFFFu) {
            const uint32_t v  = cp - 0x10000u;
            const uint32_t hi = 0xD800u | (v >> 10);
            const uint32_t lo = 0xDC00u | (v & 0x3FFu);
            appendU16Le(out, hi);
            appendU16Le(out, lo);
        } else {
            throw std::invalid_argument("utf8ToUtf16le: codepoint out of range");
        }

        i += n;
    }

    return out;
}

// ============================================================================
// isoToFiletime
// ============================================================================
namespace {

// Days from 1601-01-01 to start of year y. Inclusive of leap years.
int64_t daysFromCivil(int y, unsigned m, unsigned d) noexcept
{
    // Howard Hinnant's date algorithm. Computes serial days from civil date.
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097LL + static_cast<int64_t>(doe) - 719468LL;
    // Result is days since 1970-01-01.
}

bool isDigit(char c) noexcept { return c >= '0' && c <= '9'; }

int parseInt(const string& s, size_t& pos, size_t n)
{
    if (pos + n > s.size())
        throw std::invalid_argument("isoToFiletime: truncated numeric field");
    int v = 0;
    for (size_t k = 0; k < n; ++k) {
        const char c = s[pos + k];
        if (!isDigit(c))
            throw std::invalid_argument("isoToFiletime: non-digit in numeric field");
        v = v * 10 + (c - '0');
    }
    pos += n;
    return v;
}

} // namespace

uint64_t isoToFiletimeTicks(const string& iso)
{
    if (iso.empty()) return 0u;

    // Minimum: "YYYY-MM-DDTHH:MM:SS" = 19 chars
    if (iso.size() < 19)
        throw std::invalid_argument("isoToFiletime: input too short");

    size_t pos = 0;
    const int  year  = parseInt(iso, pos, 4);
    if (iso[pos++] != '-') throw std::invalid_argument("isoToFiletime: expected '-'");
    const int  month = parseInt(iso, pos, 2);
    if (iso[pos++] != '-') throw std::invalid_argument("isoToFiletime: expected '-'");
    const int  day   = parseInt(iso, pos, 2);
    if (iso[pos] != 'T' && iso[pos] != ' ')
        throw std::invalid_argument("isoToFiletime: expected 'T' or ' '");
    ++pos;
    const int  hour  = parseInt(iso, pos, 2);
    if (iso[pos++] != ':') throw std::invalid_argument("isoToFiletime: expected ':'");
    const int  minute = parseInt(iso, pos, 2);
    if (iso[pos++] != ':') throw std::invalid_argument("isoToFiletime: expected ':'");
    const int  second = parseInt(iso, pos, 2);

    // Optional fractional seconds.
    uint64_t fracTicks = 0;
    if (pos < iso.size() && iso[pos] == '.') {
        ++pos;
        // Up to 7 digits = 100ns resolution. Truncate beyond.
        unsigned long long frac = 0;
        size_t fracDigits = 0;
        while (pos < iso.size() && isDigit(iso[pos]) && fracDigits < 9) {
            frac = frac * 10 + static_cast<unsigned>(iso[pos] - '0');
            ++pos; ++fracDigits;
        }
        // Skip any further digits (over-precision input).
        while (pos < iso.size() && isDigit(iso[pos])) ++pos;
        // Convert to 100ns ticks.
        // 1 second = 10,000,000 ticks. fracDigits gives the implicit divisor.
        // We have frac * 10^(7 - fracDigits) ticks, modulo capping at 7.
        if (fracDigits > 7) {
            // Truncate
            unsigned long long divisor = 1;
            for (size_t k = 0; k < fracDigits - 7; ++k) divisor *= 10;
            frac /= divisor;
            fracDigits = 7;
        }
        unsigned long long mult = 1;
        for (size_t k = 0; k < 7 - fracDigits; ++k) mult *= 10;
        fracTicks = static_cast<uint64_t>(frac * mult);
    }

    // Optional timezone designator.
    int64_t offsetSec = 0;
    if (pos < iso.size()) {
        const char c = iso[pos];
        if (c == 'Z' || c == 'z') {
            ++pos;
        } else if (c == '+' || c == '-') {
            const int sign = (c == '+') ? 1 : -1;
            ++pos;
            const int oh = parseInt(iso, pos, 2);
            if (pos < iso.size() && iso[pos] == ':') ++pos;
            int om = 0;
            if (pos + 2 <= iso.size() && isDigit(iso[pos]) && isDigit(iso[pos + 1])) {
                om = parseInt(iso, pos, 2);
            }
            offsetSec = sign * (oh * 3600LL + om * 60LL);
        }
        // else: ignore trailing garbage. Graph should not emit any.
    }

    // Compute days from 1601-01-01 to date.
    const int64_t daysFrom1970     = daysFromCivil(year, static_cast<unsigned>(month),
                                                   static_cast<unsigned>(day));
    constexpr int64_t kDays1601To1970 = 134774;  // (1970-01-01) - (1601-01-01)
    const int64_t daysFrom1601     = daysFrom1970 + kDays1601To1970;

    int64_t totalSec = daysFrom1601 * 86400LL
                     + hour   * 3600LL
                     + minute * 60LL
                     + second
                     - offsetSec;
    if (totalSec < 0) {
        // Pre-1601 not supported.
        throw std::invalid_argument("isoToFiletime: pre-epoch time");
    }

    return static_cast<uint64_t>(totalSec) * 10000000ull + fracTicks;
}

std::array<uint8_t, 8> isoToFiletime(const string& iso)
{
    const uint64_t ticks = isoToFiletimeTicks(iso);
    std::array<uint8_t, 8> out{};
    detail::writeU64(out.data(), 0, ticks);
    return out;
}

// ============================================================================
// base64DecodeBinary (RFC 4648 standard alphabet)
// ============================================================================
namespace {

int b64Val(char c) noexcept
{
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return 26 + (c - 'a');
    if (c >= '0' && c <= '9') return 52 + (c - '0');
    if (c == '+')             return 62;
    if (c == '/')             return 63;
    return -1;
}

} // namespace

vector<uint8_t> base64DecodeBinary(const string& b64)
{
    // Strip whitespace into a working string.
    string s;
    s.reserve(b64.size());
    for (char c : b64) {
        if (c == ' ' || c == '\r' || c == '\n' || c == '\t') continue;
        s.push_back(c);
    }

    // Drop trailing '='.
    while (!s.empty() && s.back() == '=') s.pop_back();

    vector<uint8_t> out;
    out.reserve((s.size() * 3) / 4);

    uint32_t accum = 0;
    int      bits  = 0;
    for (char c : s) {
        const int v = b64Val(c);
        if (v < 0) throw std::invalid_argument("base64DecodeBinary: invalid char");
        accum = (accum << 6) | static_cast<uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<uint8_t>((accum >> bits) & 0xFFu));
        }
    }
    return out;
}

// ============================================================================
// makeOneOffEntryId
// ============================================================================
vector<uint8_t> makeOneOffEntryId(const string& displayName,
                                  const string& smtpAddress)
{
    vector<uint8_t> out;
    out.reserve(32 + displayName.size() * 2 + smtpAddress.size() * 2 + 16);

    // bytes 0..3 rgbFlags = 0
    out.insert(out.end(), 4, 0u);
    // bytes 4..19 ProviderUID (well-known OneOff GUID)
    out.insert(out.end(), kOneOffProviderUid.begin(), kOneOffProviderUid.end());

    // bytes 20..21 Version = 0
    out.push_back(0u); out.push_back(0u);

    // bytes 22..23 Flags = MAPI_ONE_OFF_UNICODE | MAPI_ONE_OFF_NO_RICH_INFO
    //   = 0x8000 | 0x0001 = 0x8001
    // (We omit the "explicit reply" 0x1000 bit; only set what we know.)
    out.push_back(0x01u);   // low byte
    out.push_back(0x90u);   // high byte = 0x90 -> Flags = 0x9001
                            //   bit 0 (no rich info) + bit 12 + bit 15 (Unicode)
                            // Per [MS-OXCDATA], MAPI_ONE_OFF_UNICODE = 0x8000
                            // and the additional 0x1000 marks a non-OAB resolution.
                            // Real Outlook samples set 0x9001 for Unicode SMTP.

    auto appendUtf16NullTerm = [&](const string& s) {
        const auto u16 = utf8ToUtf16le(s);
        out.insert(out.end(), u16.begin(), u16.end());
        out.push_back(0u); out.push_back(0u);  // UTF-16 NUL
    };

    appendUtf16NullTerm(displayName);
    appendUtf16NullTerm("SMTP");
    appendUtf16NullTerm(smtpAddress);

    return out;
}

// ============================================================================
// deriveSearchKey
// ============================================================================
std::array<uint8_t, 16> deriveSearchKey(const string& smtpAddress)
{
    // Build "SMTP:<UPPER(address)>" + NUL byte. Truncate or zero-pad to 16.
    string s = "SMTP:";
    for (char c : smtpAddress) {
        s.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    std::array<uint8_t, 16> out{};
    const size_t n = s.size() < 16 ? s.size() : 16;
    for (size_t i = 0; i < n; ++i) out[i] = static_cast<uint8_t>(s[i]);
    return out;
}

} // namespace graph
} // namespace pstwriter
