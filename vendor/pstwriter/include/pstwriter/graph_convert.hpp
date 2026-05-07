// pstwriter/graph_convert.hpp
//
// M7 Phase A — Graph-JSON conversion utilities, shared by M7 (mail),
// M8 (contacts) and M9 (calendar).
//
// Each utility crosses the boundary between Microsoft Graph JSON
// representations and the byte-level shapes [MS-PST] expects:
//
//   utf8ToUtf16le        — UTF-8 source string -> UTF-16-LE byte vector
//                           (no BOM, no terminator). Used for every
//                           PtypString slot (PidTagDisplayName,
//                           PidTagSubject_W, PidTagBody_W, etc.).
//   isoToFiletime        — ISO 8601 date-time string (Graph format) ->
//                           8-byte little-endian FILETIME (100ns ticks
//                           since 1601-01-01 UTC).
//   base64DecodeBinary   — Graph base64 fields (conversationIndex,
//                           attachment.contentBytes) -> raw bytes.
//   makeOneOffEntryId    — OneOff EntryID per [MS-OXCDATA] §2.2.5.1
//                           for an SMTP-addressed sender/recipient.
//   deriveSearchKey      — 16-byte deterministic search key derived
//                           from "SMTP:<addr>".

#pragma once

#include "types.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace pstwriter {
namespace graph {

// ----------------------------------------------------------------------------
// utf8ToUtf16le
//
// Decodes UTF-8 codepoints (1-4 byte sequences) and re-encodes them as
// UTF-16-LE bytes. ASCII subset stays at 2 bytes per char (ASCII byte
// followed by 0x00). Codepoints above U+FFFF emit a surrogate pair.
//
// On invalid UTF-8 input throws std::invalid_argument with a short
// human-readable message.
// ----------------------------------------------------------------------------
std::vector<uint8_t> utf8ToUtf16le(const std::string& s);

// ----------------------------------------------------------------------------
// isoToFiletime
//
// Parses Graph's ISO 8601 timestamp format. Accepts:
//   "YYYY-MM-DDTHH:MM:SSZ"           (UTC)
//   "YYYY-MM-DDTHH:MM:SS.ffffffZ"    (UTC with sub-second)
//   "YYYY-MM-DDTHH:MM:SS.fffffff"    (no Z, treated as UTC per Graph)
//   "YYYY-MM-DDTHH:MM:SS.ffffff+HH:MM" / "-HH:MM" (offset)
//
// FILETIME ticks = 100ns intervals since 1601-01-01 00:00:00 UTC.
// Returns 8 little-endian bytes. Throws std::invalid_argument on
// malformed input. Returns all-zero on empty string.
// ----------------------------------------------------------------------------
std::array<uint8_t, 8> isoToFiletime(const std::string& iso);

// Convenience: produce the same value as a uint64_t.
uint64_t isoToFiletimeTicks(const std::string& iso);

// ----------------------------------------------------------------------------
// base64DecodeBinary
//
// Decodes a standard-alphabet base64 string (RFC 4648). Whitespace and
// '=' padding tolerated; non-alphabet characters trigger
// std::invalid_argument.
// ----------------------------------------------------------------------------
std::vector<uint8_t> base64DecodeBinary(const std::string& b64);

// ----------------------------------------------------------------------------
// makeOneOffEntryId
//
// Per [MS-OXCDATA] §2.2.5.1 OneOff EntryID:
//
//   bytes  0.. 3   rgbFlags (4 bytes, all-zero)
//   bytes  4..19   ProviderUID = {0xA1, 0x6F, 0xC2, ..., 0xC8} (the
//                  16-byte well-known OneOff ProviderUID, NIDs §2.4.10)
//   bytes 20..21   Version (LE uint16, = 0x0000)
//   bytes 22..23   Flags (LE uint16):
//                    bit 0 (0x0001) = MAPI_ONE_OFF_NO_RICH_INFO
//                    bit 15 (0x8000) = MAPI_ONE_OFF_UNICODE
//                  M7 sets both = 0x9001
//                  - 0x8000 (Unicode) — DisplayName/AddressType/Email
//                                       are UTF-16-LE
//                  - 0x1000 reserved per OXCDATA — verify at backup.pst
//                  - 0x0001 (no rich info) — SMTP addresses don't carry
//                                            rich-format capabilities
//   bytes 24..    DisplayName  (UTF-16-LE, null-terminated)
//   bytes ...     AddressType  (UTF-16-LE, null-terminated; "SMTP")
//   bytes ...     EmailAddress (UTF-16-LE, null-terminated)
//
// Returns the assembled byte vector. displayName and smtpAddress are
// UTF-8 strings (Graph-native form).
//
// **KNOWN_UNVERIFIED M7-3**: Exact rgbFlags and Flags bits are best-
// effort per the spec text; verified at Phase E real-Outlook gate.
// ----------------------------------------------------------------------------
std::vector<uint8_t> makeOneOffEntryId(const std::string& displayName,
                                       const std::string& smtpAddress);

// The well-known OneOff ProviderUID GUID per [MS-OXCDATA] §2.2.5.1.
// Exposed for testing.
constexpr std::array<uint8_t, 16> kOneOffProviderUid = {{
    0x81, 0x2B, 0x1F, 0xA4,
    0xBE, 0xA3, 0x10, 0x19,
    0x9D, 0x6E, 0x00, 0xDD,
    0x01, 0x0F, 0x54, 0x02,
}};

// ----------------------------------------------------------------------------
// deriveSearchKey
//
// 16-byte search key for an SMTP recipient. Per [MS-OXOMSG] the search
// key is derived from "SMTP:<UPPER(address)>" — we use ASCII bytes
// truncated/padded to 16 bytes. Deterministic: same address -> same key.
//
// Used for PidTagSearchKey in recipient TC and sender properties.
// ----------------------------------------------------------------------------
std::array<uint8_t, 16> deriveSearchKey(const std::string& smtpAddress);

// ----------------------------------------------------------------------------
// deriveMessageSearchKey
//
// 16-byte deterministic search key for a message PC's PidTagSearchKey
// (0x300B0102). Per [MS-OXCMSG] §2.2.1.4, every IPM.* message MUST have
// PR_SEARCH_KEY — Outlook uses it for conversation tracking and dup
// detection. Derived from a stable seed string (typically the
// `internetMessageId` if non-empty, else `subject + sentDateTime`).
//
// Implementation: two-pass FNV-1a 64-bit hash with different offset
// bases, packed little-endian into 16 bytes. Stable across builds; not
// cryptographic. (M11-K P1.)
// ----------------------------------------------------------------------------
std::array<uint8_t, 16> deriveMessageSearchKey(const std::string& seed);

} // namespace graph
} // namespace pstwriter
