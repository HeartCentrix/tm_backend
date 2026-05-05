// pstwriter/ndb.cpp
//
// HEADER + ROOT + page/block trailer serialization for Unicode PST.
//
// The single most important rule in this file: every byte goes through
// detail::writeU8/16/32/64. Never memcpy or reinterpret_cast a host
// struct into the buffer (KNOWN BUG #7). MSVC will pad and silently
// corrupt the file; Outlook will reject it with no useful error.

#include "ndb.hpp"

#include "crc.hpp"
#include "types.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstddef>
#include <cstring>

using namespace std;

namespace pstwriter {

using detail::writeU8;
using detail::writeU16;
using detail::writeU32;
using detail::writeU64;

// ============================================================================
// Page / block trailers
// ============================================================================
void writePageTrailer(array<uint8_t, kPageSize>& page,
                      uint8_t                    ptype,
                      Bid                        bid,
                      Ib                         ib) noexcept
{
    const size_t off = kPageTrailerOffset;

    // ptype + ptypeRepeat (must match)
    writeU8(page.data(), off + 0, ptype);
    writeU8(page.data(), off + 1, ptype);

    // wSig — per [MS-PST] §2.2.2.7.1 / §5.5:
    //   * AMap, PMap, FMap, FPMap: wSig = 0
    //   * NBT, BBT, DList:         wSig = (uint16_t)((ib ^ bid.value) & 0xFFFF)
    uint16_t wSig = 0;
    switch (ptype) {
    case ptype::kNBT:
    case ptype::kBBT:
    case ptype::kDList:
        wSig = computeBlockSig(bid, ib);
        break;
    default:
        wSig = 0;
        break;
    }
    writeU16(page.data(), off + 2, wSig);

    // dwCRC over the first 496 bytes — must be computed BEFORE writing
    // it back into the page (otherwise we'd be CRCing zeros).
    const uint32_t dwCRC = crc32(page.data(), kPageBodySize);
    writeU32(page.data(), off + 4, dwCRC);

    // bid — every page type stores its own BID here (libpff and the
    // SPEC_GROUND_TRUTH default agree). SPEC_BRIEF's earlier note that
    // AMap/PMap/FMap had bid=0 was incorrect.
    writeU64(page.data(), off + 8, bid.value);
}

void appendBlockTrailer(vector<uint8_t>& bytes,
                        uint16_t         cbPlaintext,
                        Bid              bid,
                        Ib               ib) noexcept
{
    // CRC is taken over the encoded payload bytes ALREADY in `bytes`,
    // which the caller produced with encodePermute / encodeCyclic.
    const uint32_t dwCRC = crc32(bytes.data(), bytes.size());

    const size_t base = bytes.size();
    bytes.resize(base + kBlockTrailerSize);
    uint8_t* p = bytes.data() + base;

    writeU16(p, 0, cbPlaintext);                       // cb     — pre-encoding payload size
    writeU16(p, 2, computeBlockSig(bid, ib));          // wSig   — uses bid.value, NOT bid.index()
    writeU32(p, 4, dwCRC);                             // dwCRC  — over encoded payload
    writeU64(p, 8, bid.value);                         // bid    — raw 64-bit value
}

// ============================================================================
// rgnid[] for a fresh PST — spec-mandated starting indices per nidType.
//   NORMAL_FOLDER  (0x02): start at index 0x400   -> raw 0x00008002
//   SEARCH_FOLDER  (0x03): start at index 0x4000  -> raw 0x00080003
//   NORMAL_MESSAGE (0x04): start at index 0x10000 -> raw 0x00200004
//   ASSOC_MESSAGE  (0x08): start at index 0x8000  -> raw 0x00100008
//   everything else: start at index 0x400 -> raw (0x400 << 5) | nidType
// ============================================================================
void populateFreshRgnid(WriterState& s) noexcept
{
    for (uint32_t nidType = 0; nidType < 32; ++nidType) {
        uint32_t startIdx;
        switch (nidType) {
        case 0x03u: startIdx = 0x4000u;  break;
        case 0x04u: startIdx = 0x10000u; break;
        case 0x08u: startIdx = 0x8000u;  break;
        default:    startIdx = 0x400u;   break;
        }
        s.rgnid[nidType] = (startIdx << 5) | (nidType & 0x1Fu);
    }
}

// ============================================================================
// HEADER serialization — verified against [MS-PST] §3.2 sample header.
// HEADER is 564 bytes (0x000..0x233).  There is no separate "header copy".
// ============================================================================
array<uint8_t, kHeaderSize> serializeHeader(const WriterState& s) noexcept
{
    array<uint8_t, kHeaderSize> buf{};
    uint8_t* p = buf.data();

    // ---- Fixed preamble (0x000..0x017) ----
    writeU32(p, hdr::kMagic,          kMagicDword);     // "!BDN"
    // dwCRCPartial at 0x004 — written at the bottom, after the rest of
    // the CRC-protected region is in place.
    writeU16(p, hdr::kMagicClient,    kMagicClient);    // "SM"
    writeU16(p, hdr::kVer,            kVerUnicode);     // 23
    writeU16(p, hdr::kVerClient,      kVerClient);      // 19
    writeU8 (p, hdr::kPlatformCreate, kPlatformCreate); // 0x01
    writeU8 (p, hdr::kPlatformAccess, kPlatformAccess); // 0x01
    writeU32(p, hdr::kReserved1,      s.dwReserved1);
    writeU32(p, hdr::kReserved2,      s.dwReserved2);

    // ---- bidUnused / bidNextP / dwUnique / rgnid / qwUnused ----
    writeU64(p, hdr::kBidUnused, s.bidUnused);
    writeU64(p, hdr::kBidNextP,  s.bidNextP.value);
    writeU32(p, hdr::kDwUnique,  s.dwUnique);

    // rgnid[32] — caller is responsible for filling these.  For a fresh
    // PST, populateFreshRgnid() pre-computes the §2.2.2.6 starting
    // indices.  For round-tripping an existing HEADER, the caller copies
    // the on-disk values verbatim.
    for (size_t i = 0; i < 32; ++i) {
        writeU32(p, hdr::kRgnid + i * 4u, s.rgnid[i]);
    }

    writeU64(p, hdr::kQwUnused, 0ull);

    // ---- ROOT (0x0B4..0x0FB) ----
    writeU32(p, hdr::kRootDwReserved, 0u);
    writeU64(p, hdr::kIbFileEof,      s.root.ibFileEof.value);
    writeU64(p, hdr::kIbAMapLast,     s.root.ibAMapLast.value);
    writeU64(p, hdr::kCbAMapFree,     s.root.cbAMapFree);
    writeU64(p, hdr::kCbPMapFree,     s.root.cbPMapFree);
    writeU64(p, hdr::kBrefNbtBid,     s.root.brefNbt.bid.value);
    writeU64(p, hdr::kBrefNbtIb,      s.root.brefNbt.ib.value);
    writeU64(p, hdr::kBrefBbtBid,     s.root.brefBbt.bid.value);
    writeU64(p, hdr::kBrefBbtIb,      s.root.brefBbt.ib.value);
    writeU8 (p, hdr::kFAMapValid,     s.root.fAMapValid);     // 0x02 (VALID_AMAP2)
    writeU8 (p, hdr::kRootBReserved,  0u);
    writeU16(p, hdr::kRootWReserved,  0u);

    // ---- dwAlign + rgbFM[128] + rgbFP[128] (0xFC..0x1FF) ----
    writeU32(p, hdr::kDwAlign, 0u);
    fill_n(p + hdr::kRgbFM, 128, uint8_t{0xFFu});
    fill_n(p + hdr::kRgbFP, 128, uint8_t{0xFFu});

    // ---- Tail (0x200..0x233) — bSentinel, bCryptMethod, bidNextB, etc.
    // These are at offsets PAST 0x200 in the real HEADER, NOT inside a
    // separate "header copy". The spec sample makes that unambiguous.
    writeU8 (p, hdr::kBSentinel,    kSentinelByte);                                  // 0x80
    writeU8 (p, hdr::kBCryptMethod, static_cast<uint8_t>(CryptMethod::Permute));     // 0x01
    writeU16(p, hdr::kRgbReserved,  0u);                                             // 2 bytes
    writeU64(p, hdr::kBidNextB,     s.bidNextB.value);                               // 8 bytes
    // dwCRCFull at 0x20C written at the bottom.
    writeU16(p, hdr::kRgbReserved2,     0u);                                         // 3 bytes total
    writeU8 (p, hdr::kRgbReserved2 + 2, 0u);
    writeU8 (p, hdr::kBReserved,        0u);
    // rgbReserved3[32] at 0x214..0x233 is left zero by buf{}.

    // ---- CRCs ([MS-PST] §2.2.2.6, end-to-end verified) ----
    //   dwCRCPartial at 0x004 = crc32(p + 0x008, 471 bytes)
    //   dwCRCFull    at 0x20C = crc32(p + 0x008, 516 bytes)
    // Both ranges start at 0x008, after both CRC fields, so write order
    // doesn't matter for correctness.
    const uint32_t dwCRCPartial =
        crc32(p + hdr::kMagicClient, kHdrCrcPartialLen);
    writeU32(p, hdr::kCrcPartial, dwCRCPartial);

    const uint32_t dwCRCFull =
        crc32(p + hdr::kMagicClient, kHdrCrcFullLen);
    writeU32(p, hdr::kCrcFull, dwCRCFull);

    return buf;
}

// Page-level builders moved to src/page.cpp.

} // namespace pstwriter
