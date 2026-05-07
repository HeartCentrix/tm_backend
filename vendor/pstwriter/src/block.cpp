// pstwriter/block.cpp
//
// M3: byte-level builders for every block kind in [MS-PST] §2.2.2.8.

#include "block.hpp"

#include "crc.hpp"
#include "encoding.hpp"
#include "ndb.hpp"
#include "types.hpp"

#include <cassert>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <vector>

using namespace std;

namespace pstwriter {

using detail::writeU8;
using detail::writeU16;
using detail::writeU32;
using detail::writeU64;
using detail::readU16;
using detail::readU32;
using detail::readU64;

namespace {

// Build the on-disk image of an internal block (X / XX / SL / SI).
//
//   layout = [btype][cLevel][cEnt][lcbTotal-or-dwPadding][rgentries[]][pad to 64][trailer]
//
// `cb` field of the trailer is the LOGICAL body size: the bytes from
// offset 0 up through the end of rgentries, NOT including padding nor
// the 16-byte trailer.  The spec is consistent on this for all four
// internal block flavours.
vector<uint8_t> buildInternalBlock(uint8_t        btype,
                                   uint8_t        cLevel,
                                   uint16_t       cEnt,
                                   uint32_t       lcbTotalOrPad,
                                   const uint8_t* entriesRaw,
                                   size_t         entryBytes,
                                   Bid            bid,
                                   Ib             ib) noexcept
{
    const size_t bodyBytes = kInternalBlockHeaderSize + entryBytes;
    const size_t totalSize = roundBlockSize(bodyBytes);

    vector<uint8_t> buf(totalSize, uint8_t{0});
    uint8_t* p = buf.data();

    writeU8 (p, 0, btype);
    writeU8 (p, 1, cLevel);
    writeU16(p, 2, cEnt);
    writeU32(p, 4, lcbTotalOrPad);
    if (entryBytes > 0 && entriesRaw != nullptr) {
        memcpy(p + kInternalBlockHeaderSize, entriesRaw, entryBytes);
    }
    // Bytes [bodyBytes .. totalSize - 16) are zero padding.

    // BLOCKTRAILER goes in the last 16 bytes. cb is the LOGICAL body size.
    // [MS-PST] §2.2.2.8.1: dwCRC is the CRC of `cb` bytes of raw data, NOT
    // including the alignment padding. Scope = bodyBytes, not trailerOff.
    // (Confirmed by an Outlook 16.0.19929 BCRead error that named the
    //  cb-only CRC as the "Expected" value when the writer briefly used
    //  trailerOff scope; pst_info's trailerOff check was non-conforming.)
    const size_t trailerOff = totalSize - kBlockTrailerSize;
    const uint32_t dwCRC = crc32(p, bodyBytes);

    writeU16(p, trailerOff + 0, static_cast<uint16_t>(bodyBytes));
    writeU16(p, trailerOff + 2, computeBlockSig(bid, ib));
    writeU32(p, trailerOff + 4, dwCRC);
    writeU64(p, trailerOff + 8, bid.value);

    return buf;
}

} // anonymous namespace

// ===========================================================================
// Data block ([MS-PST] §2.2.2.8.3.1)
// ===========================================================================
vector<uint8_t> buildDataBlock(const uint8_t* payload,
                               size_t         cbPayload,
                               Bid            bid,
                               Ib             ib,
                               CryptMethod    method) noexcept
{
    // cbPayload must fit; the caller chops large data into multiple
    // blocks before calling this (typically max 8176 bytes/block).
    const size_t totalSize = roundBlockSize(cbPayload);
    vector<uint8_t> buf(totalSize, uint8_t{0});

    if (cbPayload > 0 && payload != nullptr) {
        memcpy(buf.data(), payload, cbPayload);
    }

    // Encrypt in place using the lower 32 bits of the data block's BID.
    const uint32_t cyclicKey = static_cast<uint32_t>(bid.value & 0xFFFFFFFFull);
    encodeBlock(buf.data(), cbPayload, method, cyclicKey);

    // [MS-PST] §2.2.2.8.1: dwCRC is the CRC of `cb` bytes of raw data —
    // the encrypted payload only, NOT the alignment padding. Verified
    // empirically against backup.pst on 2026-05-04 (5/5 sampled blocks
    // matched cb-only scope; 0/5 matched cb+padding scope) AND confirmed
    // again on 2026-05-05 by an Outlook 16.0.19929 BCRead error that
    // named the cb-only CRC as the "Expected" value when this scope was
    // briefly changed to trailerOff. pst_info uses trailerOff for its
    // own CRC walk — that is a pst_info bug, not a writer bug.
    const size_t trailerOff = totalSize - kBlockTrailerSize;
    const uint32_t dwCRC = crc32(buf.data(), cbPayload);

    writeU16(buf.data(), trailerOff + 0, static_cast<uint16_t>(cbPayload));
    writeU16(buf.data(), trailerOff + 2, computeBlockSig(bid, ib));
    writeU32(buf.data(), trailerOff + 4, dwCRC);
    writeU64(buf.data(), trailerOff + 8, bid.value);

    return buf;
}

// ===========================================================================
// XBLOCK / XXBLOCK ([MS-PST] §2.2.2.8.3.2)
// ===========================================================================
namespace {

vector<uint8_t> buildBidListBlock(uint8_t    cLevel,
                                  const Bid* childBids,
                                  size_t     count,
                                  uint32_t   lcbTotal,
                                  Bid        bid,
                                  Ib         ib) noexcept
{
    // Pack children into a flat buffer of LE uint64.
    vector<uint8_t> rgbid(count * kXBlockEntrySize, uint8_t{0});
    for (size_t i = 0; i < count; ++i) {
        writeU64(rgbid.data(), i * kXBlockEntrySize, childBids[i].value);
    }
    return buildInternalBlock(/*btype=*/0x01u,
                              cLevel,
                              static_cast<uint16_t>(count),
                              lcbTotal,
                              rgbid.data(),
                              rgbid.size(),
                              bid,
                              ib);
}

} // anonymous namespace

vector<uint8_t> buildXBlock(const Bid* childBids,
                            size_t     count,
                            uint32_t   lcbTotal,
                            Bid        bid,
                            Ib         ib) noexcept
{
    return buildBidListBlock(/*cLevel=*/1u, childBids, count, lcbTotal, bid, ib);
}

vector<uint8_t> buildXXBlock(const Bid* childBids,
                             size_t     count,
                             uint32_t   lcbTotal,
                             Bid        bid,
                             Ib         ib) noexcept
{
    return buildBidListBlock(/*cLevel=*/2u, childBids, count, lcbTotal, bid, ib);
}

// ===========================================================================
// SLBLOCK / SIBLOCK ([MS-PST] §2.2.2.8.3.3)
// ===========================================================================
vector<uint8_t> buildSlBlock(const SlEntry* entries,
                             size_t         count,
                             Bid            bid,
                             Ib             ib) noexcept
{
    vector<uint8_t> rg(count * kSlEntrySize, uint8_t{0});
    for (size_t i = 0; i < count; ++i) {
        const size_t off = i * kSlEntrySize;
        writeU64(rg.data(), off + 0,  static_cast<uint64_t>(entries[i].nid.value));
        writeU64(rg.data(), off + 8,  entries[i].bidData.value);
        writeU64(rg.data(), off + 16, entries[i].bidSub.value);
    }
    return buildInternalBlock(/*btype=*/0x02u,
                              /*cLevel=*/0u,
                              static_cast<uint16_t>(count),
                              /*dwPadding=*/0u,
                              rg.data(),
                              rg.size(),
                              bid,
                              ib);
}

vector<uint8_t> buildSiBlock(const SiEntry* entries,
                             size_t         count,
                             Bid            bid,
                             Ib             ib) noexcept
{
    vector<uint8_t> rg(count * kSiEntrySize, uint8_t{0});
    for (size_t i = 0; i < count; ++i) {
        const size_t off = i * kSiEntrySize;
        writeU64(rg.data(), off + 0, static_cast<uint64_t>(entries[i].nid.value));
        writeU64(rg.data(), off + 8, entries[i].bid.value);
    }
    return buildInternalBlock(/*btype=*/0x02u,
                              /*cLevel=*/1u,
                              static_cast<uint16_t>(count),
                              /*dwPadding=*/0u,
                              rg.data(),
                              rg.size(),
                              bid,
                              ib);
}

// ===========================================================================
// Reader helper
// ===========================================================================
BlockTrailerView readBlockTrailer(const uint8_t* block, size_t total) noexcept
{
    BlockTrailerView t{};
    if (total < kBlockTrailerSize) {
        return t;
    }
    const size_t off = total - kBlockTrailerSize;
    t.cb    = readU16(block, off + 0);
    t.wSig  = readU16(block, off + 2);
    t.dwCRC = readU32(block, off + 4);
    t.bid   = readU64(block, off + 8);
    return t;
}

} // namespace pstwriter
