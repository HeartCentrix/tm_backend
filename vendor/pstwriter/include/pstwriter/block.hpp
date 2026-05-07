// pstwriter/block.hpp
//
// Block primitives ([MS-PST] §2.2.2.8) for the M3 milestone.
//
// Block kinds:
//
//   * Data block          (§2.2.2.8.3.1)
//                         payload + 16-byte BLOCKTRAILER, 64-byte aligned
//                         payload is encrypted in place via the PST's
//                         CryptMethod before the trailer CRC is computed.
//
//   * XBLOCK              (§2.2.2.8.3.2.1) — internal, btype=0x01, cLevel=1
//                         body = btype + cLevel + cEnt + lcbTotal + rgbid[cEnt]
//                         indirects to a list of data block BIDs.
//
//   * XXBLOCK             (§2.2.2.8.3.2.2) — internal, btype=0x01, cLevel=2
//                         body = same shape; indirects to a list of XBLOCKs.
//
//   * SLBLOCK             (§2.2.2.8.3.3.1) — internal, btype=0x02, cLevel=0
//                         body = btype + cLevel + cEnt + dwPadding + SLENTRY[]
//                         maps subnode NIDs to (bidData, bidSub) pairs.
//
//   * SIBLOCK             (§2.2.2.8.3.3.2) — internal, btype=0x02, cLevel=1
//                         body = btype + cLevel + cEnt + dwPadding + SIENTRY[]
//                         indexes a list of SLBLOCKs.
//
// IMPORTANT distinctions:
//   * Data blocks ARE encrypted; internal blocks (X/XX/SL/SI) are NOT.
//   * BLOCKTRAILER.cb is the pre-encryption payload size for data blocks
//     and the structured body size (NOT including padding / trailer) for
//     internal blocks.
//   * Internal blocks have BID with bit[1] set (Bid::makeInternal()); data
//     blocks have bit[1] clear (Bid::makeData()).
//
// All builders return a vector<uint8_t> sized to a multiple of 64 — ready
// to fwrite at the block's chosen file offset.

#pragma once

#include "ndb.hpp"   // for kBlockTrailerSize, computeBlockSig, etc.
#include "types.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

using namespace std;

namespace pstwriter {

// Block-builder capacity constants ([MS-PST] §2.2.2.8.3.x).
constexpr size_t kInternalBlockHeaderSize = 8;   // btype+cLevel+cEnt + lcbTotal/dwPadding
constexpr size_t kSlEntrySize             = 24; // SLENTRY (Unicode)
constexpr size_t kSiEntrySize             = 16; // SIENTRY (Unicode)
constexpr size_t kXBlockEntrySize         = 8;  // BID per entry

// Maximum number of entries that fit in a single internal block, given
// the 64-byte-aligned, 16-byte-trailer rule:
//   max_body_bytes = ceil((header + entries + 16) / 64) * 64 - 16
// We don't expose a single "max entries" const because real Outlook
// allows internal blocks to grow past 8 KB (unlike data blocks). The
// test suite picks small values; the API itself accepts any count.

// Round a logical-body size up to the smallest 64-byte multiple that
// also fits the 16-byte trailer.
constexpr size_t roundBlockSize(size_t body) noexcept
{
    const size_t total = body + kBlockTrailerSize;
    const size_t aligned = (total + (kBlockAlignment - 1)) & ~(kBlockAlignment - 1);
    return aligned;
}

// ---------------------------------------------------------------------------
// SLENTRY value type ([MS-PST] §2.2.2.8.3.3.1.1) — 24 bytes on disk.
// nid is stored zero-extended to 8 bytes.
// ---------------------------------------------------------------------------
struct SlEntry {
    Nid nid     {};
    Bid bidData {};
    Bid bidSub  {}; // 0 if no nested subnode block

    constexpr SlEntry() noexcept = default;
    constexpr SlEntry(Nid n, Bid d, Bid s) noexcept
        : nid(n), bidData(d), bidSub(s) {}
};

// SIENTRY value type ([MS-PST] §2.2.2.8.3.3.2.1) — 16 bytes on disk.
struct SiEntry {
    Nid nid {};
    Bid bid {}; // BID of the SLBLOCK this SI entry indexes

    constexpr SiEntry() noexcept = default;
    constexpr SiEntry(Nid n, Bid b) noexcept : nid(n), bid(b) {}
};

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

// Encrypts `[payload, payload + cbPayload)` with the chosen method (using
// the lower 32 bits of `bid.value` as the cyclic key), appends a
// BLOCKTRAILER, and 64-byte-pads. Returns the on-disk block bytes.
//
// Pre: bid.isData() == true.
vector<uint8_t> buildDataBlock(const uint8_t* payload,
                               size_t         cbPayload,
                               Bid            bid,
                               Ib             ib,
                               CryptMethod    method) noexcept;

// XBLOCK (cLevel=1). `lcbTotal` is the sum of cb's of every data block
// that this XBLOCK indirects to. Caller is responsible for that sum.
//
// Pre: bid.isInternal() == true, count >= 1.
vector<uint8_t> buildXBlock(const Bid* childBids,
                            size_t     count,
                            uint32_t   lcbTotal,
                            Bid        bid,
                            Ib         ib) noexcept;

// XXBLOCK (cLevel=2) — indirects to XBLOCKs. Same byte shape as XBLOCK
// except the cLevel byte; the spec's distinction is purely about what
// the BIDs point to. We expose two builders to make caller intent
// explicit and to keep cLevel out of band.
vector<uint8_t> buildXXBlock(const Bid* childBids,
                             size_t     count,
                             uint32_t   lcbTotal,
                             Bid        bid,
                             Ib         ib) noexcept;

// SLBLOCK (cLevel=0).
vector<uint8_t> buildSlBlock(const SlEntry* entries,
                             size_t         count,
                             Bid            bid,
                             Ib             ib) noexcept;

// SIBLOCK (cLevel=1).
vector<uint8_t> buildSiBlock(const SiEntry* entries,
                             size_t         count,
                             Bid            bid,
                             Ib             ib) noexcept;

// ---------------------------------------------------------------------------
// Reader-side helper used by tests and pst_info to verify a block's
// trailer matches what we wrote. NOT for round-tripping data; just for
// integrity.
// ---------------------------------------------------------------------------
struct BlockTrailerView {
    uint16_t cb;
    uint16_t wSig;
    uint32_t dwCRC;
    uint64_t bid;
};

// Read the 16-byte BLOCKTRAILER at the end of `[block, block + total)`.
// `total` must be a multiple of 64 and >= 16.
BlockTrailerView readBlockTrailer(const uint8_t* block, size_t total) noexcept;

} // namespace pstwriter
