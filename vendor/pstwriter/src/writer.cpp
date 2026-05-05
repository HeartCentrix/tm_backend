// pstwriter/writer.cpp
//
// M2: writeEmptyPst — 5-page empty PST skeleton.
// M3: writeBlocksPst / writeXBlockPst — multi-block PSTs.

#include "writer.hpp"

#include "block.hpp"
#include "nbt.hpp"
#include "ndb.hpp"
#include "page.hpp"
#include "types.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using namespace std;

namespace pstwriter {

namespace {

// File offsets of the post-header pages in the M2 skeleton.
//
// Per [MS-PST] §2.6.1.1 + §3.2 sample, the FIRST AMap of a Unicode 2.0
// PST sits at file offset 0x4400 — NOT 0x400 — leaving a 0x4000-byte
// region after the HEADER for fixed-allocation content (zero-pad in
// our minimal writer; real Outlook puts a DList there). Bytes
// [0, kIbAMap) are not tracked by any AMap; they are permanent
// HEADER region. AMap[0]'s bitmap covers [kIbAMap, kIbAMap + 0x3E000).
//
// Cross-check from the §3.2 sample: ROOT.ibAMapLast = 0x9B4400, so
// `(0x9B4400 - 0x4400) / kAMapCoverage = exactly 40` — first AMap at
// 0x4400, 41 AMaps total, AMap[40] at 0x9B4400. M11-G corrected this
// from the long-standing wrong value of 0x400 (which made real
// Outlook reject the file with `Read(@4400) ... ptype=84 expected`).
constexpr uint64_t kIbAMap = 0x4400;
constexpr uint64_t kIbNbt  = 0x4600;
constexpr uint64_t kIbBbt  = 0x4800;
constexpr uint64_t kIbEof  = 0x4A00;

constexpr size_t kHeaderPadBytes = kIbAMap - kHeaderSize; // 0x4400 - 0x234 = 16844

// Helper: write `n` bytes from `data` to `fp`. Returns true on full success.
bool writeAll(FILE* fp, const void* data, size_t n) noexcept
{
    return fwrite(data, 1, n, fp) == n;
}

WriteResult fail(const char* what) noexcept
{
    WriteResult r;
    r.ok      = false;
    r.message = what;
    return r;
}

uint64_t alignUp(uint64_t v, uint64_t a) noexcept
{
    return (v + (a - 1)) & ~(a - 1);
}

// Free bytes reported by AMap[0]'s bitmap. Coverage range is
// [kIbAMap, kIbAMap + kAMapCoverage); bytes within range that are
// below `fileEof` are marked allocated, the rest free. Bytes
// [0, kIbAMap) are HEADER region — NOT tracked by any AMap.
//
// For multi-AMap files (fileEof > kIbAMap + kAMapCoverage), this
// helper only describes AMap[0] — additional AMaps are emitted at
// (kIbAMap + N × kAMapCoverage) and each carries its own
// cbAMapFree contribution.
uint64_t cbAMapFreeFor(uint64_t fileEof) noexcept
{
    const uint64_t coverageEnd  = kIbAMap + kAMapCoverage;
    const uint64_t allocatedEnd = fileEof < coverageEnd ? fileEof : coverageEnd;
    const uint64_t allocatedBytes =
        allocatedEnd > kIbAMap ? (allocatedEnd - kIbAMap) : 0;
    const uint64_t allocatedBits =
        (allocatedBytes + kBytesPerAMapBit - 1) / kBytesPerAMapBit;
    const uint64_t totalBits = static_cast<uint64_t>(kAMapBitmapBytes) * 8ull;
    return (totalBits - allocatedBits) * kBytesPerAMapBit;
}

FILE* openWb(const string& path) noexcept
{
#if defined(_MSC_VER)
    FILE* fp = nullptr;
    if (fopen_s(&fp, path.c_str(), "wb") != 0) return nullptr;
    return fp;
#else
    return std::fopen(path.c_str(), "wb");
#endif
}

} // anonymous namespace

// ===========================================================================
// M2: empty PST
// ===========================================================================
WriteResult writeEmptyPst(const string& path) noexcept
{
    // AMap occupies internal bid index 1 in the M2 reservation table, but
    // its PAGETRAILER.bid is set from its ib by buildAMap (M11-E).
    const Bid bidNbt  = Bid::makeInternal(2ull); // 0x0B
    const Bid bidBbt  = Bid::makeInternal(3ull); // 0x0F
    const Bid bidNextP{Bid::makeInternal(4ull)}; // 0x13
    const Bid bidNextB{Bid::makeData(1ull)};     // 0x04

    WriterState state{};
    state.dwUnique          = 1u;
    state.bidNextP          = bidNextP;
    state.bidNextB          = bidNextB;
    state.root.ibFileEof    = Ib{kIbEof};
    state.root.ibAMapLast   = Ib{kIbAMap};
    state.root.fAMapValid   = kAMapValid2;
    state.root.cbPMapFree   = 0ull;
    state.root.cbAMapFree   = cbAMapFreeFor(kIbEof);
    state.root.brefNbt      = Bref{bidNbt, Ib{kIbNbt}};
    state.root.brefBbt      = Bref{bidBbt, Ib{kIbBbt}};
    populateFreshRgnid(state);

    const auto header = serializeHeader(state);
    const auto amap   = buildAMap(Ib{kIbAMap}, kIbEof);
    const auto nbt    = buildEmptyNbtLeaf(bidNbt, Ib{kIbNbt});
    const auto bbt    = buildEmptyBbtLeaf(bidBbt, Ib{kIbBbt});

    FILE* fp = openWb(path);
    if (fp == nullptr) return fail("fopen failed");

    bool ok = true;
    ok = ok && writeAll(fp, header.data(), header.size());
    {
        const vector<uint8_t> pad(kHeaderPadBytes, uint8_t{0});
        ok = ok && writeAll(fp, pad.data(), pad.size());
    }
    ok = ok && writeAll(fp, amap.data(), amap.size());
    ok = ok && writeAll(fp, nbt.data(),  nbt.size());
    ok = ok && writeAll(fp, bbt.data(),  bbt.size());

    if (std::fclose(fp) != 0) return fail("fclose failed");
    if (!ok) return fail("fwrite truncated");
    return WriteResult{};
}

// ===========================================================================
// M3: multi-block PST  (writeBlocksPst / writeXBlockPst)
// ===========================================================================
namespace {

// Internal multi-block layout, post-AMap (M11-G: AMap moved to 0x4400):
//   blocksStartIb = kIbAMap + kPageSize  (= 0x4600)
//   for each block i:
//       offsets[i] = current; current += sizeOnDisk(block i)
//   bbtLeavesIb = alignUp(current, kPageSize)
//   {1 or more BBT leaves of 20 entries}
//   bbtRootIb   = bbtLeavesIb + n_leaves * kPageSize  (or = lone leaf)
//   nbtLeafIb   = (after BBT)
//   ibFileEof   = nbtLeafIb + kPageSize
constexpr uint64_t kBlocksStart = kIbAMap + kPageSize;   // 0x4600
struct Layout {
    uint64_t blocksStartIb = kBlocksStart;
    vector<uint64_t> blockOffsets;        // ib of block i
    vector<size_t>   blockOnDiskSizes;    // total bytes of block i (multiple of 64)
    uint64_t blocksEndIb = kBlocksStart;  // exclusive

    vector<uint64_t> bbtLeafIbs;          // page-aligned offsets of BBT leaves
    uint64_t         bbtRootIb = 0;       // == bbtLeafIbs[0] if single leaf, else intermediate page
    uint64_t         nbtLeafIb = 0;
    uint64_t         ibFileEof = 0;
};

WriteResult buildAndWriteBlocksPst(const string&                    path,
                                   const vector<vector<uint8_t>>&   payloads,
                                   const vector<Bid>&               blockBids,
                                   const vector<vector<uint8_t>>&   blockBuffers,
                                   /*lifetime: writer pre-built blocks already, encoded.*/
                                   /*the M3 helpers below populate these.*/
                                   /*nbt:*/ /*empty in M3*/ Bid bidNbtLeaf,
                                   Bid bidBbtRoot,
                                   const vector<Bid>& bidBbtLeaves)
{
    // 1. Compute layout: pack blocks at 64-byte alignment from kBlocksStart
    //    (0x4600 — the byte right after AMap[0] at kIbAMap=0x4400).
    Layout L;
    L.blocksStartIb = kBlocksStart;

    L.blockOffsets.reserve(blockBuffers.size());
    L.blockOnDiskSizes.reserve(blockBuffers.size());
    uint64_t cursor = L.blocksStartIb;
    for (const auto& blk : blockBuffers) {
        L.blockOffsets.push_back(cursor);
        L.blockOnDiskSizes.push_back(blk.size());
        cursor += blk.size();
        // 64-byte alignment is already guaranteed by buildDataBlock /
        // buildXBlock etc. (each returns a buffer whose size is a
        // multiple of 64), so no explicit padding here.
    }
    L.blocksEndIb = cursor;

    // 2. Page-align cursor to the next 0x200 boundary for BBT leaves.
    cursor = alignUp(cursor, kPageSize);

    // 3. Reserve BBT leaves and (optionally) a BBT intermediate root.
    L.bbtLeafIbs.reserve(bidBbtLeaves.size());
    for (size_t i = 0; i < bidBbtLeaves.size(); ++i) {
        L.bbtLeafIbs.push_back(cursor);
        cursor += kPageSize;
    }
    if (bidBbtLeaves.size() == 1) {
        L.bbtRootIb = L.bbtLeafIbs.front();
    } else {
        L.bbtRootIb = cursor;
        cursor += kPageSize;
    }

    // 4. NBT leaf (empty for M3) — comes right after BBT.
    L.nbtLeafIb = cursor;
    cursor += kPageSize;

    L.ibFileEof = cursor;

    // 5. Build BBTENTRY records sorted by BID ascending.
    vector<BbtEntry> bbtEntries;
    bbtEntries.reserve(blockBids.size());
    for (size_t i = 0; i < blockBids.size(); ++i) {
        BbtEntry e;
        e.bref = Bref{blockBids[i], Ib{L.blockOffsets[i]}};
        // cb in BBTENTRY equals the BLOCKTRAILER cb for the same block,
        // which is the pre-encryption payload size for data blocks and
        // the structured body size for internal blocks.  Reading it
        // back from the on-disk trailer keeps the two in sync without
        // duplicate bookkeeping.
        const auto t = readBlockTrailer(blockBuffers[i].data(),
                                        blockBuffers[i].size());
        e.cb   = t.cb;
        e.cRef = 1u;
        bbtEntries.push_back(e);
    }
    sort(bbtEntries.begin(), bbtEntries.end(),
         [](const BbtEntry& a, const BbtEntry& b) {
             return a.bref.bid.value < b.bref.bid.value;
         });

    // 6. Distribute entries across leaf pages (up to 20 per leaf).
    vector<vector<BbtEntry>> perLeaf;
    perLeaf.resize(bidBbtLeaves.size());
    for (size_t i = 0; i < bbtEntries.size(); ++i) {
        const size_t leafIdx = i / kBbtMaxEntriesPerLeaf;
        perLeaf[leafIdx].push_back(bbtEntries[i]);
    }

    // 7. Build the actual BBT pages.
    vector<array<uint8_t, kPageSize>> bbtLeafPages;
    bbtLeafPages.reserve(bidBbtLeaves.size());
    vector<BtEntry> intermediateEntries;
    intermediateEntries.reserve(bidBbtLeaves.size());

    for (size_t i = 0; i < bidBbtLeaves.size(); ++i) {
        bbtLeafPages.push_back(buildBbtLeaf(perLeaf[i].data(),
                                            perLeaf[i].size(),
                                            bidBbtLeaves[i],
                                            Ib{L.bbtLeafIbs[i]}));
        if (!perLeaf[i].empty()) {
            // btkey of an intermediate-page entry is the BID of the
            // first BBTENTRY in the child leaf.
            const uint64_t btkey = perLeaf[i].front().bref.bid.value;
            intermediateEntries.emplace_back(btkey,
                                             Bref{bidBbtLeaves[i],
                                                  Ib{L.bbtLeafIbs[i]}});
        }
    }

    // The BBT root: either the single leaf (count == 1) or an
    // intermediate page (cLevel = 1) that addresses the leaves.
    array<uint8_t, kPageSize> bbtRootPage{};
    Bid bidBbtRootForRoot = bidBbtRoot;
    if (bidBbtLeaves.size() == 1) {
        bbtRootPage = bbtLeafPages.front();
        bidBbtRootForRoot = bidBbtLeaves.front();
    } else {
        bbtRootPage = buildBbtIntermediate(intermediateEntries.data(),
                                           intermediateEntries.size(),
                                           /*cLevel=*/1u,
                                           bidBbtRoot,
                                           Ib{L.bbtRootIb});
    }

    // NBT root: empty leaf, M3 doesn't register nodes.
    const auto nbtPage = buildEmptyNbtLeaf(bidNbtLeaf, Ib{L.nbtLeafIb});

    // 8. AMap[0]: covers [kIbAMap, kIbAMap + kAMapCoverage). Bits for
    // bytes within [kIbAMap, ibFileEof) are set; rest free. The 16 KB
    // [0x400, 0x4400) HEADER region is NOT tracked by any AMap (M11-G).
    // PAGETRAILER.bid set from ib by buildAMap (M11-E).
    const auto amap   = buildAMap(Ib{kIbAMap}, L.ibFileEof);

    // 9. HEADER.
    WriterState state{};
    state.dwUnique         = 1u;
    state.bidNextP         = Bid::makeInternal(0x10ull);     // arbitrary > all used page BIDs
    state.bidNextB         = Bid::makeData(blockBids.size() + 1ull);
    state.root.ibFileEof   = Ib{L.ibFileEof};
    state.root.ibAMapLast  = Ib{kIbAMap};
    state.root.cbAMapFree  = cbAMapFreeFor(L.ibFileEof);
    state.root.cbPMapFree  = 0ull;
    state.root.fAMapValid  = kAMapValid2;
    state.root.brefNbt     = Bref{bidNbtLeaf, Ib{L.nbtLeafIb}};
    state.root.brefBbt     = Bref{bidBbtRootForRoot, Ib{L.bbtRootIb}};
    populateFreshRgnid(state);

    const auto header = serializeHeader(state);

    // 10. Write file in order. Pad between sections as needed.
    FILE* fp = openWb(path);
    if (fp == nullptr) return fail("fopen failed");

    bool ok = true;
    uint64_t writtenIb = 0;

    auto padTo = [&](uint64_t targetIb) {
        if (targetIb > writtenIb) {
            const size_t n = static_cast<size_t>(targetIb - writtenIb);
            const vector<uint8_t> pad(n, uint8_t{0});
            ok = ok && writeAll(fp, pad.data(), pad.size());
            writtenIb = targetIb;
        }
    };
    auto emit = [&](const void* data, size_t n) {
        ok = ok && writeAll(fp, data, n);
        writtenIb += n;
    };

    emit(header.data(), header.size());          // 0x000..0x233
    padTo(kIbAMap);                              // 0x234..0x3FF
    emit(amap.data(), amap.size());              // 0x400..0x5FF
    padTo(L.blocksStartIb);                      // (already 0x600 if no blocks-prefix gap)

    for (const auto& blk : blockBuffers) {
        emit(blk.data(), blk.size());
    }

    padTo(L.bbtLeafIbs.empty() ? L.bbtRootIb : L.bbtLeafIbs.front());
    for (size_t i = 0; i < bbtLeafPages.size(); ++i) {
        // If we used the single-leaf shortcut, the leaf IS the root and
        // we don't write it twice.
        if (bidBbtLeaves.size() == 1) {
            // root will be written via bbtRootPage below.
            break;
        }
        emit(bbtLeafPages[i].data(), bbtLeafPages[i].size());
    }
    padTo(L.bbtRootIb);
    emit(bbtRootPage.data(), bbtRootPage.size());

    padTo(L.nbtLeafIb);
    emit(nbtPage.data(), nbtPage.size());

    if (std::fclose(fp) != 0) return fail("fclose failed");
    if (!ok)                  return fail("fwrite truncated");

    // Suppress unused-parameter warnings for parameters retained for
    // future M5 expansion.
    (void)payloads;
    return WriteResult{};
}

} // anonymous namespace

WriteResult writeBlocksPst(const string&                  path,
                           const vector<vector<uint8_t>>& dataBlocks) noexcept
{
    if (dataBlocks.size() > kBbtMaxEntriesPerLeaf * kBtMaxEntriesPerPage) {
        return fail("M3 BBT supports at most 400 blocks (1 intermediate level)");
    }

    // Allocate sequential data BIDs and build encoded blocks at to-be-
    // determined offsets.  Block file offsets are filled in by
    // buildAndWriteBlocksPst — but we need the BLOCKTRAILER's `ib`
    // (used in wSig) at construction time. So: pack first, build
    // second.
    vector<Bid> blockBids;
    blockBids.reserve(dataBlocks.size());
    for (size_t i = 0; i < dataBlocks.size(); ++i) {
        blockBids.push_back(Bid::makeData(i + 1ull));
    }

    // First pass: provisional offsets at 64-byte alignment, starting
    // from kBlocksStart (= 0x4600 — right after AMap[0]).
    uint64_t cursor = kBlocksStart;
    vector<uint64_t> ibs;
    ibs.reserve(dataBlocks.size());
    for (const auto& p : dataBlocks) {
        ibs.push_back(cursor);
        cursor += roundBlockSize(p.size());
    }

    // Second pass: build encoded blocks.
    vector<vector<uint8_t>> built;
    built.reserve(dataBlocks.size());
    for (size_t i = 0; i < dataBlocks.size(); ++i) {
        built.push_back(buildDataBlock(dataBlocks[i].data(),
                                       dataBlocks[i].size(),
                                       blockBids[i],
                                       Ib{ibs[i]},
                                       CryptMethod::Permute));
    }

    // BBT plumbing: leaf count, IDs.
    const size_t nLeaves =
        (dataBlocks.size() == 0)
            ? 1u
            : (dataBlocks.size() + kBbtMaxEntriesPerLeaf - 1) / kBbtMaxEntriesPerLeaf;

    vector<Bid> bidBbtLeaves;
    bidBbtLeaves.reserve(nLeaves);
    Bid nextPageBid = Bid::makeInternal(2ull); // 0x0B onwards (AMap = 0x07)
    for (size_t i = 0; i < nLeaves; ++i) {
        bidBbtLeaves.push_back(nextPageBid);
        nextPageBid = Bid{nextPageBid.value + 4ull};
    }
    const Bid bidBbtRoot = (nLeaves == 1) ? bidBbtLeaves.front() : nextPageBid;
    if (nLeaves > 1) nextPageBid = Bid{nextPageBid.value + 4ull};
    const Bid bidNbtLeaf = nextPageBid;

    return buildAndWriteBlocksPst(path,
                                  dataBlocks,
                                  blockBids,
                                  built,
                                  bidNbtLeaf,
                                  bidBbtRoot,
                                  bidBbtLeaves);
}

// ---------------------------------------------------------------------------
// writeXBlockPst — split `payload` into `chunkSize`-byte data blocks and
// indirect them via one XBLOCK.
// ---------------------------------------------------------------------------
WriteResult writeXBlockPst(const string&  path,
                           const uint8_t* payload,
                           size_t         cbPayload,
                           size_t         chunkSize) noexcept
{
    if (chunkSize == 0)              return fail("chunkSize must be > 0");
    if (chunkSize > kMaxBlockPayload) return fail("chunkSize too large");
    if (cbPayload == 0)               return fail("XBLOCK requires a non-empty payload");

    const size_t nDataBlocks = (cbPayload + chunkSize - 1) / chunkSize;
    if (nDataBlocks > 1019) return fail("XBLOCK can address at most ~1019 children");

    // Allocate BIDs:  data blocks 1..N (data flag), XBLOCK = N+1 (internal).
    vector<Bid> dataBids;
    dataBids.reserve(nDataBlocks);
    for (size_t i = 0; i < nDataBlocks; ++i) {
        dataBids.push_back(Bid::makeData(i + 1ull));
    }
    const Bid xblockBid = Bid::makeInternal(nDataBlocks + 1ull);

    vector<Bid>  blockBids;       blockBids.reserve(nDataBlocks + 1);
    vector<Ib>   blockIbs;        blockIbs.reserve(nDataBlocks + 1);

    // Provisional offsets, starting from kBlocksStart (= 0x4600 —
    // right after AMap[0] at kIbAMap=0x4400, M11-G).
    uint64_t cursor = kBlocksStart;
    vector<uint64_t> ibs;
    ibs.reserve(nDataBlocks + 1);
    for (size_t i = 0; i < nDataBlocks; ++i) {
        ibs.push_back(cursor);
        const size_t cbThis = (i + 1 == nDataBlocks)
            ? (cbPayload - i * chunkSize)
            : chunkSize;
        cursor += roundBlockSize(cbThis);
    }
    const uint64_t xblockIb = cursor;
    ibs.push_back(xblockIb);

    // Build the data blocks.
    vector<vector<uint8_t>> built;
    built.reserve(nDataBlocks + 1);
    for (size_t i = 0; i < nDataBlocks; ++i) {
        const size_t cbThis = (i + 1 == nDataBlocks)
            ? (cbPayload - i * chunkSize)
            : chunkSize;
        built.push_back(buildDataBlock(payload + i * chunkSize,
                                       cbThis,
                                       dataBids[i],
                                       Ib{ibs[i]},
                                       CryptMethod::Permute));
        blockBids.push_back(dataBids[i]);
    }

    // Build the XBLOCK referencing all data blocks.
    built.push_back(buildXBlock(dataBids.data(),
                                dataBids.size(),
                                static_cast<uint32_t>(cbPayload),
                                xblockBid,
                                Ib{xblockIb}));
    blockBids.push_back(xblockBid);

    // BBT plumbing.
    const size_t totalBlocks = nDataBlocks + 1;
    const size_t nLeaves =
        (totalBlocks + kBbtMaxEntriesPerLeaf - 1) / kBbtMaxEntriesPerLeaf;

    vector<Bid> bidBbtLeaves;
    bidBbtLeaves.reserve(nLeaves);
    Bid nextPageBid = Bid::makeInternal(0x100ull); // far above block BIDs
    for (size_t i = 0; i < nLeaves; ++i) {
        bidBbtLeaves.push_back(nextPageBid);
        nextPageBid = Bid{nextPageBid.value + 4ull};
    }
    const Bid bidBbtRoot = (nLeaves == 1) ? bidBbtLeaves.front() : nextPageBid;
    if (nLeaves > 1) nextPageBid = Bid{nextPageBid.value + 4ull};
    const Bid bidNbtLeaf = nextPageBid;

    return buildAndWriteBlocksPst(path,
                                  /*payloads=*/{},
                                  blockBids,
                                  built,
                                  bidNbtLeaf,
                                  bidBbtRoot,
                                  bidBbtLeaves);
}

// ===========================================================================
// M5 Phase D: writeM5Pst -- end-to-end PST with NBT entries (no orphan
// blocks). See writer.hpp for the layout/contract.
// ===========================================================================
WriteResult writeM5Pst(const string&                  path,
                       const vector<M5DataBlockSpec>& blocks,
                       const vector<M5Node>&          nodes) noexcept
{
    if (blocks.size() > kBbtMaxEntriesPerLeaf * kBtMaxEntriesPerPage) {
        return fail("M5: BBT supports at most 400 blocks (1 intermediate level)");
    }
    if (nodes.size() > kNbtLeafMaxEntries * kBtIntermediateMaxEntries) {
        return fail("M5: NBT supports at most 300 nodes (1 intermediate level)");
    }

    // ---- 1. Compute layout ------------------------------------------------
    // Blocks live immediately after AMap[0] (kIbAMap=0x4400 + 0x200).
    // The 16 KB region [0x400, 0x4400) sits between HEADER and AMap[0]
    // and is filled with zero bytes by the file-write path below;
    // those bytes are HEADER region per [MS-PST] §2.6.1.1 (M11-G) and
    // are not tracked by AMap[0]'s bitmap.
    uint64_t cursor = kBlocksStart;
    vector<uint64_t> blockIbs;
    blockIbs.reserve(blocks.size());
    for (const auto& b : blocks) {
        blockIbs.push_back(cursor);
        cursor += b.encodedBlock.size();
    }
    cursor = alignUp(cursor, kPageSize);

    // BBT plumbing
    const size_t nBbtLeaves =
        blocks.empty()
            ? 1u
            : (blocks.size() + kBbtMaxEntriesPerLeaf - 1) / kBbtMaxEntriesPerLeaf;
    vector<uint64_t> bbtLeafIbs;
    bbtLeafIbs.reserve(nBbtLeaves);
    for (size_t i = 0; i < nBbtLeaves; ++i) {
        bbtLeafIbs.push_back(cursor);
        cursor += kPageSize;
    }
    const bool   hasBbtIntermediate = (nBbtLeaves > 1);
    const uint64_t bbtRootIb        = hasBbtIntermediate ? cursor : bbtLeafIbs.front();
    if (hasBbtIntermediate) cursor += kPageSize;

    // NBT plumbing
    const size_t nNbtLeaves =
        nodes.empty()
            ? 1u
            : (nodes.size() + kNbtLeafMaxEntries - 1) / kNbtLeafMaxEntries;
    vector<uint64_t> nbtLeafIbs;
    nbtLeafIbs.reserve(nNbtLeaves);
    for (size_t i = 0; i < nNbtLeaves; ++i) {
        nbtLeafIbs.push_back(cursor);
        cursor += kPageSize;
    }
    const bool   hasNbtIntermediate = (nNbtLeaves > 1);
    const uint64_t nbtRootIb        = hasNbtIntermediate ? cursor : nbtLeafIbs.front();
    if (hasNbtIntermediate) cursor += kPageSize;

    const uint64_t ibFileEof = cursor;

    // ---- 2. Allocate page BIDs (internal) --------------------------------
    // Bid::makeInternal(1) = 0x07 is the AMap. Continue from 2.
    Bid nextPageBid = Bid::makeInternal(2ull); // 0x0B
    vector<Bid> bbtLeafBids;
    bbtLeafBids.reserve(nBbtLeaves);
    for (size_t i = 0; i < nBbtLeaves; ++i) {
        bbtLeafBids.push_back(nextPageBid);
        nextPageBid = Bid{nextPageBid.value + 4ull};
    }
    const Bid bbtRootBid = hasBbtIntermediate ? nextPageBid : bbtLeafBids.front();
    if (hasBbtIntermediate) nextPageBid = Bid{nextPageBid.value + 4ull};

    vector<Bid> nbtLeafBids;
    nbtLeafBids.reserve(nNbtLeaves);
    for (size_t i = 0; i < nNbtLeaves; ++i) {
        nbtLeafBids.push_back(nextPageBid);
        nextPageBid = Bid{nextPageBid.value + 4ull};
    }
    const Bid nbtRootBid = hasNbtIntermediate ? nextPageBid : nbtLeafBids.front();
    if (hasNbtIntermediate) nextPageBid = Bid{nextPageBid.value + 4ull};

    // ---- 3. Build BBT pages ----------------------------------------------
    // BBTENTRYs sorted by BID ascending; partitioned across leaves.
    vector<BbtEntry> bbtEntries;
    bbtEntries.reserve(blocks.size());
    for (size_t i = 0; i < blocks.size(); ++i) {
        BbtEntry e;
        e.bref = Bref{blocks[i].bid, Ib{blockIbs[i]}};
        e.cb   = blocks[i].cb;
        e.cRef = 1u;
        bbtEntries.push_back(e);
    }
    std::sort(bbtEntries.begin(), bbtEntries.end(),
              [](const BbtEntry& a, const BbtEntry& b) {
                  return a.bref.bid.value < b.bref.bid.value;
              });

    vector<vector<BbtEntry>> bbtPerLeaf(nBbtLeaves);
    for (size_t i = 0; i < bbtEntries.size(); ++i) {
        bbtPerLeaf[i / kBbtMaxEntriesPerLeaf].push_back(bbtEntries[i]);
    }
    vector<array<uint8_t, kPageSize>> bbtLeafPages;
    bbtLeafPages.reserve(nBbtLeaves);
    vector<BtEntry> bbtIntermediate;
    bbtIntermediate.reserve(nBbtLeaves);
    for (size_t i = 0; i < nBbtLeaves; ++i) {
        bbtLeafPages.push_back(buildBbtLeaf(
            bbtPerLeaf[i].data(), bbtPerLeaf[i].size(),
            bbtLeafBids[i], Ib{bbtLeafIbs[i]}));
        if (!bbtPerLeaf[i].empty()) {
            const uint64_t btkey = bbtPerLeaf[i].front().bref.bid.value;
            bbtIntermediate.emplace_back(btkey,
                Bref{bbtLeafBids[i], Ib{bbtLeafIbs[i]}});
        }
    }
    array<uint8_t, kPageSize> bbtRootPage{};
    if (hasBbtIntermediate) {
        bbtRootPage = buildBbtIntermediate(
            bbtIntermediate.data(), bbtIntermediate.size(),
            /*cLevel*/ 1u, bbtRootBid, Ib{bbtRootIb});
    } else {
        bbtRootPage = bbtLeafPages.front();
    }

    // ---- 4. Build NBT pages ----------------------------------------------
    vector<NbtEntry> nbtEntries;
    nbtEntries.reserve(nodes.size());
    for (const auto& n : nodes) {
        nbtEntries.emplace_back(n.nid, n.bidData, n.bidSub, n.nidParent);
    }
    std::sort(nbtEntries.begin(), nbtEntries.end(),
              [](const NbtEntry& a, const NbtEntry& b) {
                  return a.nid.value < b.nid.value;
              });

    vector<vector<NbtEntry>> nbtPerLeaf(nNbtLeaves);
    for (size_t i = 0; i < nbtEntries.size(); ++i) {
        nbtPerLeaf[i / kNbtLeafMaxEntries].push_back(nbtEntries[i]);
    }
    vector<array<uint8_t, kPageSize>> nbtLeafPages;
    nbtLeafPages.reserve(nNbtLeaves);
    vector<BtEntry> nbtIntermediate;
    nbtIntermediate.reserve(nNbtLeaves);
    for (size_t i = 0; i < nNbtLeaves; ++i) {
        nbtLeafPages.push_back(buildNbtLeaf(
            nbtPerLeaf[i].data(), nbtPerLeaf[i].size(),
            nbtLeafBids[i], Ib{nbtLeafIbs[i]}));
        if (!nbtPerLeaf[i].empty()) {
            const uint64_t btkey =
                static_cast<uint64_t>(nbtPerLeaf[i].front().nid.value);
            nbtIntermediate.emplace_back(btkey,
                Bref{nbtLeafBids[i], Ib{nbtLeafIbs[i]}});
        }
    }
    array<uint8_t, kPageSize> nbtRootPage{};
    if (hasNbtIntermediate) {
        nbtRootPage = buildBtIntermediate(
            nbtIntermediate.data(), nbtIntermediate.size(),
            /*cLevel*/ 1u, ptype::kNBT, nbtRootBid, Ib{nbtRootIb});
    } else {
        nbtRootPage = nbtLeafPages.front();
    }

    // ---- 5. AMap + HEADER ------------------------------------------------
    // PAGETRAILER.bid set from ib by buildAMap (M11-E).
    const auto amap   = buildAMap(Ib{kIbAMap}, ibFileEof);

    WriterState state{};
    state.dwUnique         = 1u;
    state.bidNextP         = nextPageBid;
    state.bidNextB         = Bid::makeData(blocks.size() + 1ull);
    state.root.ibFileEof   = Ib{ibFileEof};
    state.root.ibAMapLast  = Ib{kIbAMap};
    state.root.cbAMapFree  = cbAMapFreeFor(ibFileEof);
    state.root.cbPMapFree  = 0ull;
    state.root.fAMapValid  = kAMapValid2;
    state.root.brefNbt     = Bref{nbtRootBid, Ib{nbtRootIb}};
    state.root.brefBbt     = Bref{bbtRootBid, Ib{bbtRootIb}};
    populateFreshRgnid(state);

    const auto header = serializeHeader(state);

    // ---- 6. Write file ---------------------------------------------------
    FILE* fp = openWb(path);
    if (fp == nullptr) return fail("fopen failed");

    bool ok = true;
    uint64_t writtenIb = 0;
    auto padTo = [&](uint64_t targetIb) {
        if (targetIb > writtenIb) {
            const size_t n = static_cast<size_t>(targetIb - writtenIb);
            const vector<uint8_t> pad(n, uint8_t{0});
            ok = ok && writeAll(fp, pad.data(), pad.size());
            writtenIb = targetIb;
        }
    };
    auto emit = [&](const void* data, size_t n) {
        ok = ok && writeAll(fp, data, n);
        writtenIb += n;
    };

    emit(header.data(), header.size());
    padTo(kIbAMap);
    emit(amap.data(), amap.size());
    padTo(kBlocksStart);
    for (const auto& b : blocks) {
        emit(b.encodedBlock.data(), b.encodedBlock.size());
    }
    // BBT leaves
    for (size_t i = 0; i < nBbtLeaves; ++i) {
        if (!hasBbtIntermediate) break; // single-leaf case writes via root below
        padTo(bbtLeafIbs[i]);
        emit(bbtLeafPages[i].data(), bbtLeafPages[i].size());
    }
    padTo(bbtRootIb);
    emit(bbtRootPage.data(), bbtRootPage.size());
    // NBT leaves
    for (size_t i = 0; i < nNbtLeaves; ++i) {
        if (!hasNbtIntermediate) break;
        padTo(nbtLeafIbs[i]);
        emit(nbtLeafPages[i].data(), nbtLeafPages[i].size());
    }
    padTo(nbtRootIb);
    emit(nbtRootPage.data(), nbtRootPage.size());

    if (std::fclose(fp) != 0) return fail("fclose failed");
    if (!ok)                  return fail("fwrite truncated");
    return WriteResult{};
}

} // namespace pstwriter
