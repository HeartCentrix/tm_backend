// pstwriter/page.cpp
//
// Page-level builders ([MS-PST] §2.2.2.7) — produce the full 512-byte page
// (body + PAGETRAILER) ready to fwrite to disk.
//
// Trailer construction (wSig, dwCRC, bid layout) is delegated to
// `writePageTrailer` in ndb.cpp; this file is only responsible for the
// page-body bytes and for handing the right BID/IB to the trailer writer.

#include "page.hpp"

#include "ndb.hpp"
#include "types.hpp"

#include <array>
#include <cstdint>
#include <cstddef>

using namespace std;

namespace pstwriter {

using detail::writeU8;
using detail::writeU16;
using detail::writeU32;
using detail::writeU64;

// ============================================================================
// AMap ([MS-PST] §2.2.2.7.2)
// ============================================================================
array<uint8_t, kPageSize> buildAMap(Ib ibAMap, uint64_t fileSize) noexcept
{
    array<uint8_t, kPageSize> page{};

    // Each bit covers 64 bytes IN THE COVERAGE RANGE [ibAMap, ibAMap +
    // kAMapCoverage). Bytes outside this AMap's range (e.g. the
    // [0, kIbAMap) HEADER region) are not tracked by this AMap.
    // Set bits 0..ceil((min(fileSize, ibAMap+coverage) - ibAMap)/64).
    // Bit 0 corresponds to bytes [ibAMap, ibAMap+64) — the AMap page
    // itself sits in bits 0..7 (512 bytes / 64) and is therefore
    // marked allocated, exactly what the spec requires. (M11-G.)
    const uint64_t coverageEnd  = ibAMap.value + kAMapCoverage;
    const uint64_t allocatedEnd = fileSize < coverageEnd ? fileSize : coverageEnd;
    const uint64_t allocatedBytes =
        allocatedEnd > ibAMap.value ? (allocatedEnd - ibAMap.value) : 0;
    const uint64_t totalBits =
        (allocatedBytes + kBytesPerAMapBit - 1) / kBytesPerAMapBit;
    const uint64_t fullBytes = totalBits / 8;
    const uint64_t leftover  = totalBits % 8;

    const uint64_t cap = (fullBytes < kAMapBitmapBytes) ? fullBytes : kAMapBitmapBytes;
    for (uint64_t i = 0; i < cap; ++i) {
        page[static_cast<size_t>(i)] = 0xFFu;
    }
    if (leftover != 0 && cap < kAMapBitmapBytes) {
        page[static_cast<size_t>(cap)] =
            static_cast<uint8_t>((1u << leftover) - 1u);
    }

    // PAGETRAILER.bid == page file offset (ib) per [MS-PST] §2.2.2.7.2 +
    // §2.6.1 — verified empirically against real Outlook open. See
    // KNOWN_UNVERIFIED.md M11-E.
    writePageTrailer(page, ptype::kAMap, Bid::makeAmap(ibAMap.value), ibAMap);
    return page;
}

// ============================================================================
// PMap page ([MS-PST] §2.2.2.7.3) at fixed offset 0x4600.
//
// PMap is deprecated for use (DList superseded it) but [MS-PST] §2.6 still
// requires the page to be physically present at 0x4600 + N×0x1F0000 with
// a valid PAGETRAILER (PTYPE=0x83, correct CRC).
//
// PMap inverse semantics: bit=1 means *free*, bit=0 means *allocated*.
// Real Outlook backup.pst files ship the PMap bitmap as ALL 0xFF (every
// page free) because PMap is unused at runtime — the bytes are vestigial.
// Earlier round-11 attempt zero-filled the bitmap (= "all allocated"),
// which was inconsistent with the AMap and triggered Outlook's
// auto-quarantine. 0xFF is the byte-for-byte match we see in real PSTs.
// ============================================================================
array<uint8_t, kPageSize> buildEmptyPMap(uint64_t ibPMap) noexcept
{
    array<uint8_t, kPageSize> page{};
    for (size_t i = 0; i < kPageBodySize; ++i) page[i] = 0xFFu;
    writePageTrailer(page, ptype::kPMap, Bid::makeAmap(ibPMap), Ib{ibPMap});
    return page;
}

// ============================================================================
// DList page ([MS-PST] §2.2.2.7.4) at fixed offset 0x4200.
//
// Real Outlook always emits a DLISTPAGE at 0x4200 (just before the first
// AMap at 0x4400). Without it, scanpst reads zero bytes there and
// surfaces "PMap page @17920 …" errors — its DList lookup directs it
// to validate offset 0x4600 as a PMap page, which in our writer is the
// first data block. Procmon trace confirmed the DList read at 0x4200
// happens before every PMap-error logging burst.
//
// DLISTPAGE body layout (496 bytes):
//   [0]      bFlags             — 0 (no backfill in progress)
//   [1]      cEntDList          — number of entries (1 for single-AMap PSTs)
//   [2..3]   wPadding           — 0
//   [4..7]   ulCurrentPage      — 0 (current AMap index)
//   [8..]    rgEntDList         — DLISTPAGEENT array (4 bytes each)
//   [...]    rgPadding          — zero-fill to 496
//
// DLISTPAGEENT (4 bytes, packed LE):
//   bits  0..19  dwPageNum   — AMap page index (0-based)
//   bits 20..31  dwFreeSlots — free 64-byte slots in that AMap
// ============================================================================
array<uint8_t, kPageSize> buildDListPage(uint64_t ibDList,
                                         uint64_t ibAMap,
                                         uint64_t fileEof) noexcept
{
    array<uint8_t, kPageSize> page{};

    // Free-slot count for AMap[0]. AMap[0] covers [ibAMap, ibAMap +
    // kAMapCoverage). Allocated bytes are everything from ibAMap up to
    // min(fileEof, coverageEnd). Free bytes are the rest of the
    // coverage. Slots are 64-byte units (kBytesPerAMapBit).
    const uint64_t coverageEnd  = ibAMap + kAMapCoverage;
    const uint64_t allocatedEnd = fileEof < coverageEnd ? fileEof : coverageEnd;
    const uint64_t allocatedBytes =
        allocatedEnd > ibAMap ? (allocatedEnd - ibAMap) : 0;
    const uint64_t freeBytes = (kAMapCoverage > allocatedBytes)
                                 ? (kAMapCoverage - allocatedBytes) : 0;
    const uint32_t freeSlots = static_cast<uint32_t>(freeBytes / kBytesPerAMapBit);

    // Round 8 emitted DList with cEntDList=1 (one entry for AMap[0])
    // and Outlook accepted it (file would scan-pst-repair through). Round
    // 12 changed cEntDList=0 to mirror real backup.pst's cEntDList=8
    // pattern, but that triggered Outlook auto-quarantine. Reverting to
    // the round-8 format — cEntDList=1 with the AMap[0] entry.
    page[0] = 0;        // bFlags
    page[1] = 1;        // cEntDList — one entry for AMap[0]
    // [2..3] wPadding — left zero
    writeU32(page.data(), 4, 0u);  // ulCurrentPage = 0 (AMap[0])

    // rgEntDList[0] — pack {dwFreeSlots(12) << 20 | dwPageNum(20)}.
    const uint32_t entry =
        (static_cast<uint32_t>(freeSlots & 0xFFFu) << 20)
      | (0u & 0xFFFFFu);  // dwPageNum = 0
    writeU32(page.data(), 8, entry);

    // PAGETRAILER. Bid for DList follows the same convention as AMap:
    // PAGETRAILER.bid == ib. Verified by an empirical Outlook open in
    // M11-E. ptype = 0x9C (kDList).
    writePageTrailer(page, ptype::kDList, Bid::makeAmap(ibDList), Ib{ibDList});
    return page;
}

// ============================================================================
// Empty BTPAGE leaves ([MS-PST] §2.2.2.7.7)
// ============================================================================
namespace {

array<uint8_t, kPageSize> buildEmptyBtLeaf(uint8_t pageType,
                                           size_t  cbEnt,
                                           Bid     bid,
                                           Ib      ib) noexcept
{
    array<uint8_t, kPageSize> page{};

    // BTPAGE layout per [MS-PST] §2.2.2.7.7.1 (SPEC_GROUND_TRUTH):
    //   [0   .. 487]  rgentries  — 488 bytes (zero for empty leaf)
    //   [488]         cEnt
    //   [489]         cEntMax
    //   [490]         cbEnt
    //   [491]         cLevel
    //   [492 .. 495]  dwPadding  — 4 bytes (KNOWN BUG #2: 4 not 8)
    //   [496 .. 511]  PAGETRAILER
    //
    // SPEC_BRIEF originally had control bytes at 492-495 with pad first;
    // that is wrong and would silently break Outlook's B-tree read.
    static_assert(kBtPagePadBytes == 4, "BTPAGE dwPadding is 4 bytes, not 8");
    static_assert(kBtPageDwPad   == 492, "BTPAGE dwPadding starts at offset 492");
    static_assert(kBtPageCEnt    == 488, "BTPAGE cEnt starts at offset 488");

    writeU8(page.data(), kBtPageCEnt,    0u);
    writeU8(page.data(), kBtPageCEntMax, static_cast<uint8_t>(kBtPageEntriesArea / cbEnt));
    writeU8(page.data(), kBtPageCbEnt,   static_cast<uint8_t>(cbEnt));
    writeU8(page.data(), kBtPageCLevel,  0u);
    writeU32(page.data(), kBtPageDwPad,  0u);

    writePageTrailer(page, pageType, bid, ib);
    return page;
}

} // anonymous namespace

array<uint8_t, kPageSize> buildEmptyNbtLeaf(Bid bid, Ib ib) noexcept
{
    return buildEmptyBtLeaf(ptype::kNBT, kNbtLeafEntrySize, bid, ib);
}

array<uint8_t, kPageSize> buildEmptyBbtLeaf(Bid bid, Ib ib) noexcept
{
    return buildEmptyBtLeaf(ptype::kBBT, kBbtLeafEntrySize, bid, ib);
}

// ============================================================================
// Filled BBT leaf ([MS-PST] §2.2.2.7.7.3)
//
//   BBTENTRY layout (24 bytes):
//     [0..15]  BREF (bid 8 + ib 8)
//     [16..17] cb        (pre-encryption payload size)
//     [18..19] cRef      (reference count)
//     [20..23] dwPadding (0)
// ============================================================================
array<uint8_t, kPageSize> buildBbtLeaf(const BbtEntry* entries,
                                       size_t          count,
                                       Bid             bid,
                                       Ib              ib) noexcept
{
    array<uint8_t, kPageSize> page{};

    // Hard cap: a leaf holds at most floor(488/24) = 20 entries.
    if (count > kBbtMaxEntriesPerLeaf) {
        count = kBbtMaxEntriesPerLeaf;
    }

    for (size_t i = 0; i < count; ++i) {
        const size_t off = i * kBbtEntrySize;
        writeU64(page.data(), off + 0,  entries[i].bref.bid.value);
        writeU64(page.data(), off + 8,  entries[i].bref.ib.value);
        writeU16(page.data(), off + 16, entries[i].cb);
        writeU16(page.data(), off + 18, entries[i].cRef);
        writeU32(page.data(), off + 20, 0u);
    }

    writeU8(page.data(), kBtPageCEnt,    static_cast<uint8_t>(count));
    writeU8(page.data(), kBtPageCEntMax, static_cast<uint8_t>(kBbtMaxEntriesPerLeaf));
    writeU8(page.data(), kBtPageCbEnt,   static_cast<uint8_t>(kBbtLeafEntrySize)); // 24
    writeU8(page.data(), kBtPageCLevel,  0u);                                      // leaf
    writeU32(page.data(), kBtPageDwPad,  0u);

    writePageTrailer(page, ptype::kBBT, bid, ib);
    return page;
}

// ============================================================================
// Intermediate BBT page ([MS-PST] §2.2.2.7.7.1)
//
//   BTENTRY layout (24 bytes):
//     [0..7]   btkey  (BID of first entry in child page, 8 bytes LE)
//     [8..23]  BREF   (child page bid + ib)
// ============================================================================
array<uint8_t, kPageSize> buildBbtIntermediate(const BtEntry* entries,
                                               size_t         count,
                                               uint8_t        cLevel,
                                               Bid            bid,
                                               Ib             ib) noexcept
{
    array<uint8_t, kPageSize> page{};

    if (count > kBtMaxEntriesPerPage) {
        count = kBtMaxEntriesPerPage;
    }

    for (size_t i = 0; i < count; ++i) {
        const size_t off = i * kBtEntrySize;
        writeU64(page.data(), off + 0,  entries[i].btkey);
        writeU64(page.data(), off + 8,  entries[i].bref.bid.value);
        writeU64(page.data(), off + 16, entries[i].bref.ib.value);
    }

    writeU8(page.data(), kBtPageCEnt,    static_cast<uint8_t>(count));
    writeU8(page.data(), kBtPageCEntMax, static_cast<uint8_t>(kBtMaxEntriesPerPage));
    writeU8(page.data(), kBtPageCbEnt,   static_cast<uint8_t>(kBtEntrySize));
    writeU8(page.data(), kBtPageCLevel,  cLevel);
    writeU32(page.data(), kBtPageDwPad,  0u);

    writePageTrailer(page, ptype::kBBT, bid, ib);
    return page;
}

} // namespace pstwriter
