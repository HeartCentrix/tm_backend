// pstwriter/page.hpp
//
// Page-level builders ([MS-PST] §2.2.2.7) — return a fully-formed 512-byte
// page (body + PAGETRAILER) ready to fwrite to disk.
//
// Every page passes through `writePageTrailer` (defined in ndb.cpp) which
// computes wSig (per §5.5) and the page-body CRC. Callers do not need to
// manage either.
//
// M2 only needs three flavours of empty page; M3 will extend with
// intermediate B-tree pages, DList, etc.

#pragma once

#include "ndb.hpp"
#include "types.hpp"

#include <array>
#include <cstdint>

using namespace std;

namespace pstwriter {

// AMap covering [(ibAMap - 0x400) .. (ibAMap - 0x400) + 253952).  The first
// AMap of a Unicode PST always lives at file offset 0x400 (immediately
// after the HEADER's zero-padding tail).  `fileSize` is the total file
// size in bytes — every 64-byte unit below `fileSize` becomes one set bit
// in the bitmap; bytes beyond are zero (free).
//
// PAGETRAILER.bid is set to `ibAMap.value` (the page's own file offset).
// Per [MS-PST] §2.2.2.7.2 + §2.6.1, real Outlook walks `Read(@ib)` and
// asserts trailer.bid == ib for AMap (and PMap) pages; passing a raw
// internal BID like `Bid::makeInternal(1)` here makes Outlook reject the
// PST with "Outlook Data File Corruption / Read(@400): Expected bid=400
// but read bid=6". See KNOWN_UNVERIFIED.md M11-E. wSig is 0 for AMap.
array<uint8_t, kPageSize> buildAMap(Ib ibAMap, uint64_t fileSize) noexcept;

// ============================================================================
// buildEmptyPMap — emit an empty PMap page at file offset 0x4600
// ([MS-PST] §2.2.2.7.3). Required by scanpst even in fAMapValid=0x02
// mode (where PMap data is unused) — scanpst always validates the slot.
// Without this, scanpst raises five "PMap page @17920" errors as it
// reads our first data block and tries to parse it as a PMap.
// ============================================================================
array<uint8_t, kPageSize> buildEmptyPMap(uint64_t ibPMap) noexcept;

// ============================================================================
// buildDListPage — emit the DLISTPAGE at file offset 0x4200 ([MS-PST]
// §2.2.2.7.4). Without it, scanpst surfaces "PMap page @17920" errors
// because its DList lookup falls through to the zero-pad bytes there
// and treats them as a corrupt PMap. ibDList = 0x4200, ibAMap = 0x4400
// in the M2 skeleton.
// ============================================================================
array<uint8_t, kPageSize> buildDListPage(uint64_t ibDList,
                                         uint64_t ibAMap,
                                         uint64_t fileEof) noexcept;

// Empty NBT / BBT leaf pages (cEnt = 0, cLevel = 0).  cbEnt is fixed by
// the page type per [MS-PST] §2.2.2.7.7.1: 32 for an NBT leaf, 24 for a
// BBT leaf.  wSig is computed from (ib ^ bid.value).
array<uint8_t, kPageSize> buildEmptyNbtLeaf(Bid bid, Ib ib) noexcept;
array<uint8_t, kPageSize> buildEmptyBbtLeaf(Bid bid, Ib ib) noexcept;

// ============================================================================
// BBT entries ([MS-PST] §2.2.2.7.7.3 BBTENTRY)
//
// Each block written to a PST has one BBTENTRY recording its location
// (BREF), pre-encryption payload size (cb), and reference count (cRef).
// BBT leaves hold these sorted by BID ascending.  When more than 20
// entries (= floor(488/24)) need to fit, the writer creates additional
// leaves and an intermediate page (cLevel = 1) of BTENTRY records that
// addresses them.
// ============================================================================
struct BbtEntry {
    Bref     bref {};   // 16 bytes: BID + IB
    uint16_t cb   {};   // pre-encryption payload size (matches BLOCKTRAILER.cb)
    uint16_t cRef {1};  // reference count (1 = single-NID block)

    constexpr BbtEntry() noexcept = default;
    constexpr BbtEntry(Bref b, uint16_t c, uint16_t r) noexcept
        : bref(b), cb(c), cRef(r) {}
};

// BTENTRY ([MS-PST] §2.2.2.7.7.1) — used in intermediate NBT/BBT pages.
//   btkey (8 bytes): NID (NBT) or BID (BBT) of the first entry in the
//                    pointed-to child page.
//   bref  (16 bytes): location of that child page.
struct BtEntry {
    uint64_t btkey {};
    Bref     bref  {};

    constexpr BtEntry() noexcept = default;
    constexpr BtEntry(uint64_t k, Bref b) noexcept : btkey(k), bref(b) {}
};

constexpr size_t kBbtEntrySize = 24;
constexpr size_t kBtEntrySize  = 24;
constexpr size_t kBbtMaxEntriesPerLeaf = kBtPageEntriesArea / kBbtEntrySize; // 20
constexpr size_t kBtMaxEntriesPerPage  = kBtPageEntriesArea / kBtEntrySize;  // 20

// Build a BBT leaf page populated with `entries[0..count)`.  Caller must
// pre-sort by `bref.bid.value` ascending.  count must be in [0, 20].
array<uint8_t, kPageSize> buildBbtLeaf(const BbtEntry* entries,
                                       size_t          count,
                                       Bid             bid,
                                       Ib              ib) noexcept;

// Build an intermediate BBT page (cLevel >= 1) holding `entries`.
// `cLevel` distinguishes 1 (children are leaves) from 2+ (children are
// intermediate pages); the byte shape is the same.
array<uint8_t, kPageSize> buildBbtIntermediate(const BtEntry* entries,
                                               size_t         count,
                                               uint8_t        cLevel,
                                               Bid            bid,
                                               Ib             ib) noexcept;

} // namespace pstwriter
