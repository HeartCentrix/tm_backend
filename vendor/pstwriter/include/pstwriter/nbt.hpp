// pstwriter/nbt.hpp
//
// M5 Phase B - NBT page writers.
//
// Spec references (verified verbatim 2026-05-02 via WebFetch):
//   * [SPEC sec 2.2.2.7.7]   BTPAGE generic structure (488 B rgentries +
//                            cEnt/cEntMax/cbEnt/cLevel + 4 B dwPadding +
//                            16 B PAGETRAILER, total 512 B Unicode).
//   * [SPEC sec 2.2.2.7.7.1] cEnt/cEntMax/cbEnt/cLevel field semantics.
//                            cbEnt table:
//                              NBT cLevel=0 -> NBTENTRY (Unicode 32 B)
//                              NBT cLevel>0 -> BTENTRY  (Unicode 24 B)
//                              BBT cLevel=0 -> BBTENTRY (Unicode 24 B)
//                              BBT cLevel>0 -> BTENTRY  (Unicode 24 B)
//   * [SPEC sec 2.2.2.7.7.2] BTENTRY: btkey (8 B, NID zero-extended for
//                            NBT, BID for BBT) + BREF (16 B = bid + ib).
//   * [SPEC sec 2.2.2.7.7.3] NBTENTRY: nid (4 B + 4 B pad) + bidData (8 B)
//                            + bidSub (8 B) + nidParent (4 B) + 4 B pad.
//   * [SPEC sec 2.2.2.7.1]   PAGETRAILER: ptype (1 B) + ptypeRepeat (1 B)
//                            + wSig (2 B) + dwCRC (4 B) + bid (8 B).
//                            dwCRC scope is bytes [0..496) -- four
//                            positive controls (sec 3.2/3.3/3.4/3.6).
//
// Phase B finding (Phase B, 2026-05-02): cEntMax is NOT empirical -- it
// equals floor(488 / cbEnt) = 15 for NBT leaf, 20 for everything else.
// No KNOWN_UNVERIFIED entry needed; the value is derivable.
//
// Reader contract (PROSPECTIVE -- enforced by Phase C):
//   * NBT walker is NID-order-agnostic. It walks the tree by binary
//     search descent against btkey, never assuming the writer's tidy
//     deterministic layout. Same lesson as M4's HID-agnostic PC reader,
//     applied prospectively to NBT before the first reader exists.

#pragma once

#include "ndb.hpp"
#include "page.hpp"   // BtEntry, BbtEntry, kBtMaxEntriesPerPage, etc.
#include "types.hpp"

#include <array>
#include <cstdint>
#include <vector>

using namespace std;

namespace pstwriter {

// ============================================================================
// NBTENTRY ([SPEC sec 2.2.2.7.7.3])
// ============================================================================
struct NbtEntry {
    Nid nid       {};   // 4 B (zero-extended to 8 B on disk)
    Bid bidData   {};   // 8 B - data block BID (0 if node has no data)
    Bid bidSub    {};   // 8 B - subnode block BID (0 if no subnodes)
    Nid nidParent {};   // 4 B - parent folder NID for messaging-layer
                        //       children, else 0 (sec 2.2.2.7.7.4.1)

    constexpr NbtEntry() noexcept = default;
    constexpr NbtEntry(Nid n, Bid bd, Bid bs, Nid np) noexcept
        : nid(n), bidData(bd), bidSub(bs), nidParent(np) {}
};

// Per-page-type cEntMax values (= floor(488 / cbEnt), [SPEC sec 2.2.2.7.7.1]).
constexpr size_t kNbtLeafMaxEntries        = 15; // 488 / 32 = 15
constexpr size_t kBtIntermediateMaxEntries = 20; // 488 / 24 = 20

// ============================================================================
// Single-page builders (Phase B core API)
//
// Both builders sort entries internally (caller may supply any order).
// Both ACCEPT count == 0 and produce a valid empty page (parallel to
// M3's buildEmptyNbtLeaf / buildEmptyBbtLeaf, which existed for the M2
// 5-page-skeleton case).
// ============================================================================

// NBT leaf page (cLevel = 0, ptype = ptypeNBT). Sorts entries by
// nid.value ascending. Throws if count > kNbtLeafMaxEntries (caller
// must paginate via buildNbtTree).
array<uint8_t, kPageSize> buildNbtLeaf(const NbtEntry* entries,
                                       size_t          count,
                                       Bid             bid,
                                       Ib              ib);

// Intermediate BTPAGE (cLevel >= 1) for either NBT or BBT.
// `ptype` is the only difference between intermediate NBT and BBT pages
// (per [SPEC sec 2.2.2.7.7] format-shared text + sec 3.3 NBT positive
// control + KNOWN_UNVERIFIED.md "intermediate BBT format" entry).
// Sorts entries by btkey ascending. Throws if count >
// kBtIntermediateMaxEntries.
array<uint8_t, kPageSize> buildBtIntermediate(const BtEntry* entries,
                                              size_t         count,
                                              uint8_t        cLevel,
                                              uint8_t        ptype,
                                              Bid            bid,
                                              Ib             ib);

// ============================================================================
// Multi-page tree builder (Phase B pagination test)
//
// Given N NBT entries, builds either:
//   * 1 leaf page             if N <= kNbtLeafMaxEntries (= 15)
//   * K leaf pages + 1 intermediate page  if N > 15 (K = ceil(N / 15))
//
// Caller supplies the (bid, ib) for each PAGE that needs to be emitted.
// Pages are returned in the same order: leaves first, intermediate (if
// any) last. The caller is responsible for placing them at the supplied
// IBs in the produced PST file.
//
// M5 supports tree depth at most 1 (= one intermediate level above
// leaves). If N exceeds K_max * kNbtLeafMaxEntries (where K_max =
// kBtIntermediateMaxEntries = 20), throws std::runtime_error -- multi-
// level intermediate pagination is M7+.
// ============================================================================
struct NbtTreeInputBids {
    const Bref* leafBrefs;       // size = ceil(count / kNbtLeafMaxEntries)
    Bref        intermediateBref;// unused if only one leaf is needed
};

struct NbtTreeOutput {
    vector<array<uint8_t, kPageSize>> leafPages;
    array<uint8_t, kPageSize>         intermediatePage{};
    bool                              hasIntermediate{false};
    Bref                              rootBref{};   // = intermediate if any, else leaf[0]
    uint8_t                           treeLevel{0}; // 0 single leaf, 1 has intermediate
};

NbtTreeOutput buildNbtTree(const NbtEntry*          entries,
                           size_t                   count,
                           const NbtTreeInputBids&  bids);

// ============================================================================
// NBT reader (Phase C)
//
// Walks an NBT given the root BREF and a memory-mapped PST byte buffer.
// The reader is **NID-order-agnostic**: it makes NO assumption about
// NID stride, contiguity, or matching M5Allocator's deterministic output
// sequence. Resolution proceeds by binary-search descent through
// intermediate BTPAGEs to a leaf, then linear scan within the leaf.
//
// This is the same lesson as the M4 PC reader's HID-agnosticism (vindicated
// by the sec 3.9 cross-validation that found Outlook-extracted HIDs
// non-monotonic). For NBT we have no spec sample with non-monotonic NIDs
// to cross-validate against, so the test [non_monotonic_nids] explicitly
// constructs that case.
//
// Spec invariants ENFORCED (each throws std::runtime_error):
//   * PAGETRAILER.ptype matches BTPAGE expectation (ptypeNBT)
//   * PAGETRAILER.ptypeRepeat == ptype
//   * BTPAGE.cbEnt is one of {kNbtLeafEntrySize=32, kBtEntrySize=24}
//   * BTPAGE.cEnt <= BTPAGE.cEntMax
//   * dwCRC matches crc32(bytes[0..496))   (when strictMode)
//   * IB read does not overflow the file buffer
//   * Tree depth bounded -- M5 cap is cLevel <= 1
//
// Spec invariants NOT ENFORCED (would silently break compatibility with
// real Outlook PSTs):
//   * NID stride / contiguity / writer-determined layout
//   * dwPadding bytes equal to zero (per 2.2.2.7.7.1: "unused space can
//     contain any value")
//   * NBTENTRY ordering consistency across leaves (we trust each leaf's
//     local sort but don't assert global monotonic across siblings)
// ============================================================================
struct NbtRecord {
    Nid nid       {};
    Bid bidData   {};
    Bid bidSub   {};
    Nid nidParent {};
};

struct NbtReadOptions {
    bool strictCrc = true; // verify each page's dwCRC
};

// Look up a NID by walking the NBT from rootBref. Returns true if found
// and populates *out. Returns false if the NID is not present (no
// throw). Throws std::runtime_error on structural / invariant violations.
bool nbtFind(const uint8_t*       fileBytes,
             size_t               fileSize,
             Bref                 rootBref,
             Nid                  target,
             NbtRecord*           out,
             const NbtReadOptions& opts = {});

// Enumerate all NBTENTRYs reachable from rootBref. Visits each
// (in NID-ascending order across all leaves). Throws on structural
// violations.
void nbtForEach(const uint8_t*       fileBytes,
                size_t               fileSize,
                Bref                 rootBref,
                vector<NbtRecord>&   outRecords,
                const NbtReadOptions& opts = {});

} // namespace pstwriter
