// pstwriter/src/nbt.cpp
//
// Implementation of the M5 Phase B NBT page writers and Phase C reader.

#include "nbt.hpp"

#include "crc.hpp"
#include "ndb.hpp"
#include "page.hpp"
#include "types.hpp"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <vector>

using namespace std;

namespace pstwriter {

using detail::readU16;
using detail::readU32;
using detail::readU64;
using detail::writeU8;
using detail::writeU32;
using detail::writeU64;

// ============================================================================
// NBT leaf page ([SPEC sec 2.2.2.7.7.3] NBTENTRY layout)
//
// Sorts entries internally. Caller may pass any ordering; the builder
// emits them in NID-ascending order.
// ============================================================================
array<uint8_t, kPageSize> buildNbtLeaf(const NbtEntry* entries,
                                       size_t          count,
                                       Bid             bid,
                                       Ib              ib)
{
    if (count > kNbtLeafMaxEntries) {
        throw runtime_error(
            "buildNbtLeaf: count exceeds kNbtLeafMaxEntries (15); caller "
            "must paginate via buildNbtTree");
    }

    // Copy and sort by NID ascending (full 32-bit NID value).
    vector<NbtEntry> sorted(entries, entries + count);
    std::sort(sorted.begin(), sorted.end(),
              [](const NbtEntry& a, const NbtEntry& b) {
                  return a.nid.value < b.nid.value;
              });

    array<uint8_t, kPageSize> page{};

    // NBTENTRY layout (Unicode, 32 bytes per entry):
    //   [0..3]   nid (4 B)
    //   [4..7]   pad (4 B, zero -- nid is "extended to 8 bytes" per spec)
    //   [8..15]  bidData (8 B)
    //   [16..23] bidSub (8 B)
    //   [24..27] nidParent (4 B)
    //   [28..31] dwPadding (4 B, MUST be zero)
    for (size_t i = 0; i < sorted.size(); ++i) {
        const size_t off = i * kNbtLeafEntrySize;
        writeU32(page.data(), off + 0,  sorted[i].nid.value);
        writeU32(page.data(), off + 4,  0u);
        writeU64(page.data(), off + 8,  sorted[i].bidData.value);
        writeU64(page.data(), off + 16, sorted[i].bidSub.value);
        writeU32(page.data(), off + 24, sorted[i].nidParent.value);
        writeU32(page.data(), off + 28, 0u);
    }

    writeU8 (page.data(), kBtPageCEnt,    static_cast<uint8_t>(sorted.size()));
    writeU8 (page.data(), kBtPageCEntMax, static_cast<uint8_t>(kNbtLeafMaxEntries));
    writeU8 (page.data(), kBtPageCbEnt,   static_cast<uint8_t>(kNbtLeafEntrySize));
    writeU8 (page.data(), kBtPageCLevel,  0u);
    writeU32(page.data(), kBtPageDwPad,   0u);

    writePageTrailer(page, ptype::kNBT, bid, ib);
    return page;
}

// ============================================================================
// Intermediate BTPAGE ([SPEC sec 2.2.2.7.7.2] BTENTRY layout)
//
// Single writer for both NBT and BBT intermediate pages -- the format
// is identical except for the ptype byte (per spec text "both
// intermediate NBT and BBT pages share this format").
//
// Sorts entries by btkey ascending.
// ============================================================================
array<uint8_t, kPageSize> buildBtIntermediate(const BtEntry* entries,
                                              size_t         count,
                                              uint8_t        cLevel,
                                              uint8_t        ptype,
                                              Bid            bid,
                                              Ib             ib)
{
    if (count > kBtIntermediateMaxEntries) {
        throw runtime_error(
            "buildBtIntermediate: count exceeds kBtIntermediateMaxEntries (20)");
    }
    if (cLevel == 0u) {
        throw runtime_error(
            "buildBtIntermediate: cLevel must be >= 1 (cLevel=0 is for leaf "
            "pages; use buildNbtLeaf or buildBbtLeaf)");
    }
    if (ptype != ptype::kNBT && ptype != ptype::kBBT) {
        throw runtime_error(
            "buildBtIntermediate: ptype must be ptypeNBT (0x81) or ptypeBBT (0x80)");
    }

    // Copy and sort by btkey ascending.
    vector<BtEntry> sorted(entries, entries + count);
    std::sort(sorted.begin(), sorted.end(),
              [](const BtEntry& a, const BtEntry& b) {
                  return a.btkey < b.btkey;
              });

    array<uint8_t, kPageSize> page{};

    // BTENTRY layout (Unicode, 24 bytes per entry):
    //   [0..7]   btkey (8 B, NID zero-extended for NBT or BID for BBT)
    //   [8..15]  BREF.bid (8 B)
    //   [16..23] BREF.ib  (8 B)
    for (size_t i = 0; i < sorted.size(); ++i) {
        const size_t off = i * kBtEntrySize;
        writeU64(page.data(), off + 0,  sorted[i].btkey);
        writeU64(page.data(), off + 8,  sorted[i].bref.bid.value);
        writeU64(page.data(), off + 16, sorted[i].bref.ib.value);
    }

    writeU8 (page.data(), kBtPageCEnt,    static_cast<uint8_t>(sorted.size()));
    writeU8 (page.data(), kBtPageCEntMax, static_cast<uint8_t>(kBtIntermediateMaxEntries));
    writeU8 (page.data(), kBtPageCbEnt,   static_cast<uint8_t>(kBtEntrySize));
    writeU8 (page.data(), kBtPageCLevel,  cLevel);
    writeU32(page.data(), kBtPageDwPad,   0u);

    writePageTrailer(page, ptype, bid, ib);
    return page;
}

// ============================================================================
// Multi-page tree builder
// ============================================================================
NbtTreeOutput buildNbtTree(const NbtEntry*         entries,
                           size_t                  count,
                           const NbtTreeInputBids& bids)
{
    // Sort entries up front so leaf splits are sorted by NID across all
    // leaves (i.e. leaf[0] holds the smallest NIDs, leaf[K-1] holds the
    // largest). This is the BTPAGE invariant the reader relies on.
    vector<NbtEntry> sorted(entries, entries + count);
    std::sort(sorted.begin(), sorted.end(),
              [](const NbtEntry& a, const NbtEntry& b) {
                  return a.nid.value < b.nid.value;
              });

    const size_t leafCount =
        (sorted.empty()) ? 0u
                         : (sorted.size() + kNbtLeafMaxEntries - 1) / kNbtLeafMaxEntries;
    if (leafCount > kBtIntermediateMaxEntries) {
        throw runtime_error(
            "buildNbtTree: entry count requires more leaves than fit in one "
            "intermediate page; multi-level pagination is M7+");
    }

    NbtTreeOutput out;

    // Empty tree case: a single empty leaf.
    if (sorted.empty()) {
        out.leafPages.push_back(
            buildNbtLeaf(nullptr, 0, bids.leafBrefs[0].bid, bids.leafBrefs[0].ib));
        out.hasIntermediate = false;
        out.rootBref = bids.leafBrefs[0];
        out.treeLevel = 0;
        return out;
    }

    // Build each leaf.
    out.leafPages.reserve(leafCount);
    for (size_t li = 0; li < leafCount; ++li) {
        const size_t lo = li * kNbtLeafMaxEntries;
        const size_t hi = std::min(lo + kNbtLeafMaxEntries, sorted.size());
        out.leafPages.push_back(
            buildNbtLeaf(sorted.data() + lo, hi - lo,
                         bids.leafBrefs[li].bid,
                         bids.leafBrefs[li].ib));
    }

    if (leafCount == 1) {
        // Single leaf serves as the root.
        out.hasIntermediate = false;
        out.rootBref = bids.leafBrefs[0];
        out.treeLevel = 0;
        return out;
    }

    // Multi-leaf: build one intermediate page above. Each BTENTRY's
    // btkey is the SMALLEST NID in the corresponding leaf (per [SPEC
    // sec 2.2.2.7.7.2]: "All the entries in the child BTPAGE referenced
    // by BREF have key values greater than or equal to this key value").
    vector<BtEntry> btEntries;
    btEntries.reserve(leafCount);
    for (size_t li = 0; li < leafCount; ++li) {
        const size_t firstIdxInLeaf = li * kNbtLeafMaxEntries;
        const uint64_t btkey =
            static_cast<uint64_t>(sorted[firstIdxInLeaf].nid.value);
        btEntries.emplace_back(btkey, bids.leafBrefs[li]);
    }

    out.intermediatePage = buildBtIntermediate(
        btEntries.data(), btEntries.size(),
        /*cLevel*/ 1u,
        /*ptype */ ptype::kNBT,
        bids.intermediateBref.bid,
        bids.intermediateBref.ib);
    out.hasIntermediate = true;
    out.rootBref = bids.intermediateBref;
    out.treeLevel = 1;
    return out;
}

// ============================================================================
// NBT reader (Phase C)
// ============================================================================
namespace {

// Return a pointer to the BTPAGE at the given IB after validating spec
// invariants. Throws on any violation.
const uint8_t* fetchAndValidatePage(const uint8_t*       fileBytes,
                                    size_t               fileSize,
                                    uint64_t             ib,
                                    const NbtReadOptions& opts)
{
    if (ib > fileSize || ib + kPageSize > fileSize) {
        throw runtime_error("nbt reader: page IB extends past EOF");
    }
    const uint8_t* page = fileBytes + static_cast<size_t>(ib);

    // PAGETRAILER (16 B at offset 496):
    const uint8_t pType       = page[kPageTrailerOffset + 0];
    const uint8_t pTypeRepeat = page[kPageTrailerOffset + 1];

    // Reader expects ptypeNBT (0x81) for NBT pages. (BBT-side reader is
    // M3's domain; we only walk NBT here.)
    if (pType != ptype::kNBT) {
        throw runtime_error(
            "nbt reader: page ptype != ptypeNBT (page is not part of NBT)");
    }
    if (pType != pTypeRepeat) {
        throw runtime_error(
            "nbt reader: PAGETRAILER ptype != ptypeRepeat");
    }

    // BTPAGE control bytes:
    const uint8_t cEnt    = page[kBtPageCEnt];
    const uint8_t cEntMax = page[kBtPageCEntMax];
    const uint8_t cbEnt   = page[kBtPageCbEnt];
    const uint8_t cLevel  = page[kBtPageCLevel];
    if (cEnt > cEntMax) {
        throw runtime_error("nbt reader: BTPAGE.cEnt > cEntMax");
    }
    if (cLevel == 0u) {
        if (cbEnt != kNbtLeafEntrySize) {
            throw runtime_error(
                "nbt reader: leaf NBT page has cbEnt != 32");
        }
    } else {
        if (cbEnt != kBtEntrySize) {
            throw runtime_error(
                "nbt reader: intermediate NBT page has cbEnt != 24");
        }
        if (cLevel > 1u) {
            // M5 supports cLevel <= 1 only (per design doc).
            throw runtime_error(
                "nbt reader: cLevel > 1 (multi-level intermediate is M7+)");
        }
    }

    if (opts.strictCrc) {
        const uint32_t storedCrc =
            readU32(page, kPageTrailerOffset + 4);
        const uint32_t computedCrc = crc32(page, kPageBodySize);
        if (storedCrc != computedCrc) {
            throw runtime_error("nbt reader: dwCRC mismatch");
        }
    }
    return page;
}

// Decode one NBTENTRY from the leaf page at the given record offset.
NbtRecord decodeNbtEntry(const uint8_t* page, size_t recOff) noexcept
{
    NbtRecord r;
    r.nid       = Nid{readU32(page, recOff + 0)};
    // bytes 4..7 are zero pad (per spec NID is "extended to 8 bytes").
    r.bidData   = Bid{readU64(page, recOff + 8)};
    r.bidSub    = Bid{readU64(page, recOff + 16)};
    r.nidParent = Nid{readU32(page, recOff + 24)};
    return r;
}

// Walk a leaf BTPAGE, looking for `target`. Returns true if found.
// Linear scan -- leaf has at most 15 NBTENTRYs.
bool searchLeaf(const uint8_t* page, Nid target, NbtRecord* out) noexcept
{
    const uint8_t cEnt = page[kBtPageCEnt];
    for (size_t i = 0; i < cEnt; ++i) {
        const NbtRecord r = decodeNbtEntry(page, i * kNbtLeafEntrySize);
        if (r.nid.value == target.value) {
            if (out) *out = r;
            return true;
        }
    }
    return false;
}

// Walk an intermediate BTPAGE. Find the BTENTRY whose btkey is the
// largest <= target (= "rightmost child whose keyspace covers target").
// Returns the child BREF, or throws if no such child exists (target
// less than the smallest btkey -- shouldn't happen in a well-formed NBT
// because the smallest btkey of the root must be <= the smallest NID
// in the tree, which in turn is the smallest NID we could ever ask for).
Bref descendIntermediate(const uint8_t* page, Nid target)
{
    const uint8_t cEnt = page[kBtPageCEnt];
    if (cEnt == 0u) {
        throw runtime_error(
            "nbt reader: intermediate BTPAGE has cEnt == 0");
    }

    // Find the entry with the largest btkey <= target.value. BTENTRYs
    // are sorted ascending by btkey (Phase B writer guarantees this).
    int chosen = -1;
    for (size_t i = 0; i < cEnt; ++i) {
        const size_t off = i * kBtEntrySize;
        const uint64_t btkey = readU64(page, off + 0);
        if (btkey <= static_cast<uint64_t>(target.value)) {
            chosen = static_cast<int>(i);
        } else {
            break;
        }
    }

    if (chosen < 0) {
        // Target is smaller than the smallest btkey. By spec,
        // "all entries in the child BTPAGE referenced by BREF have key
        // values >= this btkey" -- so target is below every child's
        // keyspace. NID is not present.
        Bref invalid; // bid=0, ib=0 -- callers check by .bid.value == 0
        return invalid;
    }

    const size_t off = static_cast<size_t>(chosen) * kBtEntrySize;
    Bref child;
    child.bid = Bid{readU64(page, off + 8)};
    child.ib  = Ib {readU64(page, off + 16)};
    return child;
}

void enumerateLeaf(const uint8_t* page, vector<NbtRecord>& out)
{
    const uint8_t cEnt = page[kBtPageCEnt];
    for (size_t i = 0; i < cEnt; ++i) {
        out.push_back(decodeNbtEntry(page, i * kNbtLeafEntrySize));
    }
}

} // anonymous namespace

bool nbtFind(const uint8_t*        fileBytes,
             size_t                fileSize,
             Bref                  rootBref,
             Nid                   target,
             NbtRecord*            out,
             const NbtReadOptions& opts)
{
    const uint8_t* page = fetchAndValidatePage(
        fileBytes, fileSize, rootBref.ib.value, opts);

    if (page[kBtPageCLevel] == 0u) {
        return searchLeaf(page, target, out);
    }

    // cLevel == 1: descend to the leaf.
    const Bref childBref = descendIntermediate(page, target);
    if (childBref.bid.value == 0u && childBref.ib.value == 0u) {
        return false; // target below all children
    }
    const uint8_t* leaf = fetchAndValidatePage(
        fileBytes, fileSize, childBref.ib.value, opts);
    if (leaf[kBtPageCLevel] != 0u) {
        throw runtime_error(
            "nbt reader: child of cLevel=1 page is not a leaf");
    }
    return searchLeaf(leaf, target, out);
}

void nbtForEach(const uint8_t*        fileBytes,
                size_t                fileSize,
                Bref                  rootBref,
                vector<NbtRecord>&    outRecords,
                const NbtReadOptions& opts)
{
    const uint8_t* root = fetchAndValidatePage(
        fileBytes, fileSize, rootBref.ib.value, opts);
    outRecords.clear();

    if (root[kBtPageCLevel] == 0u) {
        enumerateLeaf(root, outRecords);
        return;
    }

    // cLevel == 1: enumerate every leaf in BTENTRY order.
    const uint8_t cEnt = root[kBtPageCEnt];
    for (size_t i = 0; i < cEnt; ++i) {
        const size_t off = i * kBtEntrySize;
        Bref childBref;
        childBref.bid = Bid{readU64(root, off + 8)};
        childBref.ib  = Ib {readU64(root, off + 16)};
        const uint8_t* leaf = fetchAndValidatePage(
            fileBytes, fileSize, childBref.ib.value, opts);
        if (leaf[kBtPageCLevel] != 0u) {
            throw runtime_error(
                "nbt reader: forEach: child of cLevel=1 page is not a leaf");
        }
        enumerateLeaf(leaf, outRecords);
    }
}

} // namespace pstwriter
