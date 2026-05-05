// pstwriter/src/ltp.cpp
//
// LTP layer implementation ([MS-PST] §2.3).
//
// First cut: buildHeapOnNode + encodeBthHeader. BTH/PC/TC builders
// layer on top in subsequent files.

#include "ltp.hpp"

#include "types.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <vector>

using namespace std;
using namespace pstwriter;
using detail::writeU8;
using detail::writeU16;
using detail::writeU32;

namespace pstwriter {

// ----------------------------------------------------------------------------
// buildHeapOnNode
// ----------------------------------------------------------------------------
vector<uint8_t> buildHeapOnNode(const HnAllocation* allocs,
                                size_t              allocCount,
                                uint8_t             bClientSig,
                                uint32_t            hidUserRoot,
                                array<uint8_t, 4>   rgbFillLevel) noexcept
{
    // ------------------------------------------------------------------
    // Compute the layout.
    //   ibAlloc[i]   = absolute heap offset of allocation i
    //   ibHnpm       = absolute offset of the HNPAGEMAP
    //
    // The HNPAGEMAP is the FINAL element of the structured body: it
    // immediately follows the last allocation. Total HN body size =
    // ibHnpm + HNPAGEMAP size.
    // ------------------------------------------------------------------
    // Pre-flight: cumulative cursor past all user allocations. If this
    // is not DWORD-aligned, we need to insert a "phantom" allocation
    // that absorbs the alignment pad — see M11-H below.
    uint16_t cursorAfterUser = static_cast<uint16_t>(kHnHdrSize);
    for (size_t i = 0; i < allocCount; ++i) {
        cursorAfterUser = static_cast<uint16_t>(cursorAfterUser + allocs[i].size);
    }
    const uint16_t ibHnpm = static_cast<uint16_t>((cursorAfterUser + 3u) & ~uint32_t{3u});
    const bool     hasPhantom = (cursorAfterUser != ibHnpm);

    // HNPAGEMAP must start at a 4-byte (DWORD) boundary. [MS-PST]
    // §2.3.1.5 prose: "rgibAlloc[cAlloc] points to the location of
    // the next allocation. This in turn implies that the value MUST
    // be DWORD-aligned." Real-Outlook scanpst confirms this — it
    // flags `ibLast != ibHnpm` on every HN as an error in recovery.
    //
    // If cumulative user-alloc cursor is not DWORD-aligned, the
    // simplest fix (push ibHnpm as the sentinel) makes the LAST user
    // allocation appear N bytes longer to consumers (the pad bytes
    // get attributed to it), which corrupts PT_BINARY values and
    // bloats PT_UNICODE strings. Instead we insert a "phantom"
    // allocation between the last user data and ibHnpm — bytes
    // [cursorAfterUser, ibHnpm) become a separate (zero-filled)
    // allocation. cAlloc grows by 1 in this case; user allocations
    // keep their exact size when consumers compute it via
    // rgibAlloc[i+1] - rgibAlloc[i]. The phantom HID is unreferenced.
    //
    // Note the §3.11 spec sample places the sentinel at ibHnpm-1
    // without a phantom (cAlloc=7, rgibAlloc[7]=0x1BB, ibHnpm=0x1BC).
    // We follow [MS-PST] §2.3.1.5 prose + scanpst, which makes the
    // §3.11 byte-diff test diverge at the rgibAlloc[cAlloc] slot and
    // (when phantom needed) at cAlloc itself. (M11-H.)
    const uint16_t cAlloc = static_cast<uint16_t>(allocCount + (hasPhantom ? 1u : 0u));

    vector<uint16_t> ibAlloc;
    ibAlloc.reserve(static_cast<size_t>(cAlloc) + 1u);
    uint16_t cursor = static_cast<uint16_t>(kHnHdrSize);
    for (size_t i = 0; i < allocCount; ++i) {
        ibAlloc.push_back(cursor);
        cursor = static_cast<uint16_t>(cursor + allocs[i].size);
    }
    if (hasPhantom) {
        ibAlloc.push_back(cursor);  // start of phantom = end of last user alloc
    }
    ibAlloc.push_back(ibHnpm);      // sentinel = ibHnpm (DWORD-aligned, M11-H)

    const size_t   pageMapBytes  = kHnPageMapHdrSize
                                 + (static_cast<size_t>(cAlloc) + 1u) * kHnPageMapEntrySize;
    const size_t   totalSize     = static_cast<size_t>(ibHnpm) + pageMapBytes;

    vector<uint8_t> out(totalSize, 0u);

    // ------------------------------------------------------------------
    // HNHDR ([MS-PST] §2.3.1.2) — bytes [0..11]
    // ------------------------------------------------------------------
    writeU16(out.data(), 0, ibHnpm);                  // ibHnpm
    writeU8 (out.data(), 2, kHnSignature);            // bSig = 0xEC
    writeU8 (out.data(), 3, bClientSig);              // bClientSig
    writeU32(out.data(), 4, hidUserRoot);             // hidUserRoot
    out[ 8] = rgbFillLevel[0];
    out[ 9] = rgbFillLevel[1];
    out[10] = rgbFillLevel[2];
    out[11] = rgbFillLevel[3];

    // ------------------------------------------------------------------
    // User allocations — copied verbatim.
    // ------------------------------------------------------------------
    for (size_t i = 0; i < allocCount; ++i) {
        if (allocs[i].size == 0) continue;
        std::memcpy(out.data() + ibAlloc[i], allocs[i].data, allocs[i].size);
    }

    // ------------------------------------------------------------------
    // HNPAGEMAP ([MS-PST] §2.3.1.5) — at offset ibHnpm. cAlloc covers
    // user allocations PLUS the M11-H phantom (when present). cFree
    // counts zero-size user allocations (M11-J: was hardcoded 0;
    // scanpst flagged "free alloc count doesn't match (computed=N,
    // expected=0)" on every empty TC, blocking column validation).
    // The phantom is NOT counted as free — it has size 1..3 bytes.
    // ------------------------------------------------------------------
    uint16_t cFree = 0;
    for (size_t i = 0; i < allocCount; ++i) {
        if (allocs[i].size == 0u) ++cFree;
    }

    const size_t pmOff = ibHnpm;
    writeU16(out.data(), pmOff + 0, cAlloc);                              // cAlloc
    writeU16(out.data(), pmOff + 2, cFree);                               // cFree (M11-J)
    for (size_t i = 0; i <= cAlloc; ++i) {
        writeU16(out.data(),
                 pmOff + kHnPageMapHdrSize + i * kHnPageMapEntrySize,
                 ibAlloc[i]);
    }

    return out;
}

// ----------------------------------------------------------------------------
// encodeBthHeader
// ----------------------------------------------------------------------------
array<uint8_t, 8> encodeBthHeader(uint8_t  cbKey,
                                  uint8_t  cbEnt,
                                  uint8_t  bIdxLevels,
                                  uint32_t hidRoot) noexcept
{
    array<uint8_t, 8> out{};
    writeU8 (out.data(), 0, kBthSignature);  // bType = 0xB5
    writeU8 (out.data(), 1, cbKey);
    writeU8 (out.data(), 2, cbEnt);
    writeU8 (out.data(), 3, bIdxLevels);
    writeU32(out.data(), 4, hidRoot);
    return out;
}

// ----------------------------------------------------------------------------
// Read a key as an unsigned LE integer of width `cbKey`. Used as the
// sort comparator. Up to 16-byte keys are supported (the BTH spec caps
// cbKey at 16); we sort lexicographically on the bytes from MSB down,
// which is equivalent to "as a big integer".
// ----------------------------------------------------------------------------
namespace {

bool keyLess(const uint8_t* a, const uint8_t* b, size_t cbKey) noexcept
{
    // BTH stores keys little-endian, so the most-significant byte is at
    // [cbKey - 1] and the least-significant is at [0]. Compare from MSB
    // down to LSB.
    for (size_t i = cbKey; i > 0; --i) {
        const uint8_t av = a[i - 1];
        const uint8_t bv = b[i - 1];
        if (av != bv) return av < bv;
    }
    return false;
}

} // namespace

// ----------------------------------------------------------------------------
// buildBthLeafRecords
// ----------------------------------------------------------------------------
vector<uint8_t> buildBthLeafRecords(const BthRecord* records,
                                    size_t           recordCount,
                                    uint8_t          cbKey,
                                    uint8_t          cbEnt)
{
    // Validate dimension constraints first. cbKey in {1,2,4,8,16} per
    // [MS-PST] §2.3.2.1 implicitly (BTHHEADER cbKey is a byte but only
    // power-of-two widths are used by Outlook).
    const bool keyOk = (cbKey == 1 || cbKey == 2 || cbKey == 4
                     || cbKey == 8 || cbKey == 16);
    if (!keyOk) {
        throw std::invalid_argument(
            "buildBthLeafRecords: cbKey must be 1, 2, 4, 8, or 16");
    }
    if (cbEnt == 0 || cbEnt > 32) {
        throw std::invalid_argument(
            "buildBthLeafRecords: cbEnt must be in 1..32");
    }

    const size_t recSize = static_cast<size_t>(cbKey) + cbEnt;
    const size_t totalBytes = recordCount * recSize;

    // Capacity check: leaf must fit in a single HN allocation. M4 cut
    // is leaf-only; multi-level BTH (intermediate index pages) is M5+.
    if (totalBytes > kHnAllocMax) {
        throw std::length_error(
            "buildBthLeafRecords: total bytes exceed kHnAllocMax (3580); "
            "intermediate BTH levels not supported in M4");
    }

    // Sort records by key ascending. We sort indices to keep `records`
    // const and avoid copying the BthRecord struct unnecessarily.
    vector<size_t> order(recordCount);
    for (size_t i = 0; i < recordCount; ++i) order[i] = i;
    std::sort(order.begin(), order.end(),
        [records, cbKey](size_t lhs, size_t rhs) noexcept {
            return keyLess(records[lhs].keyBytes,
                           records[rhs].keyBytes, cbKey);
        });

    // Tight packing — key followed by data, no padding between records.
    vector<uint8_t> out(totalBytes);
    for (size_t i = 0; i < recordCount; ++i) {
        const BthRecord& r = records[order[i]];
        std::memcpy(out.data() + i * recSize,           r.keyBytes,  cbKey);
        std::memcpy(out.data() + i * recSize + cbKey,   r.dataBytes, cbEnt);
    }
    return out;
}

// ----------------------------------------------------------------------------
// PropType classification ([MS-OXCDATA] §2.11.1).
//
// `fixedSize(t)`:
//   * Returns the on-disk size of the value for fixed-size types.
//   * Returns 0 for variable-size types (caller's valueSize is authoritative).
//
// Rules used by the §2.3.3.3 storage-decision table:
//   * "Fixed-size, cb ≤ 4": Boolean (1), Int16 (2), Int32 / Float32 /
//     ErrorCode (4) — inline in the BTH record's 4-byte data slot.
//   * "Fixed-size, cb > 4":  Int64 / Float64 / Currency / AppTime /
//     SystemTime (8), ClassId (16) — HN allocation referenced by HID.
//   * Variable-size (everything else, including PT_MV_*): HID if
//     valueSize ≤ 3580, otherwise subnode.
// ----------------------------------------------------------------------------
namespace {

size_t fixedSize(PropType t) noexcept
{
    switch (t) {
        case PropType::Boolean:    return 1;
        case PropType::Int16:      return 2;
        case PropType::Int32:
        case PropType::Float32:
        case PropType::ErrorCode:  return 4;
        case PropType::Int64:
        case PropType::Float64:
        case PropType::Currency:
        case PropType::AppTime:
        case PropType::SystemTime: return 8;
        case PropType::ClassId:    return 16;
        default:                   return 0;   // variable or unsupported
    }
}

bool isFixedSize(PropType t) noexcept { return fixedSize(t) != 0; }

// Classification per [SPEC §2.3.3.3]. Returns the chosen storage class.
enum class ResolvedStorage : uint8_t { Inline, HnAlloc, Subnode };

ResolvedStorage resolveStorage(PropType        t,
                               size_t          valueSize,
                               PropStorageHint hint) noexcept
{
    if (hint == PropStorageHint::Subnode) return ResolvedStorage::Subnode;

    if (isFixedSize(t)) {
        return fixedSize(t) <= 4 ? ResolvedStorage::Inline
                                 : ResolvedStorage::HnAlloc;
    }
    // Variable-size.
    return valueSize <= kHnAllocMax ? ResolvedStorage::HnAlloc
                                    : ResolvedStorage::Subnode;
}

// Pack a fixed-size, cb≤4 value into a 4-byte little-endian slot.
// Boolean and Int16 zero-extend to 4 bytes; Int32 / Float32 / ErrorCode
// fill all 4 bytes natively.
uint32_t packInlineValue(PropType t,
                         const uint8_t* valueBytes,
                         size_t         valueSize) noexcept
{
    uint8_t buf[4] = {0, 0, 0, 0};
    const size_t fs = fixedSize(t);
    const size_t copyLen = (fs > 0 && fs <= 4) ? fs : (valueSize > 4 ? 4u : valueSize);
    if (valueBytes != nullptr && copyLen > 0) {
        std::memcpy(buf, valueBytes, copyLen);
    }
    return detail::readU32(buf, 0);
}

} // namespace

// ----------------------------------------------------------------------------
// encodeMvUnicode — [MS-PST] §2.3.3.4.2
// ----------------------------------------------------------------------------
vector<uint8_t> encodeMvUnicode(const MvStringEntry* strings,
                                size_t               count)
{
    // Header: ulCount (4 B) + rgulDataOffsets[count] (4 B each)
    const size_t headerSize = 4 + count * 4;

    size_t total = headerSize;
    for (size_t i = 0; i < count; ++i) total += strings[i].utf16leSize;

    vector<uint8_t> out(total);
    detail::writeU32(out.data(), 0, static_cast<uint32_t>(count));

    size_t cursor = headerSize;
    for (size_t i = 0; i < count; ++i) {
        detail::writeU32(out.data(), 4 + i * 4, static_cast<uint32_t>(cursor));
        if (strings[i].utf16leSize > 0) {
            std::memcpy(out.data() + cursor,
                        strings[i].utf16leBytes,
                        strings[i].utf16leSize);
        }
        cursor += strings[i].utf16leSize;
    }
    return out;
}

// ----------------------------------------------------------------------------
// buildPropertyContext
// ----------------------------------------------------------------------------
PcResult buildPropertyContext(const PcProperty* props,
                              size_t            propCount,
                              Nid               firstSubnodeNid)
{
    // ---- Validate ----
    // Subnode NIDs must NOT have nidType == NID_TYPE_HID (= 0), per
    // [MS-PST] §2.3.3.2 — HNID decoding picks the NID branch only when
    // hidType (= low 5 bits of the NID) is non-zero.
    if (firstSubnodeNid.type() == NidType::HID) {
        throw std::invalid_argument(
            "buildPropertyContext: firstSubnodeNid must have nidType != HID");
    }

    // Per-prop dimension validation + duplicate-key detection.
    vector<size_t> sortedIdx(propCount);
    for (size_t i = 0; i < propCount; ++i) sortedIdx[i] = i;
    std::sort(sortedIdx.begin(), sortedIdx.end(),
        [props](size_t a, size_t b) noexcept {
            return props[a].pidTagId < props[b].pidTagId;
        });
    for (size_t i = 1; i < propCount; ++i) {
        if (props[sortedIdx[i]].pidTagId == props[sortedIdx[i - 1]].pidTagId) {
            throw std::invalid_argument(
                "buildPropertyContext: duplicate pidTagId in input");
        }
    }
    for (size_t i = 0; i < propCount; ++i) {
        const PcProperty& p = props[i];
        if (isFixedSize(p.propType)) {
            const size_t fs = fixedSize(p.propType);
            if (p.valueSize != fs) {
                throw std::invalid_argument(
                    "buildPropertyContext: fixed-size value has wrong byte length");
            }
        }
        if (p.storage == PropStorageHint::Subnode && p.valueSize == 0) {
            throw std::invalid_argument(
                "buildPropertyContext: forced-subnode prop must have a value");
        }
    }

    // ---- Resolve storage class for each prop in sorted order ----
    vector<ResolvedStorage> classes(propCount);
    for (size_t i = 0; i < propCount; ++i) {
        const PcProperty& p = props[i];
        classes[i] = resolveStorage(p.propType, p.valueSize, p.storage);
    }

    // ---- Assign HIDs and NIDs ----
    // HID layout:
    //   slot 1 (HID 0x20) = BTHHEADER  -- always
    //   slot 2 (HID 0x40) = BTH leaf   -- always (even with 0 props; that's
    //                                     a 0-byte allocation, still valid)
    //   slot 3+ (HID 0x60, 0x80, ...) = per-prop HN allocations, in
    //                                   sorted-PidTag order
    vector<uint32_t> propHid(propCount, 0u);
    vector<Nid>      propNid(propCount, Nid{});
    uint32_t nextHidIndex = 3;  // 1=header, 2=leaf, 3+=values
    Nid       nextNid       = firstSubnodeNid;

    PcResult out;
    for (size_t s = 0; s < propCount; ++s) {
        const size_t i = sortedIdx[s];
        const PcProperty& p = props[i];
        switch (classes[i]) {
            case ResolvedStorage::Inline:
                break;
            case ResolvedStorage::HnAlloc:
                propHid[i] = makeHid(static_cast<uint16_t>(nextHidIndex));
                ++nextHidIndex;
                break;
            case ResolvedStorage::Subnode:
                propNid[i] = nextNid;
                out.subnodes.push_back({nextNid, p.pidTagId,
                                        p.valueBytes, p.valueSize});
                // Advance counter by 4 (raw NID counter increment — the
                // bottom 5 bits are nidType so monotonic NIDs differ by
                // 32 in the index field, equivalent to +4 in the raw
                // counter for nidType-aligned NIDs... actually +32 is
                // the correct stride if we want the same nidType.)
                nextNid = Nid{nextNid.value + (uint32_t{1} << 5)};
                break;
        }
    }

    // ---- Build the BTH leaf records (one per prop, in sorted order) ----
    constexpr uint8_t kPcCbKey = 2;  // PidTag id (low 16 bits of PidTag is
                                     // PropType — stored in `cbEnt`'s first
                                     // 2 bytes; high 16 bits is the key.)
    constexpr uint8_t kPcCbEnt = 6;  // 2-byte propType + 4-byte dwValueHnid

    // Storage for each record's key (2 B) and data (6 B). We materialize
    // them in sorted order; buildBthLeafRecords will sort again, but the
    // sort is stable on already-sorted input.
    vector<array<uint8_t, kPcCbKey>> keyBuf(propCount);
    vector<array<uint8_t, kPcCbEnt>> dataBuf(propCount);
    vector<BthRecord> bthRecs(propCount);
    for (size_t s = 0; s < propCount; ++s) {
        const size_t i = sortedIdx[s];
        const PcProperty& p = props[i];

        detail::writeU16(keyBuf[s].data(), 0, p.pidTagId);

        // dwValueHnid encoding per resolved storage class.
        uint32_t dwValueHnid = 0;
        switch (classes[i]) {
            case ResolvedStorage::Inline:
                dwValueHnid = packInlineValue(p.propType, p.valueBytes, p.valueSize);
                break;
            case ResolvedStorage::HnAlloc:
                dwValueHnid = propHid[i];
                break;
            case ResolvedStorage::Subnode:
                dwValueHnid = propNid[i].value;
                break;
        }
        detail::writeU16(dataBuf[s].data(), 0, static_cast<uint16_t>(p.propType));
        detail::writeU32(dataBuf[s].data(), 2, dwValueHnid);

        bthRecs[s].keyBytes  = keyBuf[s].data();
        bthRecs[s].dataBytes = dataBuf[s].data();
    }
    const auto bthLeaf = buildBthLeafRecords(bthRecs.data(), bthRecs.size(),
                                             kPcCbKey, kPcCbEnt);

    // ---- BTHHEADER for the PC. hidRoot = HID 0x40 (slot 2). ----
    const auto bthHdr = encodeBthHeader(kPcCbKey, kPcCbEnt,
                                        /*bIdxLevels=*/0,
                                        /*hidRoot=*/makeHid(2));

    // ---- Assemble HN allocations in slot order ----
    // slot 1 = BTHHEADER, slot 2 = BTH leaf, slot 3+ = each HN-allocated
    // prop's value bytes in sorted-PidTag order.
    vector<HnAllocation> allocs;
    allocs.reserve(2 + propCount);
    allocs.push_back({bthHdr.data(),  bthHdr.size()});
    allocs.push_back({bthLeaf.data(), bthLeaf.size()});
    for (size_t s = 0; s < propCount; ++s) {
        const size_t i = sortedIdx[s];
        if (classes[i] == ResolvedStorage::HnAlloc) {
            allocs.push_back({props[i].valueBytes, props[i].valueSize});
        }
    }

    out.hnBytes = buildHeapOnNode(allocs.data(), allocs.size(),
                                  /*bClientSig=*/kBClientSigPC,
                                  /*hidUserRoot=*/makeHid(1));

    if (out.hnBytes.size() > kMaxHnBodyBytes) {
        throw std::length_error(
            "buildPropertyContext: HN body exceeds single-block cap "
            "(8176); split or promote large props to subnodes");
    }

    return out;
}

// ----------------------------------------------------------------------------
// computeTcRgib — TCINFO.rgib[4] formula, derived from TCOLDESCs.
//
// Per [MS-PST] §2.3.4.1 + §2.3.4.4.1, the row-data regions are:
//   [0          .. rgib[TCI_4b])    8/4-byte values   (rgdwData)
//   [rgib[TCI_4b].. rgib[TCI_2b])   2-byte values     (rgwData)
//   [rgib[TCI_2b].. rgib[TCI_1b])   1-byte values     (rgbData)
//   [rgib[TCI_1b].. rgib[TCI_bm])   Cell Existence Block — ceil(cCols/8) bytes
//
// rgib values are END offsets (exclusive). When a region has no
// columns its end equals the previous region's end (no advance).
//
// The CEB cannot be computed from per-column ibData (§3.11's TCOLDESCs
// don't tell us where the CEB lives — only "Boolean iBit=10" doesn't
// imply ibData=0x35 for the CEB byte). We compute rgib[TCI_bm] from
// rgib[TCI_1b] + ceil(cCols/8) per §2.3.4.4.1.
// ----------------------------------------------------------------------------
TcRgib computeTcRgib(const TcColumn* cols, size_t colCount) noexcept
{
    uint16_t end4b = 0;
    uint16_t end2b = 0;
    uint16_t end1b = 0;
    bool any4b = false, any2b = false, any1b = false;

    for (size_t i = 0; i < colCount; ++i) {
        const TcColumn& c = cols[i];
        const uint16_t end = static_cast<uint16_t>(c.ibData + c.cbData);
        if (c.cbData == 4u || c.cbData == 8u) {
            if (!any4b || end > end4b) end4b = end;
            any4b = true;
        } else if (c.cbData == 2u) {
            if (!any2b || end > end2b) end2b = end;
            any2b = true;
        } else if (c.cbData == 1u) {
            if (!any1b || end > end1b) end1b = end;
            any1b = true;
        }
        // cbData not in {1,2,4,8}: caller-side validation in the full
        // builder; the rgib helper just ignores them so it stays
        // noexcept and order-agnostic.
    }

    // Cascade: regions with no columns inherit the previous region's end.
    if (!any2b) end2b = end4b;
    if (!any1b) end1b = end2b;

    const uint16_t cebBytes = static_cast<uint16_t>((colCount + 7u) / 8u);
    const uint16_t endBm    = static_cast<uint16_t>(end1b + cebBytes);

    return {end4b, end2b, end1b, endBm};
}

// ----------------------------------------------------------------------------
// buildTableContext — emit a single-block TC HN.
//
// HN layout (deterministic; reader-side is HID-agnostic per Amendment 1):
//   slot 1 (HID 0x20) = RowIndex BTHHEADER (8 B)
//   slot 2 (HID 0x40) = TCINFO + TCOLDESC array (22 + 8 × cCols B)
//   slot 3 (HID 0x60) = RowIndex BTH leaf records (8 B × rowCount; empty
//                       if rowCount == 0, but the slot still exists)
//   slot 4 (HID 0x80) = Row Matrix (endBm × rowCount bytes)
//   slot 5+ (HID 0xA0, 0xC0, …) = variable-size column values, in
//                       row-major order: row0 cells in input cell order,
//                       then row1 cells, etc. (Pre-registered as the
//                       row-major TC assumption in KNOWN_UNVERIFIED.md.)
// ----------------------------------------------------------------------------
TcResult buildTableContext(const TcColumn* cols, size_t colCount,
                           const TcRow*    rows, size_t rowCount,
                           Nid             firstSubnodeNid)
{
    if (colCount == 0) {
        throw std::invalid_argument("buildTableContext: must have at least one column");
    }

    // Subnode NIDs (when row matrix promotion is enabled) MUST have
    // nidType != HID so HNID decoding picks the NID branch per
    // [MS-PST] §2.3.3.2.
    if (firstSubnodeNid.value != 0u
        && firstSubnodeNid.type() == NidType::HID)
    {
        throw std::invalid_argument(
            "buildTableContext: firstSubnodeNid must have nidType != HID");
    }

    // ---- Validate per-column constraints ----
    for (size_t i = 0; i < colCount; ++i) {
        const TcColumn& c = cols[i];
        if (c.cbData != 1u && c.cbData != 2u && c.cbData != 4u && c.cbData != 8u) {
            throw std::invalid_argument(
                "buildTableContext: cbData must be 1, 2, 4, or 8");
        }
    }

    // ---- Sort columns by tag ascending per [MS-PST] §2.3.4.1 ----
    // tag = (pidTagId << 16) | propType
    vector<size_t> colOrder(colCount);
    for (size_t i = 0; i < colCount; ++i) colOrder[i] = i;
    std::sort(colOrder.begin(), colOrder.end(),
        [cols](size_t a, size_t b) noexcept {
            const uint32_t ta = (static_cast<uint32_t>(cols[a].pidTagId) << 16)
                              | static_cast<uint32_t>(cols[a].propType);
            const uint32_t tb = (static_cast<uint32_t>(cols[b].pidTagId) << 16)
                              | static_cast<uint32_t>(cols[b].propType);
            return ta < tb;
        });

    // ---- Compute rgib (uses original-order column views; result is
    // order-agnostic so we don't need to sort first). ----
    const TcRgib rg = computeTcRgib(cols, colCount);

    // ---- Validate rows ----
    for (size_t i = 0; i < rowCount; ++i) {
        if (rows[i].rowSize != rg.endBm) {
            throw std::invalid_argument(
                "buildTableContext: row size doesn't match computed endBm");
        }
        if (rows[i].rowBytes == nullptr && rg.endBm > 0) {
            throw std::invalid_argument(
                "buildTableContext: rowBytes must not be null");
        }
        for (size_t j = 0; j < rows[i].varlenCount; ++j) {
            const auto& v = rows[i].varlenCells[j];
            if (v.colIndex >= colCount) {
                throw std::invalid_argument(
                    "buildTableContext: varlen cell colIndex out of range");
            }
            if (cols[v.colIndex].cbData != 4u) {
                throw std::invalid_argument(
                    "buildTableContext: varlen cell column must have cbData == 4");
            }
        }
    }

    // ---- Sort rows by rowId ascending (RowIndex BTH key invariant) ----
    vector<size_t> rowOrder(rowCount);
    for (size_t i = 0; i < rowCount; ++i) rowOrder[i] = i;
    std::sort(rowOrder.begin(), rowOrder.end(),
        [rows](size_t a, size_t b) noexcept {
            return rows[a].rowId < rows[b].rowId;
        });

    // ---- Build RowIndex BTHHEADER (cbKey=4 NID, cbEnt=4 row#, hidRoot=HID 0x60) ----
    const auto rowIdxHdr = encodeBthHeader(/*cbKey=*/4, /*cbEnt=*/4,
                                           /*bIdxLevels=*/0,
                                           /*hidRoot=*/rowCount > 0u ? makeHid(3) : 0u);

    // ---- Build RowIndex BTH leaf records ----
    // 8 bytes each: 4-byte rowId key + 4-byte zero-based row index.
    vector<uint8_t> rowIdxLeaf(rowCount * 8u);
    if (rowCount > 0u) {
        // We sorted rowOrder by rowId ascending; row i in the sorted order
        // gets row index i in the matrix. The BTH key→data mapping says
        // "rowId X is at row position rowOrder^-1(X)"; since we lay out the
        // matrix in sorted order, sorted position s maps to matrix index s.
        for (size_t s = 0; s < rowCount; ++s) {
            const size_t srcIdx = rowOrder[s];
            detail::writeU32(rowIdxLeaf.data(), s * 8u + 0, rows[srcIdx].rowId);
            detail::writeU32(rowIdxLeaf.data(), s * 8u + 4, static_cast<uint32_t>(s));
        }
    }

    // ---- Compute Row Matrix size + collect varlen-payload pointers ----
    // Row Matrix is `rowCount × endBm` bytes. Varlen HID-slot patching
    // is deferred until after the promotion decision — slot numbering
    // depends on whether the row matrix occupies HN slot 4 (no
    // promotion) or moves to a subnode (promoted).
    const size_t rowMatrixSize = rowCount * rg.endBm;
    vector<uint8_t> rowMatrix(rowMatrixSize);

    struct VarlenAlloc {
        size_t         rowMatrixOffset;  // where to patch the HID
        const uint8_t* data;
        size_t         size;
    };
    vector<VarlenAlloc> varlenAllocs;

    size_t totalVarlenBytes = 0;
    for (size_t s = 0; s < rowCount; ++s) {
        const size_t srcIdx = rowOrder[s];
        const TcRow& r = rows[srcIdx];

        // Copy the caller's row bytes (varlen HID-slot patches happen below).
        if (rg.endBm > 0u) {
            std::memcpy(rowMatrix.data() + s * rg.endBm, r.rowBytes, rg.endBm);
        }
        for (size_t j = 0; j < r.varlenCount; ++j) {
            const TcVarlenCell& v = r.varlenCells[j];
            const TcColumn& c = cols[v.colIndex];
            VarlenAlloc va;
            va.rowMatrixOffset = s * rg.endBm + c.ibData;
            va.data            = v.bytes;
            va.size            = v.size;
            varlenAllocs.push_back(va);
            totalVarlenBytes += v.size;
        }
    }

    // ---- Decide whether to promote the row matrix to a subnode ----
    //
    // HN body size estimate (for the no-promotion layout):
    //   HNHDR (kHnHdrSize) + sum(alloc sizes) + DWORD-align pad
    //                       + HNPAGEMAP (4 + 2*(allocCount+1) bytes)
    //
    // Rather than build the HN twice on overflow, we compute the
    // estimate up front and choose layout. When the estimate exceeds
    // kMaxHnBodyBytes AND firstSubnodeNid is valid, we promote: the
    // row matrix moves out of the HN into a single subnode payload
    // (caller handles further chunking via XBLOCK if rowMatrixSize
    // itself exceeds the per-block cap).
    const size_t tcInfoSize = 22u + 8u * colCount;
    auto estimateHnSize = [&](bool promoted) noexcept -> size_t {
        size_t total = 8u                       // RowIndex BTHHEADER
                     + tcInfoSize               // TCINFO + TCOLDESC
                     + rowCount * 8u            // RowIndex BTH leaf
                     + totalVarlenBytes;        // varlen payloads
        size_t allocCount = 3u + varlenAllocs.size();
        if (!promoted) {
            total      += rowMatrixSize;        // row matrix inline
            allocCount += 1u;
        }
        const size_t cursorRaw     = kHnHdrSize + total;
        const size_t cursorAligned = (cursorRaw + 3u) & ~size_t{3u};
        // M11-H: a phantom allocation absorbs the alignment pad when
        // cumulative cursor is not DWORD-aligned, adding one more
        // rgibAlloc entry (2 bytes) to HNPAGEMAP.
        if (cursorRaw != cursorAligned) {
            allocCount += 1u;
        }
        const size_t hnpmSize = 4u + 2u * (allocCount + 1u);
        return cursorAligned + hnpmSize;
    };

    const size_t estIfInline = estimateHnSize(/*promoted=*/false);
    bool promote = false;
    if (estIfInline > kMaxHnBodyBytes && rowCount > 0u) {
        if (firstSubnodeNid.value == 0u) {
            throw std::length_error(
                "buildTableContext: HN body exceeds single-block cap (8176); "
                "pass firstSubnodeNid (with nidType != HID) to enable "
                "row-matrix-as-subnode promotion per [MS-PST] §2.3.4.4.1");
        }
        const size_t estIfPromoted = estimateHnSize(/*promoted=*/true);
        if (estIfPromoted > kMaxHnBodyBytes) {
            throw std::length_error(
                "buildTableContext: HN body exceeds single-block cap (8176) "
                "even after row-matrix promotion — varlen payloads in the "
                "parent HN are themselves too large");
        }
        promote = true;
    }

    // ---- Patch row-matrix varlen HIDs with the right slot start ----
    // Without promotion: row matrix is HN slot 4, varlens start at 5.
    // With promotion: row matrix moves to a subnode, varlens shift to 4.
    {
        uint16_t nextVarlenSlot = static_cast<uint16_t>(promote ? 4u : 5u);
        for (auto& v : varlenAllocs) {
            const uint32_t hid = makeHid(nextVarlenSlot++);
            detail::writeU32(rowMatrix.data() + v.rowMatrixOffset, 0, hid);
        }
    }

    // ---- Build TCINFO + TCOLDESC array ----
    vector<uint8_t> tcInfo(tcInfoSize);
    detail::writeU8 (tcInfo.data(),  0, kTcSignature);
    detail::writeU8 (tcInfo.data(),  1, static_cast<uint8_t>(colCount));
    detail::writeU16(tcInfo.data(),  2, rg.end4b);
    detail::writeU16(tcInfo.data(),  4, rg.end2b);
    detail::writeU16(tcInfo.data(),  6, rg.end1b);
    detail::writeU16(tcInfo.data(),  8, rg.endBm);
    // hidRowIndex = HID 0x20 (RowIndex BTHHEADER, slot 1)
    detail::writeU32(tcInfo.data(), 10, makeHid(1));
    // hnidRows: per [MS-PST] §2.3.4.1
    //   * 0          if rowCount == 0
    //   * HID 0x80   if row matrix lives at HN slot 4 (no promotion)
    //   * NID        if promoted to a subnode (HNID NID branch)
    uint32_t hnidRowsValue = 0u;
    if (rowCount > 0u) {
        hnidRowsValue = promote ? firstSubnodeNid.value : makeHid(4);
    }
    detail::writeU32(tcInfo.data(), 14, hnidRowsValue);
    // hidIndex = 0 (deprecated; creators MUST set to zero)
    detail::writeU32(tcInfo.data(), 18, 0u);

    // TCOLDESC array, sorted by tag.
    for (size_t s = 0; s < colCount; ++s) {
        const TcColumn& c = cols[colOrder[s]];
        uint8_t* dst = tcInfo.data() + 22u + s * 8u;
        const uint32_t tag = (static_cast<uint32_t>(c.pidTagId) << 16)
                           | static_cast<uint32_t>(c.propType);
        detail::writeU32(dst, 0, tag);
        detail::writeU16(dst, 4, c.ibData);
        detail::writeU8 (dst, 6, c.cbData);
        detail::writeU8 (dst, 7, c.iBit);
    }

    // ---- Assemble HN allocations in slot order ----
    vector<HnAllocation> allocs;
    allocs.reserve(4u + varlenAllocs.size());
    allocs.push_back({rowIdxHdr.data(), rowIdxHdr.size()});       // slot 1 — HID 0x20
    allocs.push_back({tcInfo.data(),    tcInfo.size()});          // slot 2 — HID 0x40
    allocs.push_back({rowIdxLeaf.data(),rowIdxLeaf.size()});      // slot 3 — HID 0x60
    if (!promote) {
        allocs.push_back({rowMatrix.data(), rowMatrix.size()});   // slot 4 — HID 0x80
    }
    for (const auto& v : varlenAllocs) {
        allocs.push_back({v.data, v.size});                       // slots 4+/5+
    }

    TcResult out;
    out.hnBytes = buildHeapOnNode(allocs.data(), allocs.size(),
                                  /*bClientSig=*/kBClientSigTC,
                                  /*hidUserRoot=*/makeHid(2));    // TCINFO at HID 0x40

    if (out.hnBytes.size() > kMaxHnBodyBytes) {
        // Defensive: estimateHnSize should match buildHeapOnNode bytes
        // exactly; throwing here means the estimator drifted from the
        // builder. Catch it loudly rather than ship a corrupt HN.
        throw std::length_error(
            "buildTableContext: HN body exceeds single-block cap (8176) "
            "after layout decision — internal estimator drift");
    }

    if (promote) {
        // Caller schedules these bytes through its data-block layer
        // (XBLOCK chained when rowMatrixSize > kMaxBlockPayload) and
        // emplaces a SLENTRY {firstSubnodeNid, bidData, 0}.
        // We move rowMatrix into out.subnodes' returned vector so the
        // pointer/size pair stays valid for the caller; we wrap it in
        // a small heap-owning shim by leveraging PcSubnodeOut's owned
        // bytes contract — but PcSubnodeOut takes raw data + size, so
        // we must keep `rowMatrix` alive. Move into a member of
        // TcResult instead.
        //
        // (We extend TcResult here to carry owned subnode bytes;
        // see ltp.hpp.)
        out.subnodes.push_back({firstSubnodeNid, /*pidTagId=*/0u,
                                /*data=*/nullptr, /*size=*/0u});
        out.ownedSubnodeBytes.push_back(std::move(rowMatrix));
        out.subnodes.back().data = out.ownedSubnodeBytes.back().data();
        out.subnodes.back().size = out.ownedSubnodeBytes.back().size();
    }

    return out;
}

// ----------------------------------------------------------------------------
// readPropertyContext — HID-agnostic PC decoder
// ----------------------------------------------------------------------------
//
// Walks: HNHDR → HNPAGEMAP → BTHHEADER → leaf-records → resolve each HNID.
// Spec-invariant-only: never assumes anything about the writer's HID
// layout. The §3.9 cross-validation test exercises this on real
// Outlook-extracted bytes (see MILESTONES.md "M4 Part 2 — HID order
// contract" for why this matters).
// ----------------------------------------------------------------------------
namespace {

// Resolve an HID against a single-block HN body. Returns the byte
// range [start, end) within hnBytes, or throws on any violation of
// [MS-PST] §2.3.1.1 invariants.
struct HnSlice {
    size_t off;     // offset within hnBytes
    size_t size;    // bytes
};

HnSlice resolveHidInSingleBlockHn(uint32_t       hid,
                                  const uint8_t* hnBytes,
                                  size_t         hnSize,
                                  size_t         hnpmOff,
                                  uint16_t       cAlloc)
{
    const uint8_t  hType = static_cast<uint8_t> (hid & 0x1Fu);
    const uint16_t hIdx  = static_cast<uint16_t>((hid >> 5) & 0x07FFu);
    const uint16_t hBlk  = static_cast<uint16_t>((hid >> 16) & 0xFFFFu);

    if (hType != 0u) {
        throw std::runtime_error(
            "readPropertyContext: HID has nidType != NID_TYPE_HID");
    }
    if (hIdx == 0u) {
        throw std::runtime_error(
            "readPropertyContext: HID has hidIndex == 0 (illegal per §2.3.1.1)");
    }
    if (hBlk != 0u) {
        throw std::runtime_error(
            "readPropertyContext: HID has hidBlockIndex != 0; multi-block "
            "HN reads not supported in M4");
    }
    if (hIdx > cAlloc) {
        throw std::runtime_error(
            "readPropertyContext: HID hidIndex exceeds cAlloc");
    }

    // rgibAlloc[hidIndex - 1] is the start; rgibAlloc[hidIndex] is the end
    // (which is rgibAlloc[hidIndex - 1] + size of allocation hidIndex).
    const size_t startOff = hnpmOff + 4u + (static_cast<size_t>(hIdx) - 1u) * 2u;
    const size_t endOff   = hnpmOff + 4u +  static_cast<size_t>(hIdx)        * 2u;
    if (endOff + 2u > hnSize) {
        throw std::runtime_error(
            "readPropertyContext: HNPAGEMAP rgibAlloc entry out of range");
    }
    const uint16_t s = detail::readU16(hnBytes, startOff);
    const uint16_t e = detail::readU16(hnBytes, endOff);
    if (e < s || e > hnSize) {
        throw std::runtime_error(
            "readPropertyContext: HN allocation bounds invalid");
    }
    return {s, static_cast<size_t>(e) - s};
}

} // namespace

vector<ReadPcProp> readPropertyContext(const uint8_t* hnBytes, size_t hnSize)
{
    if (hnSize < kHnHdrSize) {
        throw std::runtime_error("readPropertyContext: HN body too small for HNHDR");
    }

    // ---- HNHDR ----
    const uint16_t ibHnpm   = detail::readU16(hnBytes, 0);
    const uint8_t  bSig     = hnBytes[2];
    const uint8_t  bClient  = hnBytes[3];
    const uint32_t hidUserRoot = detail::readU32(hnBytes, 4);

    if (bSig != kHnSignature) {
        throw std::runtime_error(
            "readPropertyContext: HNHDR.bSig != 0xEC");
    }
    if (bClient != kBClientSigPC) {
        throw std::runtime_error(
            "readPropertyContext: bClientSig != 0xBC (not a PC)");
    }
    if (ibHnpm + kHnPageMapHdrSize > hnSize) {
        throw std::runtime_error(
            "readPropertyContext: ibHnpm out of range");
    }

    // ---- HNPAGEMAP ----
    const uint16_t cAlloc = detail::readU16(hnBytes, ibHnpm + 0);
    const size_t   pmEnd  = ibHnpm + kHnPageMapHdrSize
                          + (static_cast<size_t>(cAlloc) + 1u) * kHnPageMapEntrySize;
    if (pmEnd > hnSize) {
        throw std::runtime_error(
            "readPropertyContext: HNPAGEMAP extends past HN end");
    }

    // ---- BTHHEADER (resolved via hidUserRoot) ----
    const HnSlice bthHdrSlice =
        resolveHidInSingleBlockHn(hidUserRoot, hnBytes, hnSize, ibHnpm, cAlloc);
    if (bthHdrSlice.size != 8u) {
        throw std::runtime_error(
            "readPropertyContext: BTHHEADER allocation has wrong size (expected 8 B)");
    }

    const uint8_t  bthType   = hnBytes[bthHdrSlice.off + 0];
    const uint8_t  cbKey     = hnBytes[bthHdrSlice.off + 1];
    const uint8_t  cbEnt     = hnBytes[bthHdrSlice.off + 2];
    const uint8_t  idxLevels = hnBytes[bthHdrSlice.off + 3];
    const uint32_t hidRoot   = detail::readU32(hnBytes, bthHdrSlice.off + 4);

    if (bthType != kBthSignature) {
        throw std::runtime_error(
            "readPropertyContext: BTHHEADER.bType != 0xB5");
    }
    if (cbKey != 2u) {
        throw std::runtime_error(
            "readPropertyContext: PC requires cbKey == 2");
    }
    if (cbEnt != 6u) {
        throw std::runtime_error(
            "readPropertyContext: PC requires cbEnt == 6");
    }
    if (idxLevels != 0u) {
        throw std::runtime_error(
            "readPropertyContext: bIdxLevels > 0 (multi-level BTH not supported in M4)");
    }

    // ---- BTH leaf records ----
    // Empty PC is legal (hidRoot = 0 represents an empty BTH). We treat
    // hidIndex == 0 as a sentinel for "no leaf"; only resolve if non-zero.
    vector<ReadPcProp> out;
    if (((hidRoot >> 5) & 0x07FFu) == 0u) {
        return out;  // empty PC
    }

    const HnSlice leafSlice =
        resolveHidInSingleBlockHn(hidRoot, hnBytes, hnSize, ibHnpm, cAlloc);
    if (leafSlice.size % 8u != 0u) {
        throw std::runtime_error(
            "readPropertyContext: BTH leaf size not divisible by record size (8)");
    }
    const size_t recordCount = leafSlice.size / 8u;
    out.reserve(recordCount);

    for (size_t i = 0; i < recordCount; ++i) {
        const size_t recOff = leafSlice.off + i * 8u;

        ReadPcProp p{};
        p.pidTagId    = detail::readU16(hnBytes, recOff + 0);
        p.propType    = static_cast<PropType>(detail::readU16(hnBytes, recOff + 2));
        const uint32_t dwValueHnid = detail::readU32(hnBytes, recOff + 4);

        // Storage classification per [MS-PST] §2.3.3.3 — derived from
        // propType, NOT from the bit pattern of dwValueHnid:
        //   * Fixed-size with cb ≤ 4: Inline (dwValueHnid IS the value).
        //   * Otherwise: dwValueHnid is an HNID. Discriminate per §2.3.3.2:
        //       low 5 bits == 0 (NID_TYPE_HID) → HID, resolve to HN slot
        //       low 5 bits != 0                → NID, caller resolves
        if (isFixedSize(p.propType) && fixedSize(p.propType) <= 4u) {
            p.storage     = ReadPcProp::Storage::Inline;
            p.inlineValue = dwValueHnid;
        } else if ((dwValueHnid & 0x1Fu) == 0u) {
            const HnSlice s = resolveHidInSingleBlockHn(
                dwValueHnid, hnBytes, hnSize, ibHnpm, cAlloc);
            p.storage    = ReadPcProp::Storage::HnAlloc;
            p.valueBytes = hnBytes + s.off;
            p.valueSize  = s.size;
        } else {
            p.storage    = ReadPcProp::Storage::Subnode;
            p.subnodeNid = Nid{dwValueHnid};
        }
        out.push_back(p);
    }
    return out;
}

} // namespace pstwriter
