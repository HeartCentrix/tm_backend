// pstwriter/ltp.hpp
//
// LTP layer ([MS-PST] §2.3) — Heap-on-Node, BTH, PropertyContext,
// TableContext.
//
// M4 first cut delivers `buildHeapOnNode(...)` — the foundation that
// BTH/PC/TC all sit on. Higher-level builders are layered on top.
//
// This header is the M4 API surface; src/ltp.cpp holds the
// implementations.

#pragma once

#include "types.hpp"

#include <array>
#include <cstdint>
#include <vector>

using namespace std;

namespace pstwriter {

// ============================================================================
// HID encoding ([MS-PST] §2.3.1.1) — 32 bits packed:
//   bits  0..4   hidType         (0 for HN-resident HIDs; non-zero indicates
//                                  HNID variants like NID-as-subnode)
//   bits  5..15  hidIndex        1-based index into HNPAGEMAP.rgibAlloc[]
//   bits 16..31  hidBlockIndex   0 for first HN block, 1..N for subsequent
//
// Helpers: build a HID from its parts and decode.
// ============================================================================
constexpr uint32_t makeHid(uint16_t hidIndex1Based,
                           uint16_t hidBlockIndex = 0,
                           uint8_t  hidType       = 0) noexcept
{
    return static_cast<uint32_t>(hidType & 0x1Fu)
         | (static_cast<uint32_t>(hidIndex1Based & 0x07FFu) << 5)
         | (static_cast<uint32_t>(hidBlockIndex) << 16);
}

constexpr uint8_t  hidType (uint32_t hid) noexcept { return static_cast<uint8_t>(hid & 0x1Fu); }
constexpr uint16_t hidIndex(uint32_t hid) noexcept { return static_cast<uint16_t>((hid >> 5) & 0x07FFu); }
constexpr uint16_t hidBlock(uint32_t hid) noexcept { return static_cast<uint16_t>((hid >> 16) & 0xFFFFu); }

// ============================================================================
// HN block constants ([MS-PST] §2.3.1)
// ============================================================================
constexpr uint8_t  kHnSignature  = 0xEC;  // bSig
constexpr uint8_t  kBthSignature = 0xB5;  // BTHHEADER.bType
constexpr uint8_t  kTcSignature  = 0x7C;  // TCINFO.bType

// bClientSig values ([MS-PST] §2.3.1.2)
constexpr uint8_t  kBClientSigPC = 0xBC;  // bTypePC — Property Context
constexpr uint8_t  kBClientSigTC = 0x7C;  // bTypeTC — Table Context

constexpr size_t   kHnHdrSize        = 12;   // HNHDR
constexpr size_t   kHnPageMapHdrSize = 4;    // cAlloc + cFree
constexpr size_t   kHnPageMapEntrySize = 2;  // each rgibAlloc[i]

// ============================================================================
// HnAllocation — opaque view onto one user allocation.
//
// Lifetime: the bytes pointed to must outlive the buildHeapOnNode call.
// ============================================================================
struct HnAllocation {
    const uint8_t* data;
    size_t         size;
};

// ============================================================================
// buildHeapOnNode — emit the structured body of a single-block HN.
//
// Layout produced (matches [MS-PST] §2.3.1):
//   [0          .. 11]            HNHDR (ibHnpm, bSig=0xEC, bClientSig,
//                                  hidUserRoot, rgbFillLevel)
//   [12         .. 12+sumSizes-1] user allocations laid out contiguously
//                                  in the order given (no per-allocation
//                                  alignment — the spec doesn't require it
//                                  and the §3.8 sample shows variable-sized
//                                  records packed tight)
//   [ibHnpm     .. end-1]         HNPAGEMAP (cAlloc, cFree, rgibAlloc[N+1])
//
// `bClientSig` distinguishes PC (0xBC) / TC (0x7C) / NameID (0x7F) /
// Subnode (0x80) HNs.
//
// `hidUserRoot` is the HID of the allocation that holds the client's
// metadata (BTHHEADER for PC/TC RowIndex, TCINFO for TC). Caller is
// responsible for picking the right value from the allocation list:
//   makeHid(1) = HID 0x20  → first allocation
//   makeHid(2) = HID 0x40  → second
//   ...
//
// `rgbFillLevel` defaults to all zero. The §3.8 / §3.11 samples show
// {0,0,0,0} even though the blocks have lots of free space — Outlook's
// fill-level updates appear to lag actual occupancy. We accept the
// default and let M5+ override if a real Outlook strategy emerges.
//
// Returns the structured HN body. Caller wraps it in a data block via
// buildDataBlock(...) to produce the on-disk form (encrypts, appends
// BLOCKTRAILER, 64-byte aligns).
vector<uint8_t> buildHeapOnNode(const HnAllocation* allocs,
                                size_t              allocCount,
                                uint8_t             bClientSig,
                                uint32_t            hidUserRoot,
                                array<uint8_t, 4>   rgbFillLevel = {{0, 0, 0, 0}}) noexcept;

// ============================================================================
// Convenience constructors for the structured fields callers will need
// to build before passing them as HN allocations.
// ============================================================================

// Encode an 8-byte BTHHEADER ([MS-PST] §2.3.2.1).
//   cbKey       in {1, 2, 4, 8, 16}
//   cbEnt       in {1..32}
//   bIdxLevels  0 for leaf-only BTH; 1..6 for intermediate index levels
//   hidRoot     HID of the first BTH page (intermediate or leaf) inside
//               the parent HN
array<uint8_t, 8> encodeBthHeader(uint8_t  cbKey,
                                  uint8_t  cbEnt,
                                  uint8_t  bIdxLevels,
                                  uint32_t hidRoot) noexcept;

// ============================================================================
// BTH leaf-records builder ([MS-PST] §2.3.2.3)
//
// Packs an arbitrary-order array of (key, data) records into a single
// HN allocation containing the sorted leaf bytes. Sort order is
// "key as unsigned little-endian integer of width cbKey, ascending" —
// matches the BTH binary-search invariant.
//
// Caller passes records in any order; builder sorts internally. This is
// the contract pinned in MILESTONES.md "BTH leaf-record builder":
// callers should not pre-sort.
//
// Output size = recordCount * (cbKey + cbEnt) bytes; tight packing,
// no inter-record padding.
//
// Constraints (enforced):
//   * cbKey in {1, 2, 4, 8, 16}
//   * cbEnt in {1..32}
//   * recordCount * (cbKey + cbEnt) <= kHnAllocMax (3580 B per [MS-PST]
//     §2.3.3.3) — leaf-only BTH (M4 cut) MUST fit in one HN allocation.
//
// On any constraint violation we throw `std::length_error` (or for the
// type/size validation, `std::invalid_argument`). Silent truncation is
// explicitly NOT a failure mode.
// ============================================================================
constexpr size_t kHnAllocMax = 3580;  // [MS-PST] §2.3.3.3 — variable-size HN cap

struct BthRecord {
    const uint8_t* keyBytes;   // exactly cbKey bytes (no null terminator)
    const uint8_t* dataBytes;  // exactly cbEnt bytes
};

vector<uint8_t> buildBthLeafRecords(const BthRecord* records,
                                    size_t           recordCount,
                                    uint8_t          cbKey,
                                    uint8_t          cbEnt);

// ============================================================================
// PropertyContext (PC) writer  ([MS-PST] §2.3.3)
//
// A PC is an HN with bClientSig=0xBC, hosting a BTH whose records are
// 8-byte (PidTag id, PropType + dwValueHnid) tuples per §2.3.3.3.
// Storage decision (inline / HN / subnode) follows the §2.3.3.3
// 4-row truth table verbatim — see MILESTONES.md "M4 Part 2" for the
// citation breakdown.
//
// Writer contract (per MILESTONES.md Amendment 1):
//   * Deterministic: same logical input ⇒ byte-identical output.
//   * HID slots 0x20 / 0x40 / 0x60+ assigned in PidTag-ascending order.
//   * Build-from-scratch only — no in-place edits.
// Reader contract (`readPropertyContext`, future Phase C): HID-driven,
// makes no order assumptions.
// ============================================================================
constexpr size_t kMaxHnBodyBytes = kMaxBlockPayload;  // 8176 — single-block HN cap

enum class PropStorageHint : uint8_t {
    Auto,    // builder picks per [SPEC §2.3.3.3]
    Subnode  // caller forces subnode (escape hatch — DESIGN row of the table)
};

struct PcProperty {
    uint16_t        pidTagId;          // upper 16 bits of PidTag (BTH key)
    PropType        propType;          // lower 16 bits
    const uint8_t*  valueBytes;        // value payload — NOT owned
    size_t          valueSize;         // bytes in valueBytes
    PropStorageHint storage = PropStorageHint::Auto;
};

// Subnode allocations the caller must materialize as separate blocks.
// `data`/`size` are caller-owned (point back at the original PcProperty);
// `nid` is assigned by the builder as `firstSubnodeNid + 4 * i`.
struct PcSubnodeOut {
    Nid            nid;
    uint16_t       pidTagId;
    const uint8_t* data;
    size_t         size;
};

struct PcResult {
    vector<uint8_t>      hnBytes;     // ready to wrap with buildDataBlock
    vector<PcSubnodeOut> subnodes;
};

// Build the HN body for a Property Context.
//
// `firstSubnodeNid` is the starting NID for builder-allocated subnodes.
// nidType MUST be non-zero (≠ NID_TYPE_HID = 0) so HNID decoding picks
// the NID branch per [MS-PST] §2.3.3.2. Each promoted prop receives
// firstSubnodeNid, firstSubnodeNid+4, ... in sorted-PidTag order.
//
// Throws:
//   * std::invalid_argument — duplicate pidTagId, fixed-size mismatch,
//     PropStorageHint::Subnode with valueSize == 0, NID_TYPE_HID
//     starting NID, etc.
//   * std::length_error — total HN body would exceed kMaxHnBodyBytes
//     (8176, single-block HN cap from [MS-PST] §1.3.2.5 / §2.2.2.8.3.1).
PcResult buildPropertyContext(const PcProperty* props,
                              size_t            propCount,
                              Nid               firstSubnodeNid);

// ============================================================================
// PropertyContext (PC) reader  ([MS-PST] §2.3.3)
//
// HID-AGNOSTIC by contract (per MILESTONES.md Amendment 1): walks the
// HNHDR → HNPAGEMAP → BTHHEADER → leaf-records chain and resolves each
// record's HNID directly. Makes NO assumption about HID order,
// contiguity, or which slot contains what. Real Outlook-produced PCs
// (e.g. [MS-PST] §3.9) have arbitrary HID layouts because Outlook
// edits in place; the §3.9 cross-validation test in test_ltp.cpp
// is the regression oracle for this.
//
// Spec invariants enforced (throws std::runtime_error on violation):
//   * HNHDR.bSig          == 0xEC                  (§2.3.1.2)
//   * BTHHEADER.bType     == 0xB5                  (§2.3.2.1)
//   * BTHHEADER.cbKey     == 2                     (PC: PidTag id)
//   * BTHHEADER.cbEnt     == 6                     (PC: 2-B type + 4-B value)
//   * BTHHEADER.bIdxLevels == 0                    (M4: leaf-only)
//   * Each HID's hidIndex is in [1..cAlloc]        (§2.3.1.1)
//   * Each HID's hidBlockIndex == 0                (M4: single-block HN)
//
// Spec invariants NOT enforced (caller's responsibility / not in scope):
//   * HID order, contiguity, density across slots
//   * Specific value byte content (caller decodes per propType)
//   * Subnode block presence — readPropertyContext returns the NID;
//     caller looks the subnode up via NBT/BBT and reads the block
// ============================================================================
struct ReadPcProp {
    enum class Storage : uint8_t { Inline, HnAlloc, Subnode };

    uint16_t       pidTagId;
    PropType       propType;
    Storage        storage;

    // Inline values: 4-byte raw dwValueHnid slot (caller decodes per
    // propType — Boolean reads byte 0, Int16 reads bytes 0..1, etc.)
    uint32_t       inlineValue;

    // HN-allocated values: bytes inside the source hnBytes buffer.
    // valueBytes is null and valueSize is 0 for non-HnAlloc storage.
    const uint8_t* valueBytes;
    size_t         valueSize;

    // Subnode-promoted values: NID the caller must resolve via NBT.
    // valueBytes/valueSize are 0 for Subnode storage.
    Nid            subnodeNid;
};

// Decode a PC HN body into its property records. Returns props in
// BTH-leaf order — which is BTH-key (PidTag id) ascending per the
// spec's BTH invariant, NOT writer-defined.
//
// `hnBytes`/`hnSize` must outlive the returned ReadPcProp::valueBytes
// pointers (they alias into hnBytes for HnAlloc props).
vector<ReadPcProp> readPropertyContext(const uint8_t* hnBytes, size_t hnSize);

// ============================================================================
// TableContext (TC) writer  ([MS-PST] §2.3.4)
//
// A TC is an HN with bClientSig=0x7C, hosting:
//   * TCINFO (header) — at hidUserRoot, contains rgib[4] + RowIndex/Row
//     Matrix HIDs + sorted TCOLDESC array. ([MS-PST] §2.3.4.1)
//   * RowIndex BTH (cbKey=4 NID, cbEnt=4 row#) — keyed by row NID,
//     value = 0-based position in Row Matrix. ([MS-PST] §2.3.4.3)
//   * Row Matrix — contiguous rows, each TCI_bm bytes, structured as
//     four regions: 4/8-byte / 2-byte / 1-byte / CEB. ([MS-PST] §2.3.4.4)
//   * Variable-size column values — one HN allocation per (row, varlen-col)
//     pair, in **row-major order** per KNOWN_UNVERIFIED.md TC entry.
//
// Caller responsibilities:
//   * Pre-pack each row's TCI_bm bytes per the column descriptors
//     (we don't compose row bytes from per-column values in M4).
//   * Supply TCOLDESCs whose ibData/cbData honor §2.3.4.4.1 constraints
//     (PidTagLtpRowId at iBit=0/ibData=0, PidTagLtpRowVer at iBit=1/ibData=4).
// ============================================================================
struct TcColumn {
    uint16_t pidTagId;   // upper 16 bits of PidTag
    PropType propType;   // lower 16 bits of PidTag
    uint16_t ibData;     // byte offset within row data
    uint8_t  cbData;     // 1, 2, 4, or 8 (HID values use cbData=4 per §2.3.4.2)
    uint8_t  iBit;       // 0-based CEB index
};

// One variable-size cell value to splice into a row.
//   colIndex    — index into the TcColumn[] array supplied to
//                 buildTableContext. The column at this index MUST
//                 have cbData == 4 (HID slot) and a variable-size
//                 propType (Unicode/String8/Binary/Mv*).
//   bytes/size  — the value payload; the writer allocates an HN slot,
//                 copies the bytes, and patches the row's 4-byte cell
//                 at columns[colIndex].ibData with the assigned HID.
struct TcVarlenCell {
    size_t         colIndex;
    const uint8_t* bytes;
    size_t         size;
};

struct TcRow {
    uint32_t              rowId;        // dwRowID (= PidTagLtpRowId)
    const uint8_t*        rowBytes;     // exactly endBm bytes — caller writes
                                        // fixed values; varlen-cell HID slots
                                        // are placeholders the writer overwrites.
    size_t                rowSize;      // bytes in rowBytes (= computed endBm)
    const TcVarlenCell*   varlenCells;  // may be nullptr if varlenCount == 0
    size_t                varlenCount;
};

// rgib[4] in TCINFO: derived from TCOLDESCs per [MS-PST] §2.3.4.1 +
// §2.3.4.4.1. Each entry is the END offset of one row-data region:
//   end4b = max(ibData+cbData) over cbData ∈ {4, 8}; if none, 0
//   end2b = max(ibData+cbData) over cbData == 2;   if none, end4b
//   end1b = max(ibData+cbData) over cbData == 1;   if none, end2b
//   endBm = end1b + ceil(cCols / 8)
struct TcRgib {
    uint16_t end4b;
    uint16_t end2b;
    uint16_t end1b;
    uint16_t endBm;
};

// Standalone rgib formula. Exposed for testing in isolation against
// the [MS-PST] §3.11 oracle BEFORE the rest of buildTableContext lands.
// Caller does NOT need to pre-sort columns; the formula is order-agnostic.
TcRgib computeTcRgib(const TcColumn* cols, size_t colCount) noexcept;

struct TcResult {
    vector<uint8_t>      hnBytes;
    vector<PcSubnodeOut> subnodes;  // for future row-matrix-as-subnode promotion
};

// Build a TC's HN body. M4 cut: single-block HN, no subnodes for
// either Row Matrix or variable-size column values (those go in HN
// allocations).
//
// Throws:
//   * std::invalid_argument — cbData not in {1,2,4,8}; iBit > 7*cCols;
//     §2.3.4.4.1 requires PidTagLtpRowId at iBit=0/ibData=0 and
//     PidTagLtpRowVer at iBit=1/ibData=4 (we don't enforce this,
//     trust the caller); rowBytes null
//   * std::length_error — HN body would exceed kMaxHnBodyBytes
TcResult buildTableContext(const TcColumn* cols, size_t colCount,
                           const TcRow*    rows, size_t rowCount);

// Convenience: encode a PT_MV_UNICODE value per [MS-PST] §2.3.3.4.2.
// Layout:
//   ulCount (4 B LE) + rgulDataOffsets[ulCount] (4 B LE each, byte offsets
//   from the start of the structure) + each string's UTF-16-LE bytes.
//
// `strings` holds the UTF-16-LE bytes already encoded; the helper just
// concatenates them with the offset table. (UTF-16 encoding is the
// caller's job — pstwriter does not depend on a Unicode library.)
struct MvStringEntry {
    const uint8_t* utf16leBytes;
    size_t         utf16leSize;
};

vector<uint8_t> encodeMvUnicode(const MvStringEntry* strings,
                                size_t               count);

} // namespace pstwriter
