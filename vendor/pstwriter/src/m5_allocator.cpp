// pstwriter/src/m5_allocator.cpp
//
// Implementation of the M5 NID allocation service.

#include "m5_allocator.hpp"

#include <array>
#include <stdexcept>
#include <string>

namespace pstwriter {

namespace {

// [SPEC sec 2.4.1] reserved NIDs, in spec-table order.
constexpr Nid kReservedTable[M5Allocator::kReservedCount] = {
    Nid{0x00000021u}, // NID_MESSAGE_STORE
    Nid{0x00000061u}, // NID_NAME_TO_ID_MAP
    Nid{0x000000A1u}, // NID_NORMAL_FOLDER_TEMPLATE
    Nid{0x000000C1u}, // NID_SEARCH_FOLDER_TEMPLATE
    Nid{0x00000122u}, // NID_ROOT_FOLDER (nidType = NormalFolder, not Internal)
    Nid{0x000001E1u}, // NID_SEARCH_MANAGEMENT_QUEUE
    Nid{0x00000201u}, // NID_SEARCH_ACTIVITY_LIST
    Nid{0x00000241u}, // NID_RESERVED1
    Nid{0x00000261u}, // NID_SEARCH_DOMAIN_OBJECT
    Nid{0x00000281u}, // NID_SEARCH_GATHERER_QUEUE
    Nid{0x000002A1u}, // NID_SEARCH_GATHERER_DESCRIPTOR
    Nid{0x000002E1u}, // NID_RESERVED2
    Nid{0x00000301u}, // NID_RESERVED3
    Nid{0x00000321u}, // NID_SEARCH_GATHERER_FOLDER_QUEUE
};

// 27-bit nidIndex space ceiling.
constexpr uint32_t kMaxNidIndex = (1u << 27);

// Starting nidIndex for user-allocatable nidTypes.
//
// Per [MS-PST] §2.4.3 the low nidIndex range is reserved for system-
// mandated NIDs. Real Outlook (and Aspose-produced) PSTs place every
// user-allocated folder/message/attachment/table at nidIndex >= 0x400.
// Allocating user nodes inside the reserved range causes Outlook to
// reject the file as malformed at open time — confirmed empirically
// against an Aspose-vs-pstwriter byte-diff (Aspose folder 0x8082 idx
// 0x404, message 0x200024 idx 0x10001; ours 0x22 / 0x24 idx 0x1).
//
// Internal (0x01) keeps starting at idx=1 because [SPEC sec 2.4.1]
// pins multiple reserved Internal NIDs at low indices (NID_MESSAGE_STORE
// = idx 1, NID_NAME_TO_ID_MAP = idx 3, ...). The reserved set is
// pre-populated below and the counter skips past those automatically.
//
// HID (0x00) is not a node type — it identifies allocations inside the
// LTP heap, never a node in the NBT — so its counter is unused but kept
// at 1 for parity.
constexpr uint32_t kUserAllocStart = 0x400u;

// M12.14 (2026-05-13) — start user-allocated NormalFolder (type 0x02)
// NIDs at idx 0x404 instead of 0x400.
//
// idx 0x401/0x402/0x403 are claimed by the baseline IPM Subtree /
// Search Root / Deleted Items reserved set, so user folders would
// otherwise start at idx 0x400 (NID 0x8002). Real-Outlook contacts.pst
// byte-diff (2026-05-13) shows the first user contact folder lives at
// idx 0x404 (NID 0x8082) — idx 0x400 is left unused.
//
// Why this matters for the import wizard: when our user Contacts
// folder lives at NID 0x8002 the IPM Subtree's Hierarchy TC sorts it
// at row 0 (ahead of Deleted Items at 0x8062). REF puts Deleted Items
// at row 0 and the user folder at row 1. The user's M12.13 test
// imported our PST into a fresh local-test.pst and observed the
// destination contained only Deleted Items + Search Folders — the
// wizard did not create a destination Contacts folder, i.e. it didn't
// recognise our source Contacts folder at all. Shifting to idx 0x404
// makes our row layout match REF, eliminating that as a variable.
constexpr uint32_t kNormalFolderUserAllocStart = 0x404u;

} // namespace

// --------------------------------------------------------------------------
// Static helpers
// --------------------------------------------------------------------------
bool M5Allocator::isValidNidType(NidType nt) noexcept
{
    const uint32_t v = static_cast<uint32_t>(nt) & 0x1Fu;
    // Per [SPEC sec 2.2.2.1] table: 0x00, 0x01..0x08, 0x0A..0x13, 0x1F.
    if (v == 0x00u || v == 0x1Fu) return true;
    if (v >= 0x01u && v <= 0x08u) return true;
    if (v >= 0x0Au && v <= 0x13u) return true;
    return false;
}

Nid M5Allocator::reservedNidFor(ReservedNid which) noexcept
{
    return Nid{static_cast<uint32_t>(which)};
}

const Nid* M5Allocator::allReservedNids() noexcept
{
    return kReservedTable;
}

// --------------------------------------------------------------------------
// Construction
// --------------------------------------------------------------------------
M5Allocator::M5Allocator() noexcept
{
    // See kUserAllocStart comment above for the seeding rationale.
    for (size_t i = 0; i < 32; ++i) {
        nextIndex_[i] = kUserAllocStart;
    }
    nextIndex_[static_cast<size_t>(NidType::HID)      & 0x1Fu] = 1u;
    nextIndex_[static_cast<size_t>(NidType::Internal) & 0x1Fu] = 1u;
    // M12.14: NormalFolder starts at idx 0x404 (NID 0x8082 for first
    // user folder) — matches real-Outlook layout. See kNormalFolderUserAllocStart
    // comment for rationale.
    nextIndex_[static_cast<size_t>(NidType::NormalFolder) & 0x1Fu] = kNormalFolderUserAllocStart;
    // Round L (byte-diff vs backup.pst): real-Outlook PSTs allocate
    // messages at idx ≥ 0x10001, separate from folder/table idx range
    // (≤ ~0x400). Sharing idx 0x400 between folder NID 0x8002 (type 02)
    // and message NID 0x8004 (type 04) appears to trip scanpst's
    // "Contents Table for X, row doesn't match sub-object" check —
    // scanpst's NBT walker doesn't expect the same idx to host both.
    nextIndex_[static_cast<size_t>(NidType::NormalMessage) & 0x1Fu] = 0x10001u;
    nextIndex_[static_cast<size_t>(NidType::AssocMessage)  & 0x1Fu] = 0x10001u;

    // Reserve the 14 spec-mandated NIDs.
    for (size_t i = 0; i < kReservedCount; ++i) {
        allocated_.insert(kReservedTable[i].value);
    }
}

// --------------------------------------------------------------------------
// allocate
// --------------------------------------------------------------------------
Nid M5Allocator::allocate(NidType nt)
{
    if (!isValidNidType(nt)) {
        throw std::runtime_error(
            "M5Allocator::allocate: invalid nidType (not one of the 19 "
            "values from [SPEC sec 2.2.2.1])");
    }
    const uint32_t typeBits = static_cast<uint32_t>(nt) & 0x1Fu;

    // Advance counter past any NID already taken.
    while (true) {
        const uint32_t idx = nextIndex_[typeBits];
        if (idx >= kMaxNidIndex) {
            throw std::runtime_error(
                "M5Allocator::allocate: 27-bit nidIndex counter exhausted "
                "for this nidType");
        }
        ++nextIndex_[typeBits];
        const Nid candidate(nt, idx);
        if (allocated_.find(candidate.value) == allocated_.end()) {
            allocated_.insert(candidate.value);
            return candidate;
        }
        // Otherwise the slot was reserved or pre-registered — try the next
        // index. Loop is guaranteed to terminate because the reserved set
        // is finite and the index space has 2^27 slots per nidType.
    }
}

// --------------------------------------------------------------------------
// registerExternal
// --------------------------------------------------------------------------
void M5Allocator::registerExternal(Nid nid)
{
    if (!isValidNidType(nid.type())) {
        throw std::runtime_error(
            "M5Allocator::registerExternal: NID has invalid nidType");
    }
    if (allocated_.find(nid.value) != allocated_.end()) {
        throw std::runtime_error(
            "M5Allocator::registerExternal: NID already allocated");
    }
    allocated_.insert(nid.value);
}

// --------------------------------------------------------------------------
// Diagnostics
// --------------------------------------------------------------------------
bool M5Allocator::isAllocated(Nid nid) const noexcept
{
    return allocated_.find(nid.value) != allocated_.end();
}

size_t M5Allocator::allocatedCount() const noexcept
{
    return allocated_.size();
}

} // namespace pstwriter
