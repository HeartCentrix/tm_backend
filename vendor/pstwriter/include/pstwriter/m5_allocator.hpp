// pstwriter/m5_allocator.hpp
//
// M5 — NID assignment service.
//
// IMPORTANT — reader contract (parallel to the M4 PC HID-agnostic-reader
// contract): the allocator produces a deterministic NID stream for THIS
// writer instance. Any reader walking an NBT MUST resolve NIDs via NBT
// binary-search descent, NEVER by reconstructing the allocator's sequence.
// Real Outlook PSTs are write-once-with-edits, so their NID layouts are
// arbitrary and cannot match any single deterministic allocator's output.
// Same lesson as PC HID-agnosticism — applied prospectively, not after a
// real-Outlook surprise.
//
// Spec references (all verified against actual section text 2026-05-02):
//   * [SPEC sec 2.2.2.1] NID layout: low 5 bits = nidType, high 27 bits
//     = nidIndex. Encoding: NID = (nidIndex << 5) | (nidType & 0x1F).
//   * [SPEC sec 2.4.1]  14 reserved internal NIDs. All but NID_ROOT_FOLDER
//     have nidType = NID_TYPE_INTERNAL (0x01); NID_ROOT_FOLDER = 0x122 has
//     nidType = NID_TYPE_NORMAL_FOLDER (0x02). The "Internal" in the
//     section title means "internal to the implementation", not literally
//     nidType == NID_TYPE_INTERNAL.

#pragma once

#include "types.hpp"

#include <cstdint>
#include <set>

namespace pstwriter {

class M5Allocator {
public:
    // Pre-populates the reserved-NID set per [SPEC sec 2.4.1] in the
    // constructor. Per-nidType counter seeding:
    //   * Internal (0x01) and HID (0x00): start at nidIndex = 1, the
    //     smallest legal index per [SPEC sec 2.2.2.1].
    //   * Every other (user-allocatable) nidType: start at nidIndex =
    //     0x400. Per [MS-PST] §2.4.3 the low-index range is reserved
    //     for system NIDs; real Outlook / Aspose-produced PSTs place
    //     user folders/messages/attachments/tables at idx >= 0x400.
    //     Allocating below this causes Outlook to reject the file.
    M5Allocator() noexcept;

    // ------------------------------------------------------------------
    // Reserved-NID lookup ([SPEC sec 2.4.1])
    // ------------------------------------------------------------------
    enum class ReservedNid : uint32_t {
        MessageStore                = 0x00000021u, // NID_MESSAGE_STORE
        NameToIdMap                 = 0x00000061u, // NID_NAME_TO_ID_MAP
        NormalFolderTemplate        = 0x000000A1u, // NID_NORMAL_FOLDER_TEMPLATE
        SearchFolderTemplate        = 0x000000C1u, // NID_SEARCH_FOLDER_TEMPLATE
        RootFolder                  = 0x00000122u, // NID_ROOT_FOLDER
        SearchManagementQueue       = 0x000001E1u, // NID_SEARCH_MANAGEMENT_QUEUE
        SearchActivityList          = 0x00000201u, // NID_SEARCH_ACTIVITY_LIST
        Reserved1                   = 0x00000241u, // NID_RESERVED1
        SearchDomainObject          = 0x00000261u, // NID_SEARCH_DOMAIN_OBJECT
        SearchGathererQueue         = 0x00000281u, // NID_SEARCH_GATHERER_QUEUE
        SearchGathererDescriptor    = 0x000002A1u, // NID_SEARCH_GATHERER_DESCRIPTOR
        Reserved2                   = 0x000002E1u, // NID_RESERVED2
        Reserved3                   = 0x00000301u, // NID_RESERVED3
        SearchGathererFolderQueue   = 0x00000321u, // NID_SEARCH_GATHERER_FOLDER_QUEUE
    };

    // Returns the spec-mandated NID value for the given reserved kind.
    // This is the ONLY supported way to obtain a reserved NID — direct
    // construction via Nid(...) is not validated against this set.
    static Nid reservedNidFor(ReservedNid which) noexcept;

    // The full set of reserved NIDs as a fixed-size span (suitable for
    // tests that enumerate the table). The order matches the [SPEC sec
    // 2.4.1] table top-to-bottom.
    static constexpr size_t kReservedCount = 14;
    static const Nid* allReservedNids() noexcept;

    // ------------------------------------------------------------------
    // Auto-allocation (deterministic per [DESIGN])
    // ------------------------------------------------------------------
    // Allocate the next NID with the given nidType. The per-nidType
    // counter advances by 1 (NID += 0x20) per call, automatically
    // skipping any NID already taken (reserved or pre-registered).
    //
    // Throws std::runtime_error on:
    //   * nidType not one of the 19 valid values from [SPEC sec 2.2.2.1]
    //   * 27-bit nidIndex counter exhausted for this nidType
    Nid allocate(NidType nt);

    // Pre-register a caller-supplied NID. Future allocate(nt) calls will
    // skip past this NID. Useful for callers that need a specific NID
    // value and want the auto-counter to avoid colliding with it.
    //
    // Throws std::runtime_error on:
    //   * NID's nidType not one of the 19 valid values
    //   * NID already allocated (reserved or previously registered)
    void registerExternal(Nid nid);

    // ------------------------------------------------------------------
    // Diagnostics
    // ------------------------------------------------------------------
    bool isAllocated(Nid nid) const noexcept;
    size_t allocatedCount() const noexcept;

    // [SPEC sec 2.2.2.1] nidType validity check. Returns true iff nt is
    // one of the 19 defined values (0x00-0x08, 0x0A-0x13, 0x1F).
    static bool isValidNidType(NidType nt) noexcept;

private:
    std::set<uint32_t> allocated_;
    // Per-nidType counter (5-bit index space = 32 entries). Only the 19
    // valid nidType slots are ever used; the rest are inert.
    uint32_t nextIndex_[32]{};
};

} // namespace pstwriter
