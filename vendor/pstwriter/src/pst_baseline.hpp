// pstwriter/src/pst_baseline.hpp
//
// M10 — Internal helper that builds the 27 §2.7.1 mandatory nodes'
// bodies + nidParent wiring. Shared by writeM7Pst, writeM8Pst,
// writeM9Pst (previously each had ~200 lines of identical code).
//
// NOT a public API. Lives under src/ rather than include/pstwriter/.
//
// What's included:
//   * NIDs 0x0021, 0x0061, 0x0122 (Root + self-parent), 0x012D, 0x012E,
//     0x012F, 0x01E1, 0x0201, 0x060D, 0x060E, 0x060F, 0x0610, 0x0671,
//     0x0692, 0x2223, 0x8022, 0x8042, 0x804D, 0x804E, 0x804F, 0x8062,
//     0x806D, 0x806E, 0x806F  (= 24 entries)
//
// What's EXCLUDED (caller-built because content depends on user folders):
//   * NID 0x802D — IPM Subtree Hierarchy TC (rows: 1 per user folder)
//   * NIDs 0x802E, 0x802F — IPM Subtree Contents/FAI (typically 0-row;
//     callers can use buildFolderContentsTc / buildFolderFaiContentsTc
//     directly to fill these)
//
// So the helper returns 24 entries; caller adds 3 more (802D/802E/802F)
// for the full 27.

#pragma once

#include "messaging.hpp"
#include "m5_allocator.hpp"
#include "types.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace pstwriter {

struct PstBaselineEntry {
    Nid                  nid;
    Nid                  nidParent;
    std::vector<uint8_t> body;
};

// Build the 24 mandatory nodes (27 minus the 3 IPM Subtree sibling tables).
//
// `pstDisplayName` is UTF-8 — encoded into the message store PC's
// PidTagDisplayName slot. May be empty.
std::vector<PstBaselineEntry>
buildPstBaselineEntries(const std::array<uint8_t, 16>& providerUid,
                        const std::string&             pstDisplayName);

// Pre-register every reserved §2.7.1 NID the baseline uses into an M5Allocator
// so subsequent allocate() calls skip them.
void registerBaselineReservedNids(M5Allocator& alloc);

} // namespace pstwriter
