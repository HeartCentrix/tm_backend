// pstwriter/event.hpp
//
// M9 — Calendar/event builders. Layered on M4 (PC), M6 (folder PC),
// M7 (mail builders + writer pattern), M8 (contact builders), and M9
// Phase A (graph_event + JSON parser).
//
// Public surface:
//   buildEventPc(GraphEvent, ctx)     — IPM.Appointment PC bytes
//   writeM9Pst(M9PstConfig)           — Phase C end-to-end PST writer

#pragma once

#include "graph_event.hpp"
#include "ltp.hpp"
#include "mail.hpp"      // for MailPcResult, MailPcBuildContext
#include "types.hpp"
#include "writer.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace pstwriter {

// ============================================================================
// buildEventPc — emit an IPM.Appointment PC for one Graph event.
//
// Properties emitted (top-level PidTags only — named-property storage
// per [MS-OXOCAL] PSETID_Appointment is M10 hardening; see
// KNOWN_UNVERIFIED M9-1):
//
//   * PidTagMessageClass_W           (0x001A001F) "IPM.Appointment"
//   * PidTagSubject_W                (0x0037001F)
//   * PidTagBody_W                   (0x1000001F) plain-text body fallback
//   * PidTagBodyHtml                 (0x10130102) HTML body when present
//   * PidTagImportance               (0x00170003)
//   * PidTagSensitivity              (0x00360003) — 0=normal/2=private/etc.
//   * PidTagCreationTime             (0x30070040)
//   * PidTagLastModificationTime     (0x30080040)
//
//   Appointment top-level mirrors of the named properties:
//   * PidTagStartDate                (0x00600040) mirrors PidLidAppointmentStartWhole
//   * PidTagEndDate                  (0x00610040) mirrors PidLidAppointmentEndWhole
//
//   Organizer (Graph 'organizer' field) — re-uses M7 sender PidTags:
//   * PidTagSenderName_W             (0x0C1A001F)
//   * PidTagSenderEmailAddress_W     (0x0C1F001F)
//   * PidTagSenderAddressType_W      (0x0C1E001F) "SMTP"
//   * PidTagSenderEntryId            (0x0C190102)
//   * PidTagSenderSearchKey          (0x0C1D0102)
//   * PidTagSentRepresentingName_W + mirrors
//
//   Location:
//   * Top-level: emits combined location.displayName as a bonus
//     PidTagSubject suffix is NOT done; instead we just skip location
//     (M10 hardening adds PidLidLocation).
//
// M10: PidLid* named props under PSETID_Appointment are now emitted
// alongside the legacy PidTag mirrors. The Name-to-Id Map at NID 0x61
// (see buildNameToIdMapPc in messaging.cpp) registers:
//   * kLidAppointmentStartWhole (dispid 0x820D) — PtypTime
//   * kLidAppointmentEndWhole   (dispid 0x820E) — PtypTime
//   * kLidLocation              (dispid 0x8208) — PtypString
//   * kLidAppointmentDuration   (dispid 0x8213) — PtypInteger32
//   * kLidAppointmentSubType    (dispid 0x8215) — PtypBoolean
// Outlook's Calendar UI reads these named variants to render the grid.
// ============================================================================
MailPcResult buildEventPc(const graph::GraphEvent&    event,
                          const MailPcBuildContext&   ctx);

// ============================================================================
// M9 — Folder for calendar events (IPF.Appointment).
// ============================================================================
struct M9CalendarFolder {
    std::string  displayName;
    Nid          nid;
    Nid          parentNid;
    std::string  containerClass {"IPF.Appointment"};

    // Optional folder identity for nesting (same contract as M7Folder).
    // When ``path`` is non-empty and ``parentPath`` references another
    // M9CalendarFolder's ``path`` in the same config, the writer
    // resolves the parent's allocated NID and overrides ``parentNid``
    // accordingly. Use these to materialise the source mailbox's
    // multi-calendar layout in Outlook (e.g. "Calendar/Default",
    // "Calendar/United States holidays") instead of a single flat
    // "Calendar" folder. Empty values fall back to legacy parentNid
    // wiring.
    std::string  path;
    std::string  parentPath;

    std::vector<const graph::GraphEvent*> events;
};

// ============================================================================
// M9 — End-to-end writer.
// Produces a PST with:
//   * 27 §2.7.1 mandatory nodes (carryover from M6).
//   * Per M9CalendarFolder: Folder PC (containerClass = "IPF.Appointment")
//     + hierarchy/contents/FAI sibling tables.
//   * Per event: Event PC (NormalMessage NID type + IPM.Appointment).
//   * IPM Subtree's Hierarchy TC populated with each calendar folder.
// ============================================================================
struct M9PstConfig {
    std::string             path;
    std::array<uint8_t, 16> providerUid;
    std::string             pstDisplayName  {"M9 Calendar PST"};

    std::vector<M9CalendarFolder> folders;
};

WriteResult writeM9Pst(const M9PstConfig& config) noexcept;

} // namespace pstwriter
