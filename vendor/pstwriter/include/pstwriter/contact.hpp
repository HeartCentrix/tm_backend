// pstwriter/contact.hpp
//
// M8 — Contact builders. Layered on M4 (PC), M6 (folder PC), M7 (mail
// folder + writer pattern), and M8 Phase A (graph_contact + JSON
// parser).
//
// Public surface:
//   buildContactPc(GraphContact, ctx)  — IPM.Contact PC bytes
//   writeM8Pst(M8PstConfig)            — Phase C end-to-end PST writer

#pragma once

#include "graph_contact.hpp"
#include "ltp.hpp"
#include "mail.hpp"      // for MailPcResult, MailPcBuildContext, M7Folder pieces
#include "types.hpp"
#include "writer.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace pstwriter {

// ============================================================================
// buildContactPc — emit an IPM.Contact PC for one Graph contact.
//
// Properties emitted (subset of [MS-OXOCNTC] property list):
//   * PidTagMessageClass_W           (0x001A001F) "IPM.Contact"
//   * PidTagDisplayName_W            (0x3001001F)
//   * PidTagGivenName_W              (0x3A06001F)
//   * PidTagSurname_W                (0x3A11001F)
//   * PidTagMiddleName_W             (0x3A44001F)
//   * PidTagNickname_W               (0x3A4F001F)
//   * PidTagInitials_W               (0x3A0A001F)
//   * PidTagGeneration_W             (0x3A05001F)
//   * PidTagDisplayNamePrefix_W      (0x3A45001F) — Graph 'title' field
//   * PidTagTitle_W                  (0x3A17001F) — Graph 'jobTitle' field
//   * PidTagCompanyName_W            (0x3A16001F)
//   * PidTagDepartmentName_W         (0x3A18001F)
//   * PidTagOfficeLocation_W         (0x3A19001F)
//   * PidTagProfession_W             (0x3A46001F)
//
//   Phones:
//   * PidTagBusinessTelephoneNumber_W (0x3A08001F) — first businessPhones[]
//   * PidTagMobileTelephoneNumber_W   (0x3A1C001F)
//   * PidTagHomeTelephoneNumber_W     (0x3A09001F) — first homePhones[]
//
//   Birthday / anniversary:
//   * PidTagBirthday                  (0x3A420040)
//   * PidTagWeddingAnniversary        (0x3A410040)
//
//   Email (first only):
//   * PidTagEmailAddress_W            (0x3003001F) — first emailAddresses[]
//   * PidTagAddressType_W             (0x3002001F) — "SMTP"
//
//   Addresses (concatenated lines + per-component):
//   * PidTagBusinessAddressStreet_W            (0x3A29001F)
//   * PidTagBusinessAddressCity_W              (0x3A27001F)
//   * PidTagBusinessAddressStateOrProvince_W   (0x3A28001F)
//   * PidTagBusinessAddressPostalCode_W        (0x3A2A001F)
//   * PidTagBusinessAddressCountry_W           (0x3A26001F)
//   * Same group at 0x3A5? (home address) and 0x3A6? (other)
//
// Notes / KNOWN_UNVERIFIED M8-1: Outlook-native contact email storage uses
// PidLid* NAMED PROPERTIES (PidLidEmail1Address, etc.) per [MS-OXOCNTC]
// §2.2.1.1, requiring Name-to-ID Map population. M8 emits the simpler
// PidTagEmailAddress_W (0x3003) — same tag used by recipient rows. This
// is ENOUGH for the contact's "email address" field to round-trip via
// readPropertyContext, but Outlook's contact UI may not pick it up
// without the named-property variant. Outlook gate at M8 Phase D
// surfaces the gap.
// ============================================================================
MailPcResult buildContactPc(const graph::GraphContact& contact,
                            const MailPcBuildContext&  ctx);

// ============================================================================
// Build a Contact (IPF.Contact) folder PC. Wraps the universal envelope
// builder `buildFolderPcExtended` (mail.hpp / messaging.cpp) with the
// contact container-class bytes baked in.
// ============================================================================
PcResult buildContactFolderPc(const M7FolderSchema& schema,
                              Nid                   firstSubnodeNid);

// ============================================================================
// M8 — Folder for contacts (IPF.Contact).
//
// Reuses M7Folder shape but defaults containerClass to "IPF.Contact".
// ============================================================================
struct M8ContactFolder {
    std::string  displayName;
    Nid          nid;
    Nid          parentNid;
    std::string  containerClass {"IPF.Contact"};

    // Contacts contained in this folder, in display order.
    std::vector<const graph::GraphContact*> contacts;
};

// ============================================================================
// M8 — End-to-end writer.
// Produces a PST with:
//   * 27 §2.7.1 mandatory nodes (carryover from M6 / M7).
//   * Per M8ContactFolder: Folder PC (containerClass = IPF.Contact) +
//     hierarchy/contents/FAI sibling tables.
//   * Per contact: Contact PC (NormalMessage NID type + IPM.Contact
//     message class) parented to its containing folder.
//   * IPM Subtree's Hierarchy TC populated with each contacts folder.
// ============================================================================
struct M8PstConfig {
    std::string             path;
    std::array<uint8_t, 16> providerUid;
    std::string             pstDisplayName  {"M8 Contacts PST"};

    std::vector<M8ContactFolder> folders;
};

WriteResult writeM8Pst(const M8PstConfig& config) noexcept;

} // namespace pstwriter
