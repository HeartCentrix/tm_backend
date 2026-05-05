// pstwriter/graph_contact.hpp
//
// M8 Phase A — Microsoft Graph "contact" resource model + JSON parser.
//
// Reference: learn.microsoft.com/en-us/graph/api/resources/contact
//
// Mirrors the M7 GraphMessage approach: parse Graph JSON into a struct;
// keep UTF-8 strings verbatim for later UTF-16-LE conversion at PC-build
// time. Unknown fields are ignored.

#pragma once

#include "graph_convert.hpp"
#include "graph_message.hpp"   // for EmailAddress

#include <cstdint>
#include <string>
#include <vector>

namespace pstwriter {
namespace graph {

// ============================================================================
// PhysicalAddress — Graph "physicalAddress" complex type.
// Used for businessAddress / homeAddress / otherAddress.
// ============================================================================
struct PhysicalAddress {
    std::string street;
    std::string city;
    std::string state;
    std::string postalCode;
    std::string countryOrRegion;
};

// ============================================================================
// GraphContact — root contact object. Fields mirror the Graph "contact"
// resource. Optional / not-present fields are empty strings or empty
// vectors.
// ============================================================================
struct GraphContact {
    // Identity
    std::string id;
    std::string parentFolderId;
    std::string changeKey;

    // Times
    std::string createdDateTime;
    std::string lastModifiedDateTime;

    // Personal info
    std::string displayName;       // Graph 'displayName' (often computed)
    std::string givenName;
    std::string surname;
    std::string middleName;
    std::string nickName;
    std::string initials;
    std::string generation;        // "Jr.", "Sr." etc.
    std::string title;             // Display-name prefix, e.g. "Mr.", "Dr."
    std::string fileAs;            // How Outlook files the contact

    // Birthday + anniversary (Graph-style ISO 8601 OR null)
    std::string birthday;
    std::string anniversary;       // not in stock Graph contact, but kept
                                   // for the M9-shared design

    // Job
    std::string jobTitle;          // PR_TITLE in MAPI (0x3A17)
    std::string companyName;
    std::string department;
    std::string officeLocation;
    std::string profession;

    // Email addresses (Graph contact.emailAddresses[])
    std::vector<EmailAddress> emailAddresses;

    // Phone numbers
    std::vector<std::string> businessPhones;
    std::string mobilePhone;
    std::vector<std::string> homePhones;

    // IM addresses
    std::vector<std::string> imAddresses;

    // Addresses
    PhysicalAddress businessAddress;
    PhysicalAddress homeAddress;
    PhysicalAddress otherAddress;

    // Web
    std::string businessHomePage;
    std::string personalNotes;

    // Categories (string array — Graph)
    std::vector<std::string> categories;
};

// ============================================================================
// parseGraphContact(json) — single contact object.
// parseGraphContactList(json) — array form or {value:[...]} envelope.
// ============================================================================
GraphContact parseGraphContact(const std::string& json);
std::vector<GraphContact> parseGraphContactList(const std::string& json);

} // namespace graph
} // namespace pstwriter
