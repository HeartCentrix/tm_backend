// pstwriter/graph_event.hpp
//
// M9 Phase A — Microsoft Graph "event" resource model + JSON parser.
//
// Reference: learn.microsoft.com/en-us/graph/api/resources/event
//
// Mirrors M7 graph_message + M8 graph_contact: parse Graph JSON into a
// struct; UTF-8 strings preserved verbatim; unknown fields ignored.

#pragma once

#include "graph_convert.hpp"
#include "graph_message.hpp"   // for EmailAddress, Body

#include <cstdint>
#include <string>
#include <vector>

namespace pstwriter {
namespace graph {

// ============================================================================
// DateTimeTimeZone — Graph dateTimeTimeZone complex type.
//   { dateTime: "2024-06-01T15:00:00", timeZone: "UTC" }
// ============================================================================
struct DateTimeTimeZone {
    std::string dateTime;   // ISO 8601 without TZ designator (per Graph)
    std::string timeZone;   // Windows / IANA tz name; "UTC" most common
};

// ============================================================================
// Location — Graph "location" complex type. Subset.
// ============================================================================
struct Location {
    std::string displayName;     // free text
    std::string locationType;    // "default", "conferenceRoom", etc.
    std::string locationUri;     // optional URI for online locations
};

// ============================================================================
// Attendee + AttendeeStatus — Graph attendee complex type.
// ============================================================================
enum class AttendeeKind : uint8_t {
    Required = 0,
    Optional = 1,
    Resource = 2,
};

enum class AttendeeResponse : uint8_t {
    None              = 0,
    Organizer         = 1,
    TentativelyAccepted = 2,
    Accepted          = 3,
    Declined          = 4,
    NotResponded      = 5,
};

struct Attendee {
    AttendeeKind     kind     {AttendeeKind::Required};
    AttendeeResponse response {AttendeeResponse::None};
    EmailAddress     emailAddress;
};

// ============================================================================
// EventType — Graph 'type' field discriminator.
// ============================================================================
enum class EventType : uint8_t {
    SingleInstance = 0,
    Occurrence     = 1,
    Exception      = 2,
    SeriesMaster   = 3,
};

// ============================================================================
// ShowAs — Graph 'showAs' field.
// ============================================================================
enum class ShowAs : uint8_t {
    Free             = 0,
    Tentative        = 1,
    Busy             = 2,
    Oof              = 3,   // out of office
    WorkingElsewhere = 4,
    Unknown          = 5,
};

// ============================================================================
// GraphEvent — root event object. Subset of Graph "event" resource.
// ============================================================================
struct GraphEvent {
    // Identity
    std::string id;
    std::string parentFolderId;
    std::string changeKey;
    std::string iCalUId;
    std::string transactionId;

    // Times (server-side bookkeeping)
    std::string createdDateTime;
    std::string lastModifiedDateTime;

    // Subject + body
    std::string subject;
    std::string bodyPreview;
    Body        body;

    // When + where
    DateTimeTimeZone start;
    DateTimeTimeZone end;
    Location         location;
    std::vector<Location> locations;

    // Flags
    bool      isAllDay        {false};
    bool      isCancelled     {false};
    bool      isDraft         {false};
    bool      isOnlineMeeting {false};
    bool      isOrganizer     {false};
    bool      isReminderOn    {false};
    int32_t   reminderMinutesBeforeStart {0};
    bool      responseRequested {true};
    bool      hasAttachments    {false};

    // Importance / sensitivity / show-as
    Importance importance {Importance::Normal};
    ShowAs     showAs     {ShowAs::Busy};

    // Series
    EventType type {EventType::SingleInstance};
    std::string seriesMasterId;
    std::string originalStartTimeZone;
    std::string originalEndTimeZone;

    // Organizer + attendees
    EmailAddress         organizer;
    bool                 hasOrganizer {false};
    std::vector<Attendee> attendees;

    // Categories
    std::vector<std::string> categories;

    // Online meeting
    std::string onlineMeetingProvider;
    std::string onlineMeetingUrl;
};

// ============================================================================
// parseGraphEvent / parseGraphEventList
// ============================================================================
GraphEvent parseGraphEvent(const std::string& json);
std::vector<GraphEvent> parseGraphEventList(const std::string& json);

} // namespace graph
} // namespace pstwriter
