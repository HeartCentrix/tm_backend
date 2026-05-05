// pstwriter/src/graph_event.cpp
//
// M9 Phase A — Graph event JSON parser.

#include "graph_event.hpp"

#include "graph_convert.hpp"
#include "internal_json.hpp"

#include <stdexcept>
#include <string>
#include <vector>

using std::string;
using std::vector;

namespace pstwriter {
namespace graph {

using namespace json_detail;

namespace {

DateTimeTimeZone extractDtTz(const JsonValue& obj)
{
    DateTimeTimeZone d;
    d.dateTime = getStr(obj, "dateTime");
    d.timeZone = getStr(obj, "timeZone");
    return d;
}

Location extractLocation(const JsonValue& obj)
{
    Location loc;
    loc.displayName  = getStr(obj, "displayName");
    loc.locationType = getStr(obj, "locationType");
    loc.locationUri  = getStr(obj, "locationUri");
    return loc;
}

AttendeeKind attendeeKindFrom(const string& s) noexcept
{
    if (s == "optional") return AttendeeKind::Optional;
    if (s == "resource") return AttendeeKind::Resource;
    return AttendeeKind::Required;
}

AttendeeResponse attendeeResponseFrom(const string& s) noexcept
{
    if (s == "organizer")            return AttendeeResponse::Organizer;
    if (s == "tentativelyAccepted")  return AttendeeResponse::TentativelyAccepted;
    if (s == "accepted")             return AttendeeResponse::Accepted;
    if (s == "declined")             return AttendeeResponse::Declined;
    if (s == "notResponded")         return AttendeeResponse::NotResponded;
    return AttendeeResponse::None;
}

ShowAs showAsFrom(const string& s) noexcept
{
    if (s == "free")             return ShowAs::Free;
    if (s == "tentative")        return ShowAs::Tentative;
    if (s == "oof")              return ShowAs::Oof;
    if (s == "workingElsewhere") return ShowAs::WorkingElsewhere;
    if (s == "unknown")          return ShowAs::Unknown;
    return ShowAs::Busy;
}

EventType eventTypeFrom(const string& s) noexcept
{
    if (s == "occurrence")    return EventType::Occurrence;
    if (s == "exception")     return EventType::Exception;
    if (s == "seriesMaster")  return EventType::SeriesMaster;
    return EventType::SingleInstance;
}

Importance importanceFrom(const string& s) noexcept
{
    if (s == "low")  return Importance::Low;
    if (s == "high") return Importance::High;
    return Importance::Normal;
}

void extractAttendees(const JsonValue& parent, vector<Attendee>& out)
{
    const JsonValue* arr = find(parent, "attendees");
    if (arr == nullptr || arr->type != JsonType::Array) return;
    for (const auto& el : arr->arr) {
        if (el.type != JsonType::Object) continue;
        Attendee a;
        a.kind = attendeeKindFrom(getStr(el, "type"));
        if (const JsonValue* st = find(el, "status"))
            a.response = attendeeResponseFrom(getStr(*st, "response"));
        if (const JsonValue* ea = find(el, "emailAddress")) {
            a.emailAddress.name    = getStr(*ea, "name");
            a.emailAddress.address = getStr(*ea, "address");
        }
        out.push_back(std::move(a));
    }
}

void extractBody(const JsonValue& parent, Body& out)
{
    const JsonValue* b = find(parent, "body");
    if (b == nullptr) return;
    out.content = getStr(*b, "content");
    const string ct = getStr(*b, "contentType");
    if (ct == "html" || ct == "Html" || ct == "HTML") out.contentType = BodyType::Html;
    else                                              out.contentType = BodyType::Text;
}

void extractLocations(const JsonValue& parent, vector<Location>& out)
{
    const JsonValue* arr = find(parent, "locations");
    if (arr == nullptr || arr->type != JsonType::Array) return;
    for (const auto& el : arr->arr) {
        if (el.type != JsonType::Object) continue;
        out.push_back(extractLocation(el));
    }
}

GraphEvent extractEvent(const JsonValue& obj)
{
    GraphEvent e;
    e.id                   = getStr(obj, "id");
    e.parentFolderId       = getStr(obj, "parentFolderId");
    e.changeKey            = getStr(obj, "changeKey");
    e.iCalUId              = getStr(obj, "iCalUId");
    e.transactionId        = getStr(obj, "transactionId");

    e.createdDateTime      = getStr(obj, "createdDateTime");
    e.lastModifiedDateTime = getStr(obj, "lastModifiedDateTime");

    e.subject     = getStr(obj, "subject");
    e.bodyPreview = getStr(obj, "bodyPreview");
    extractBody(obj, e.body);

    if (const JsonValue* s = find(obj, "start"))    e.start = extractDtTz(*s);
    if (const JsonValue* en = find(obj, "end"))     e.end   = extractDtTz(*en);
    if (const JsonValue* l = find(obj, "location")) e.location = extractLocation(*l);
    extractLocations(obj, e.locations);

    e.isAllDay                    = getBool(obj, "isAllDay");
    e.isCancelled                 = getBool(obj, "isCancelled");
    e.isDraft                     = getBool(obj, "isDraft");
    e.isOnlineMeeting             = getBool(obj, "isOnlineMeeting");
    e.isOrganizer                 = getBool(obj, "isOrganizer");
    e.isReminderOn                = getBool(obj, "isReminderOn");
    e.reminderMinutesBeforeStart  = getInt (obj, "reminderMinutesBeforeStart");
    e.responseRequested           = getBool(obj, "responseRequested", true);
    e.hasAttachments              = getBool(obj, "hasAttachments");

    e.importance = importanceFrom(getStr(obj, "importance"));
    e.showAs     = showAsFrom(getStr(obj, "showAs"));
    e.type       = eventTypeFrom(getStr(obj, "type"));

    e.seriesMasterId        = getStr(obj, "seriesMasterId");
    e.originalStartTimeZone = getStr(obj, "originalStartTimeZone");
    e.originalEndTimeZone   = getStr(obj, "originalEndTimeZone");

    if (const JsonValue* org = find(obj, "organizer")) {
        if (const JsonValue* ea = find(*org, "emailAddress")) {
            e.organizer.name    = getStr(*ea, "name");
            e.organizer.address = getStr(*ea, "address");
            e.hasOrganizer = !e.organizer.address.empty() || !e.organizer.name.empty();
        }
    }

    extractAttendees(obj, e.attendees);
    e.categories = getStringArray(obj, "categories");

    if (const JsonValue* om = find(obj, "onlineMeeting")) {
        e.onlineMeetingUrl = getStr(*om, "joinUrl");
    }
    e.onlineMeetingProvider = getStr(obj, "onlineMeetingProvider");

    return e;
}

} // namespace

GraphEvent parseGraphEvent(const string& json)
{
    Parser p(json);
    JsonValue v = p.parseTopLevel();
    if (v.type != JsonType::Object)
        throw std::runtime_error("graph_event JSON: expected top-level object");
    return extractEvent(v);
}

vector<GraphEvent> parseGraphEventList(const string& json)
{
    Parser p(json);
    JsonValue v = p.parseTopLevel();

    const JsonArray* arr = nullptr;
    if (v.type == JsonType::Array) {
        arr = &v.arr;
    } else if (v.type == JsonType::Object) {
        const JsonValue* val = find(v, "value");
        if (val != nullptr && val->type == JsonType::Array) arr = &val->arr;
    }
    if (arr == nullptr)
        throw std::runtime_error("graph_event JSON: expected array or {value:[...]}");

    vector<GraphEvent> out;
    out.reserve(arr->size());
    for (const auto& el : *arr) {
        if (el.type != JsonType::Object) continue;
        out.push_back(extractEvent(el));
    }
    return out;
}

} // namespace graph
} // namespace pstwriter
