// pstwriter/src/graph_contact.cpp
//
// M8 Phase A — Graph contact JSON parser.

#include "graph_contact.hpp"

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

PhysicalAddress extractAddress(const JsonValue& obj)
{
    PhysicalAddress a;
    a.street          = getStr(obj, "street");
    a.city            = getStr(obj, "city");
    a.state           = getStr(obj, "state");
    a.postalCode      = getStr(obj, "postalCode");
    a.countryOrRegion = getStr(obj, "countryOrRegion");
    return a;
}

void extractEmailAddresses(const JsonValue& parent, vector<EmailAddress>& out)
{
    const JsonValue* arr = find(parent, "emailAddresses");
    if (arr == nullptr || arr->type != JsonType::Array) return;
    for (const auto& el : arr->arr) {
        if (el.type != JsonType::Object) continue;
        EmailAddress e;
        e.name    = getStr(el, "name");
        e.address = getStr(el, "address");
        out.push_back(std::move(e));
    }
}

GraphContact extractContact(const JsonValue& obj)
{
    GraphContact c;
    c.id                   = getStr(obj, "id");
    c.parentFolderId       = getStr(obj, "parentFolderId");
    c.changeKey            = getStr(obj, "changeKey");
    c.createdDateTime      = getStr(obj, "createdDateTime");
    c.lastModifiedDateTime = getStr(obj, "lastModifiedDateTime");

    c.displayName = getStr(obj, "displayName");
    c.givenName   = getStr(obj, "givenName");
    c.surname     = getStr(obj, "surname");
    c.middleName  = getStr(obj, "middleName");
    c.nickName    = getStr(obj, "nickName");
    c.initials    = getStr(obj, "initials");
    c.generation  = getStr(obj, "generation");
    c.title       = getStr(obj, "title");
    c.fileAs      = getStr(obj, "fileAs");

    c.birthday    = getStr(obj, "birthday");
    c.anniversary = getStr(obj, "anniversary");

    c.jobTitle       = getStr(obj, "jobTitle");
    c.companyName    = getStr(obj, "companyName");
    c.department     = getStr(obj, "department");
    c.officeLocation = getStr(obj, "officeLocation");
    c.profession     = getStr(obj, "profession");

    extractEmailAddresses(obj, c.emailAddresses);

    c.businessPhones = getStringArray(obj, "businessPhones");
    c.mobilePhone    = getStr        (obj, "mobilePhone");
    c.homePhones     = getStringArray(obj, "homePhones");
    c.imAddresses    = getStringArray(obj, "imAddresses");

    if (const JsonValue* a = find(obj, "businessAddress"))
        c.businessAddress = extractAddress(*a);
    if (const JsonValue* a = find(obj, "homeAddress"))
        c.homeAddress = extractAddress(*a);
    if (const JsonValue* a = find(obj, "otherAddress"))
        c.otherAddress = extractAddress(*a);

    c.businessHomePage = getStr(obj, "businessHomePage");
    c.personalNotes    = getStr(obj, "personalNotes");

    c.categories = getStringArray(obj, "categories");

    return c;
}

} // namespace

GraphContact parseGraphContact(const string& json)
{
    Parser p(json);
    JsonValue v = p.parseTopLevel();
    if (v.type != JsonType::Object)
        throw std::runtime_error("graph_contact JSON: expected top-level object");
    return extractContact(v);
}

vector<GraphContact> parseGraphContactList(const string& json)
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
        throw std::runtime_error("graph_contact JSON: expected array or {value:[...]}");

    vector<GraphContact> out;
    out.reserve(arr->size());
    for (const auto& el : *arr) {
        if (el.type != JsonType::Object) continue;
        out.push_back(extractContact(el));
    }
    return out;
}

} // namespace graph
} // namespace pstwriter
