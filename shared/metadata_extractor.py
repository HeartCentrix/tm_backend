"""Metadata Extractor - Structured metadata extraction for backup items

Extracts permissions, relationships, thread structure, and other metadata
from raw Graph API responses to enable full-fidelity restore.
"""
from typing import Dict, Any, Optional, List
from datetime import datetime


class MetadataExtractor:
    """Extract structured metadata from Graph API responses for backup items"""

    # ==================== SharePoint Metadata ====================

    @staticmethod
    def extract_sharepoint_item_metadata(item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from SharePoint drive item (file/folder)"""
        return {
            "type": "sharepoint_file" if item.get("file") else "sharepoint_folder",
            "web_url": item.get("webUrl"),
            "size": item.get("size", 0),
            "created_by": {
                "user_id": item.get("createdBy", {}).get("user", {}).get("id"),
                "display_name": item.get("createdBy", {}).get("user", {}).get("displayName"),
            },
            "last_modified_by": {
                "user_id": item.get("lastModifiedBy", {}).get("user", {}).get("id"),
                "display_name": item.get("lastModifiedBy", {}).get("user", {}).get("displayName"),
            },
            "created_at": item.get("createdDateTime"),
            "last_modified_at": item.get("lastModifiedDateTime"),
            "parent_reference": item.get("parentReference", {}),
            "file_hash": item.get("file", {}).get("hashes", {}),
            "mime_type": item.get("file", {}).get("mimeType"),
            "folder_child_count": item.get("folder", {}).get("childCount"),
        }

    @staticmethod
    def extract_sharepoint_list_item_metadata(item: Dict[str, Any], list_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from SharePoint list item"""
        fields = item.get("fields", {})
        return {
            "list_id": list_info.get("id"),
            "list_name": list_info.get("displayName") or list_info.get("name"),
            "content_type": item.get("contentType", {}).get("name"),
            "created_at": item.get("createdDateTime"),
            "last_modified_at": item.get("lastModifiedDateTime"),
            "field_values": fields,
        }

    @staticmethod
    def extract_permissions_metadata(permissions_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract SharePoint site permissions metadata"""
        permissions = []
        for perm in permissions_data.get("value", []):
            permissions.append({
                "id": perm.get("id"),
                "roles": perm.get("roles", []),
                "granted_to_identities": [
                    {
                        "type": "user" if "user" in identity else "group" if "group" in identity else "application",
                        "id": identity.get("user", identity.get("group", identity.get("application", {}))).get("id"),
                        "display_name": identity.get("user", identity.get("group", identity.get("application", {}))).get("displayName"),
                    }
                    for identity in perm.get("grantedToIdentities", [])
                ],
                "link": perm.get("link", {}),
                "has_password": perm.get("hasPassword", False),
                "expiration": perm.get("expirationDateTime"),
            })

        return {
            "type": "sharepoint_permissions",
            "permissions": permissions,
            "total_count": len(permissions),
        }

    # ==================== Teams Metadata ====================

    @staticmethod
    def extract_teams_channel_metadata(channel: Dict[str, Any], team_id: str) -> Dict[str, Any]:
        """Extract Teams channel metadata"""
        return {
            "type": "teams_channel",
            "team_id": team_id,
            "channel_id": channel.get("id"),
            "display_name": channel.get("displayName"),
            "description": channel.get("description"),
            "web_url": channel.get("webUrl"),
            "email": channel.get("email"),
            "membership_type": channel.get("membershipType", "standard"),
            "created_at": channel.get("createdDateTime"),
        }

    @staticmethod
    def extract_teams_message_metadata(message: Dict[str, Any], is_reply: bool = False) -> Dict[str, Any]:
        """Extract Teams message metadata with thread structure"""
        body = message.get("body", {})
        from_info = message.get("from", {})
        user = from_info.get("user", {})

        # Extract attachments/hosted content info
        attachments = message.get("attachments", [])
        hosted_contents = message.get("hostedContents", [])

        # Extract mentions
        mentions = message.get("mentions", [])

        # Extract reactions
        reactions = message.get("reactions", [])

        return {
            "type": "teams_message_reply" if is_reply else "teams_message",
            "message_id": message.get("id"),
            "reply_to_id": message.get("replyToId"),
            "etag": message.get("etag"),
            "message_type": message.get("messageType"),
            "created_at": message.get("createdDateTime"),
            "last_modified_at": message.get("lastModifiedDateTime"),
            "last_edited_at": message.get("lastEditedDateTime"),
            "deleted_at": message.get("deletedDateTime"),
            "subject": message.get("subject"),
            "summary": message.get("summary"),
            "importance": message.get("importance"),
            "locale": message.get("locale"),
            "web_url": message.get("webUrl"),
            "body": {
                "content_type": body.get("contentType"),
                "content_preview": body.get("content", "")[:500] if body.get("content") else "",
            },
            "from": {
                "user_id": user.get("id"),
                "display_name": user.get("displayName"),
                "tenant_id": user.get("tenantId"),
            },
            "thread_info": {
                "is_reply": is_reply,
                "reply_to_id": message.get("replyToId"),
                "has_attachments": message.get("hasAttachments", False),
                "attachment_count": len(attachments),
                "hosted_content_count": len(hosted_contents),
            },
            "attachments": [
                {
                    "id": att.get("id"),
                    "content_type": att.get("contentType"),
                    "content_url": att.get("contentUrl"),
                    "name": att.get("name"),
                    "thumbnail_url": att.get("thumbnailUrl"),
                }
                for att in attachments
            ],
            "mentions": [
                {
                    "id": mention.get("id"),
                    "mention_text": mention.get("mentionText"),
                    "mentioned": mention.get("mentioned", {}),
                }
                for mention in mentions
            ],
            "reactions": [
                {
                    "reaction_type": reaction.get("reactionType"),
                    "created_at": reaction.get("createdDateTime"),
                    "user": {
                        "user_id": reaction.get("user", {}).get("user", {}).get("id"),
                        "display_name": reaction.get("user", {}).get("user", {}).get("displayName"),
                    },
                }
                for reaction in reactions
            ],
        }

    @staticmethod
    def extract_teams_chat_metadata(resource) -> Dict[str, Any]:
        """Extract Teams chat metadata from resource"""
        extra_data = resource.extra_data or {}
        return {
            "type": "teams_chat",
            "chat_id": resource.external_id,
            "display_name": resource.display_name,
            "chat_type": extra_data.get("chatType"),
            "members": extra_data.get("members", []),
            "created_at": extra_data.get("createdDateTime"),
            "last_updated_at": extra_data.get("lastUpdatedDateTime"),
        }

    # ==================== Entra ID Metadata ====================

    @staticmethod
    def extract_entra_user_metadata(user_profile: Dict[str, Any]) -> Dict[str, Any]:
        """Extract comprehensive Entra ID user profile metadata"""
        return {
            "type": "entra_user_profile",
            "user_id": user_profile.get("id"),
            "user_principal_name": user_profile.get("userPrincipalName"),
            "display_name": user_profile.get("displayName"),
            "given_name": user_profile.get("givenName"),
            "surname": user_profile.get("surname"),
            "mail": user_profile.get("mail"),
            "mail_nickname": user_profile.get("mailNickname"),
            "job_title": user_profile.get("jobTitle"),
            "department": user_profile.get("department"),
            "company_name": user_profile.get("companyName"),
            "office_location": user_profile.get("officeLocation"),
            "mobile_phone": user_profile.get("mobilePhone"),
            "business_phones": user_profile.get("businessPhones", []),
            "preferred_language": user_profile.get("preferredLanguage"),
            "account_enabled": user_profile.get("accountEnabled", True),
            "created_at": user_profile.get("createdDateTime"),
            "usage_location": user_profile.get("usageLocation"),
            "assigned_licenses": [
                {"sku_id": lic.get("skuId"), "disabled_plans": lic.get("disabledPlans", [])}
                for lic in user_profile.get("assignedLicenses", [])
            ],
            "assigned_plans": [
                {"service": plan.get("service"), "capability_status": plan.get("capabilityStatus")}
                for plan in user_profile.get("assignedPlans", [])
            ],
            "proxy_addresses": user_profile.get("proxyAddresses", []),
            "on_premises_sync_enabled": user_profile.get("onPremisesSyncEnabled"),
            "on_premises_immutable_id": user_profile.get("onPremisesImmutableId"),
        }

    @staticmethod
    def extract_entra_group_metadata(group_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Entra ID group metadata"""
        return {
            "type": "entra_group",
            "group_id": group_data.get("id"),
            "display_name": group_data.get("displayName"),
            "description": group_data.get("description"),
            "mail": group_data.get("mail"),
            "mail_enabled": group_data.get("mailEnabled"),
            "mail_nickname": group_data.get("mailNickname"),
            "security_enabled": group_data.get("securityEnabled"),
            "group_types": group_data.get("groupTypes", []),
            "visibility": group_data.get("visibility"),
            "created_at": group_data.get("createdDateTime"),
            "membership_rule": group_data.get("membershipRule"),
            "membership_rule_processing_state": group_data.get("membershipRuleProcessingState"),
            "proxy_addresses": group_data.get("proxyAddresses", []),
        }

    @staticmethod
    def extract_relationship_metadata(related_user: Dict[str, Any], relationship_type: str, user_id: str) -> Dict[str, Any]:
        """Extract relationship metadata (manager, direct report)"""
        return {
            "type": "entra_relationship",
            "relationship_type": relationship_type,
            "user_id": user_id,
            "related_user_id": related_user.get("id"),
            "related_user_principal_name": related_user.get("userPrincipalName"),
            "related_display_name": related_user.get("displayName"),
            "related_mail": related_user.get("mail"),
            "related_job_title": related_user.get("jobTitle"),
            "related_department": related_user.get("department"),
        }

    @staticmethod
    def extract_membership_metadata(group: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        """Extract group membership metadata"""
        # Determine group type
        group_types = group.get("groupTypes", [])
        if "Unified" in group_types:
            membership_type = "Microsoft 365 Group / Team"
        elif group.get("securityEnabled"):
            membership_type = "Security Group"
        else:
            membership_type = "Distribution Group"

        return {
            "type": "entra_membership",
            "user_id": user_id,
            "group_id": group.get("id"),
            "group_display_name": group.get("displayName"),
            "group_type": membership_type,
            "group_mail": group.get("mail"),
            "group_description": group.get("description"),
        }

    @staticmethod
    def extract_contact_metadata(contact: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        """Extract contact metadata"""
        return {
            "type": "entra_contact",
            "user_id": user_id,
            "contact_id": contact.get("id"),
            "display_name": contact.get("displayName"),
            "given_name": contact.get("givenName"),
            "surname": contact.get("surname"),
            "email_addresses": contact.get("emailAddresses", []),
            "business_phones": contact.get("businessPhones", []),
            "mobile_phone": contact.get("mobilePhone"),
            "company_name": contact.get("companyName"),
            "job_title": contact.get("jobTitle"),
            "department": contact.get("department"),
            "office_location": contact.get("officeLocation"),
        }

    @staticmethod
    def extract_calendar_event_metadata(event: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        """Extract calendar event metadata"""
        organizer = event.get("organizer", {})
        attendees = event.get("attendees", [])

        return {
            "type": "entra_calendar_event",
            "user_id": user_id,
            "event_id": event.get("id"),
            "subject": event.get("subject"),
            "body_preview": event.get("bodyPreview"),
            "importance": event.get("importance"),
            "sensitivity": event.get("sensitivity"),
            "show_as": event.get("showAs"),
            "is_all_day": event.get("isAllDay", False),
            "is_cancelled": event.get("isCancelled", False),
            "is_organizer": event.get("isOrganizer", False),
            "start": event.get("start"),
            "end": event.get("end"),
            "location": event.get("location", {}),
            "locations": event.get("locations", []),
            "organizer": {
                "name": organizer.get("emailAddress", {}).get("name"),
                "email": organizer.get("emailAddress", {}).get("address"),
            },
            "attendee_count": len(attendees),
            "attendees": [
                {
                    "name": att.get("emailAddress", {}).get("name"),
                    "email": att.get("emailAddress", {}).get("address"),
                    "status": att.get("status", {}).get("response"),
                }
                for att in attendees[:10]  # Limit to first 10 attendees
            ],
            "recurrence": event.get("recurrence"),
            "online_meeting": event.get("onlineMeeting", {}),
            "categories": event.get("categories", []),
        }

    @staticmethod
    def extract_group_member_metadata(member: Dict[str, Any], group_id: str) -> Dict[str, Any]:
        """Extract group member metadata"""
        return {
            "type": "entra_group_member",
            "group_id": group_id,
            "member_id": member.get("id"),
            "member_type": "user" if "userPrincipalName" in member else "group",
            "display_name": member.get("displayName"),
            "mail": member.get("mail"),
            "user_principal_name": member.get("userPrincipalName"),
        }

    @staticmethod
    def extract_group_owner_metadata(owner: Dict[str, Any], group_id: str) -> Dict[str, Any]:
        """Extract group owner metadata"""
        return {
            "type": "entra_group_owner",
            "group_id": group_id,
            "owner_id": owner.get("id"),
            "owner_type": "user" if "userPrincipalName" in owner else "group",
            "display_name": owner.get("displayName"),
            "mail": owner.get("mail"),
            "user_principal_name": owner.get("userPrincipalName"),
        }
