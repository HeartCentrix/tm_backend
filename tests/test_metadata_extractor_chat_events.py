from shared.metadata_extractor import MetadataExtractor


def _system_msg(odata_type, **payload):
    return {
        "id": "msg1",
        "messageType": "systemEventMessage",
        "createdDateTime": "2026-03-12T10:42:00Z",
        "from": None,
        "body": {"contentType": "html", "content": ""},
        "eventDetail": {"@odata.type": odata_type, **payload},
    }


def test_extract_call_ended_event():
    m = _system_msg(
        "#microsoft.graph.callEndedEventMessageDetail",
        callEventType="call",
        callDuration="PT3M12S",
        initiator={"user": {"id": "u1", "displayName": "Amit Mishra"}},
        callParticipants=[
            {"participant": {"user": {"displayName": "Amit Mishra"}}},
            {"participant": {"user": {"displayName": "Akshat Verma"}}},
        ],
    )
    out = MetadataExtractor.extract_teams_chat_message_metadata(m)
    ed = out["event_detail"]
    assert ed is not None
    assert ed["kind"] == "call_ended"
    assert ed["initiator"]["display_name"] == "Amit Mishra"
    assert ed["call_duration"] == "PT3M12S"
    assert len(ed["participants"]) == 2
    assert ed["raw_odata_type"] == "#microsoft.graph.callEndedEventMessageDetail"


def test_extract_members_added_event():
    m = _system_msg(
        "#microsoft.graph.membersAddedEventMessageDetail",
        initiator={"user": {"displayName": "Akshat Verma"}},
        members=[{"displayName": "Vinay Chauhan", "id": "u9"}],
    )
    ed = MetadataExtractor.extract_teams_chat_message_metadata(m)["event_detail"]
    assert ed["kind"] == "members_added"
    assert ed["members"][0]["display_name"] == "Vinay Chauhan"


def test_extract_chat_renamed_event():
    m = _system_msg(
        "#microsoft.graph.chatRenamedEventMessageDetail",
        initiator={"user": {"displayName": "Amit Mishra"}},
        chatDisplayName="Project Alpha",
    )
    ed = MetadataExtractor.extract_teams_chat_message_metadata(m)["event_detail"]
    assert ed["kind"] == "chat_renamed"
    assert ed["new_chat_name"] == "Project Alpha"


def test_extract_unknown_event_preserves_odata_type():
    m = _system_msg("#microsoft.graph.fooBarEventMessageDetail")
    ed = MetadataExtractor.extract_teams_chat_message_metadata(m)["event_detail"]
    assert ed["kind"] == "unknown"
    assert ed["raw_odata_type"].endswith("fooBarEventMessageDetail")


def test_extract_normal_message_has_no_event_detail():
    m = {
        "id": "msg2", "messageType": "message",
        "createdDateTime": "2026-03-12T10:42:00Z",
        "from": {"user": {"id": "u1", "displayName": "A"}},
        "body": {"contentType": "html", "content": "hi"},
    }
    out = MetadataExtractor.extract_teams_chat_message_metadata(m)
    assert out["event_detail"] is None


def test_hosted_content_ids_collected():
    m = {
        "id": "msg3", "messageType": "message",
        "createdDateTime": "2026-03-12T10:42:00Z",
        "from": {"user": {"id": "u1", "displayName": "A"}},
        "body": {"contentType": "html", "content": "<img src='hc/1/$value'>"},
        "hostedContents": [{"id": "hc1"}, {"id": "hc2"}],
    }
    out = MetadataExtractor.extract_teams_chat_message_metadata(m)
    assert out["hosted_content_ids"] == ["hc1", "hc2"]
