from __future__ import annotations

from typing import Any

import dispatcher
from conftest import BUNDLE_ROOT, HOST_CONTRACT_VERSION
import yaml

from gmail.errors import GmailExtensionError
from gmail.transport import GmailTransport


def _payload(
    *, mode: str = "polling", state: dict | None = None, generation: int = 3
) -> dict[str, Any]:
    return {
        "contract_version": HOST_CONTRACT_VERSION,
        "mode": "action",
        "runtime_context": {
            "group_id": None,
            "for_each_id": None,
            "step_id": None,
            "step_execution_id": None,
        },
        "action": {
            "action_id": "poll_gmail_inbox",
            "input": {},
            "target": {
                "connection": {
                    "connection_id": "gmail-1",
                    "connection_type_id": "google_gmail_account",
                    "secrets": {"access_token": "test-token"},
                    "ingress_config": {
                        "gmail_trigger_mode": mode,
                        **({"gmail_sync": state} if state is not None else {}),
                    },
                    "state_version": 7,
                    "config_generation": generation,
                }
            },
        },
    }


def _state(response: dict[str, Any]) -> dict[str, Any]:
    return response["host_mutations"][0]["payload"]["ingress_config"]["gmail_sync"]


def _history(*message_ids: str, latest: str = "109", token: str = "") -> dict[str, Any]:
    return {
        "historyId": latest,
        **({"nextPageToken": token} if token else {}),
        "history": [
            {
                "messagesAdded": [{"message": {"id": item}} for item in message_ids],
                "labelsAdded": [{"message": {"id": "old-relabelled"}}],
            }
        ],
    }


def _mock_gmail(monkeypatch, pages=(), *, listed=(), missing=(), archived=()):
    calls = []
    history = iter(pages)

    def request(_self, method, path, *, query=None, **_kwargs):
        calls.append((method, path, query))
        if path.endswith("/profile"):
            return {"historyId": "100"}
        if path.endswith("/history"):
            page = next(history)
            if isinstance(page, Exception):
                raise page
            return page
        if path.endswith("/messages"):
            return {"messages": [{"id": message_id} for message_id in listed]}
        message_id = path.rsplit("/", 1)[-1]
        if message_id in missing:
            raise GmailExtensionError("message_not_found", "Message was deleted", http_status=404)
        return {
            "id": message_id,
            "threadId": "thread",
            "labelIds": ["UNREAD"] if message_id in archived else ["INBOX", "UNREAD"],
            "internalDate": "1788871920000",
            "payload": {
                "headers": [
                    {"name": "From", "value": "Example <SENDER@example.com>"},
                    {"name": "Subject", "value": "Invoice"},
                ]
            },
        }

    monkeypatch.setattr(GmailTransport, "request_json", request)
    return calls


def test_gmail_declares_an_opt_in_trigger_for_a_selected_connection():
    manifest = yaml.safe_load((BUNDLE_ROOT / "extension.yaml").read_text())
    (trigger,) = manifest.get("workflow_triggers", [])
    assert trigger["event_name"] == "google_workspace.gmail.message.received"
    assert trigger["config_schema"]["required"] == ["connection_id"]
    assert trigger["event_name"] in {row["event_type"] for row in manifest["event_subscriptions"]}
    connections = yaml.safe_load((BUNDLE_ROOT / "contracts/connection_types.yaml").read_text())
    gmail = next(
        row
        for row in connections["connection_types"]
        if row["connection_type_id"] == "google_gmail_account"
    )
    assert gmail["ingress_schema"]["properties"]["gmail_trigger_mode"]["default"] == "off"
    page = yaml.safe_load((BUNDLE_ROOT / "ui/pages/gmail-triggers.yaml").read_text())
    assert page["connection"]["connection_type_ids"] == ["google_gmail_account"]
    (form,) = page["components"]
    assert form["data"]["save_action"] == "save_gmail_trigger_settings"
    assert form["data"]["load_query"] == "get_gmail_trigger_settings"
    action_manifest = yaml.safe_load((BUNDLE_ROOT / "ui/actions/actions.yaml").read_text())
    actions = {row["action_id"]: row for row in action_manifest["actions"]}
    save = actions["save_gmail_trigger_settings"]
    assert save["handler"]["operation"] == "project_connection_ingress_patch"
    assert save["handler"]["config"] == {
        "connection_type_ids": ["google_gmail_account"],
        "fields": ["gmail_trigger_mode"],
    }
    poll = actions["poll_gmail_inbox"]
    assert poll["connection_type_ids"] == ["google_gmail_account"]
    assert poll["contract"]["background"]["mode_field"] == "gmail_trigger_mode"


def test_disabled_mailbox_makes_no_provider_call_or_mutation(monkeypatch):
    calls = _mock_gmail(monkeypatch)
    response = dispatcher.dispatch_runtime(_payload(mode="off"))
    assert response["ok"] and response["result"]["status"] == "disabled"
    assert "host_mutations" not in response
    assert calls == []


def test_first_poll_and_reenable_start_from_current_history(monkeypatch):
    calls = _mock_gmail(monkeypatch)
    first = dispatcher.dispatch_runtime(_payload())
    assert first["events"] == []
    assert _state(first)["history_id"] == "100"
    second = dispatcher.dispatch_runtime(
        _payload(state={**_state(first), "history_id": "90"}, generation=5)
    )
    assert second["events"] == []
    assert _state(second)["history_id"] == "100"
    assert [path.rsplit("/", 1)[-1] for _, path, _ in calls] == ["profile", "profile"]


def test_poll_emits_one_event_per_inbox_message_and_resumes_next_page(monkeypatch):
    calls = _mock_gmail(
        monkeypatch,
        [_history("a", "b", "a", "gone", "archived", token="next"), _history("c", latest="118")],
        missing=("gone",),
        archived=("archived",),
    )
    initial = dispatcher.dispatch_runtime(_payload())
    first = dispatcher.dispatch_runtime(_payload(state=_state(initial)))
    second = dispatcher.dispatch_runtime(_payload(state=_state(first)))
    assert [event["payload"]["message_id"] for event in first["events"]] == ["a", "b"]
    assert [event["payload"]["message_id"] for event in second["events"]] == ["c"]
    assert [event["source_id"] for event in first["events"]] == [
        "gmail:gmail-1:a",
        "gmail:gmail-1:b",
    ]
    assert first["events"][0]["payload"]["from"] == "sender@example.com"
    assert first["events"][0]["payload"]["connection_id"] == "gmail-1"
    assert first["host_mutations"][0]["payload"]["expected_state_version"] == 7
    assert first["host_mutations"][0]["payload"]["expected_config_generation"] == 3
    assert _state(second)["history_id"] == "118"
    history_calls = [query for _, path, query in calls if path.endswith("/history")]
    assert [query["startHistoryId"] for query in history_calls] == ["100", "100"]
    assert history_calls[1]["pageToken"] == "next"


def test_temporary_gmail_failure_leaves_cursor_available_for_retry(monkeypatch):
    _mock_gmail(monkeypatch, [GmailExtensionError("rate_limited", "Try again", http_status=429)])
    initial = dispatcher.dispatch_runtime(_payload())
    response = dispatcher.dispatch_runtime(_payload(state=_state(initial)))
    assert response["error_code"] == "rate_limited"
    assert "host_mutations" not in response


def test_large_history_page_resumes_in_the_middle(monkeypatch):
    from gmail import trigger

    monkeypatch.setattr(trigger, "MAX_MESSAGES_PER_POLL", 2)
    _mock_gmail(monkeypatch, [_history("a", "b", "c", "d"), _history("b", "c", "d")])
    initial = dispatcher.dispatch_runtime(_payload())
    first = dispatcher.dispatch_runtime(_payload(state=_state(initial)))
    assert _state(first)["through_message_id"] == "b"
    assert _state(first)["history_id"] == "100"
    second = dispatcher.dispatch_runtime(_payload(state=_state(first)))
    assert [event["payload"]["message_id"] for event in second["events"]] == ["c", "d"]
    assert _state(second)["history_id"] == "109"
    assert "through_message_id" not in _state(second)


def test_expired_history_recovers_the_inbox_then_resumes_history(monkeypatch):
    calls = _mock_gmail(
        monkeypatch,
        [
            GmailExtensionError("message_not_found", "History expired", http_status=404),
            _history("during-recovery", latest="120"),
        ],
        listed=("missed",),
    )
    expired = dispatcher.dispatch_runtime(
        _payload(state={"config_generation": 3, "history_id": "20", "checkpoint_ms": 1788871919000})
    )
    assert expired["result"]["status"] == "recovering"
    assert expired["events"] == []
    assert _state(expired)["history_id"] == "100"
    recovered = dispatcher.dispatch_runtime(_payload(state=_state(expired)))
    assert recovered["result"]["status"] == "recovered"
    assert [event["payload"]["message_id"] for event in recovered["events"]] == ["missed"]
    assert "recovery_end_ms" not in _state(recovered)
    live = dispatcher.dispatch_runtime(_payload(state=_state(recovered)))
    assert [event["payload"]["message_id"] for event in live["events"]] == ["during-recovery"]
    assert _state(live)["history_id"] == "120"
    recovery_query = next(query for _, path, query in calls if path.endswith("/messages"))
    assert recovery_query["q"].startswith("after:1788871918 before:")
    assert recovery_query["labelIds"] == "INBOX"


def test_settings_shows_the_selected_connection_without_returning_cursor(monkeypatch):
    calls = _mock_gmail(monkeypatch)
    payload = _payload()
    connection = payload.pop("action")["target"]["connection"]
    payload["mode"] = "query"
    payload["query"] = {"query_id": "get_gmail_trigger_settings"}
    payload["runtime_context"].update(
        connection_id="gmail-1",
        resources={
            "project_connection": {
                "connection_id": "gmail-1",
                "connection_type": "google_gmail_account",
                "ingress_config": {},
                "secrets": connection["secrets"],
            }
        },
    )
    assert dispatcher.dispatch_runtime(payload) == {
        "ok": True,
        "result": {"summary": {"Connection ID": "gmail-1", "Current status": "Off"}},
    }
    payload["runtime_context"]["connection_id"] = "gmail-2"
    assert dispatcher.dispatch_runtime(payload)["error_code"] == "invalid_connection"
    assert calls == []
