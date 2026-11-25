"""Poll Gmail's Inbox history for an opted-in project connection."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from email.utils import parseaddr
from typing import Any

from flowsteward_extension_sdk import host_mutation

from .errors import GmailExtensionError
from .messages import _internal_date
from .transport import GmailTransport
from .validation import access_token, connection_payload, mapping

POLL_ACTION_ID = "poll_gmail_inbox"
STATUS_QUERY_ID = "get_gmail_trigger_settings"
EVENT_TYPE = "google_workspace.gmail.message.received"
MODE_FIELD = "gmail_trigger_mode"
STATE_FIELD = "gmail_sync"
CONNECTION_TYPE = "google_gmail_account"
MAX_MESSAGES_PER_POLL = 12
REQUEST_TIMEOUT_SECONDS = 4
POLL_BUDGET_SECONDS = 20


def poll_inbox(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        connection = connection_payload(payload)
        ingress = mapping(connection.get("ingress_config"))
        if ingress.get(MODE_FIELD) != "polling":
            return {"ok": True, "result": {"status": "disabled"}, "events": []}

        connection_id = str(connection.get("connection_id") or "")
        if not connection_id:
            raise GmailExtensionError("invalid_connection", "A Gmail connection is required")
        token = access_token(payload, connection_ref=connection_id)
        transport = GmailTransport(token, timeout_seconds=REQUEST_TIMEOUT_SECONDS)
        state = mapping(ingress.get(STATE_FIELD))
        generation = connection.get("config_generation")
        now = datetime.now(UTC)
        now_ms = int(now.timestamp() * 1000)
        if state.get("config_generation") != generation or not state.get("history_id"):
            state = {
                "config_generation": generation,
                "history_id": _history_id(transport.profile().get("historyId")),
                "checkpoint_ms": now_ms,
            }
            status, events = "initialized", []
        else:
            status, events = _poll_history(transport, state, connection_id, now_ms)

        mutation = host_mutation(
            "project_connection_upsert",
            {
                "connection_id": connection_id,
                "connection_type": CONNECTION_TYPE,
                "ingress_config": {
                    **ingress,
                    STATE_FIELD: state,
                    "last_inbound_sync_at": now.isoformat(),
                },
                "expected_state_version": connection.get("state_version"),
                "expected_config_generation": generation,
            },
        )
        return {
            "ok": True,
            "result": {"status": status, "processed_updates": len(events)},
            "events": events,
            "host_mutations": [mutation],
        }
    except GmailExtensionError as exc:
        return _error(exc)


def trigger_settings(payload: dict[str, Any]) -> dict[str, Any]:
    runtime = mapping(payload.get("runtime_context"))
    connection = mapping(mapping(runtime.get("resources")).get("project_connection"))
    if (
        not runtime.get("connection_id")
        or connection.get("connection_id") != runtime["connection_id"]
        or connection.get("connection_type") != CONNECTION_TYPE
    ):
        return _error(GmailExtensionError("invalid_connection", "Select a Gmail connection"))
    ingress = mapping(connection.get("ingress_config"))
    state = mapping(ingress.get(STATE_FIELD))
    summary = {
        "Connection ID": connection["connection_id"],
        "Current status": "On" if ingress.get(MODE_FIELD) == "polling" else "Off",
    }
    if ingress.get(MODE_FIELD) == "polling" and state.get("recovery_end_ms"):
        summary["History"] = "Recovering recent messages"
    return {
        "ok": True,
        "result": {"summary": summary},
    }


def _poll_history(
    transport: GmailTransport, state: dict[str, Any], connection_id: str, now_ms: int
) -> tuple[str, list[dict[str, Any]]]:
    deadline = time.monotonic() + POLL_BUDGET_SECONDS
    page_token = state.get("page_token")
    recovery_end = state.get("recovery_end_ms")
    try:
        if recovery_end is None:
            page = transport.message_history(_history_id(state["history_id"]), page_token)
            message_ids = _added_message_ids(page)
            latest = _history_id(page.get("historyId"))
        else:
            page = _recovery_page(transport, state["checkpoint_ms"], recovery_end, page_token)
            message_ids = _message_ids(page.get("messages", []))
            latest = state["history_id"]
    except GmailExtensionError as exc:
        if page_token and exc.http_status in {400, 404}:
            # Retry with just the committed history ID or time range.
            # Core deduplicates events already accepted from earlier pages.
            state.pop("page_token", None)
            state.pop("through_message_id", None)
            return "pagination_restarted", []
        if recovery_end is not None or exc.code != "message_not_found":
            raise
        # Take a fresh history position before scanning the time gap. Mail
        # arriving during recovery will also appear in subsequent history.
        state.update(
            history_id=_history_id(transport.profile().get("historyId")),
            recovery_end_ms=max(now_ms, int(datetime.now(UTC).timestamp() * 1000)),
        )
        state.pop("through_message_id", None)
        state.pop("scan_started_ms", None)
        return "recovering", []

    state.setdefault("scan_started_ms", now_ms)
    through_message_id = state.get("through_message_id")
    # Recovery pages can change as messages leave the Inbox. Resume by ID or
    # replay the page if that message disappeared.
    offset = message_ids.index(through_message_id) + 1 if through_message_id in message_ids else 0
    events = []
    for message_id in message_ids[offset : offset + MAX_MESSAGES_PER_POLL]:
        if time.monotonic() + REQUEST_TIMEOUT_SECONDS >= deadline:
            break
        event = _message_event(
            transport,
            connection_id,
            message_id,
            received_after_ms=state["checkpoint_ms"] if recovery_end is not None else None,
        )
        if event is not None:
            events.append(event)
        offset += 1
    if offset < len(message_ids):
        if offset:
            state["through_message_id"] = message_ids[offset - 1]
        return "partial", events

    state.pop("through_message_id", None)
    next_token = page.get("nextPageToken")
    if next_token:
        if not isinstance(next_token, str) or next_token == page_token or len(next_token) > 4096:
            raise GmailExtensionError("provider_unavailable", "Invalid Gmail history page token")
        state["page_token"] = next_token
        return "polled", events

    state.pop("page_token", None)
    state["history_id"] = latest
    # Using the first request time keeps the recovery range conservative even
    # when a history scan needed several polls.
    scan_start = state.pop("scan_started_ms")
    state["checkpoint_ms"] = recovery_end if recovery_end is not None else scan_start
    if recovery_end is not None:
        state.pop("recovery_end_ms")
        return "recovered", events
    return "polled", events


def _recovery_page(
    transport: GmailTransport, after_ms: int, before_ms: int, page_token: str | None
) -> dict[str, Any]:
    return transport.messages_list(
        {
            "labelIds": "INBOX",
            "q": f"after:{max(0, after_ms // 1000 - 1)} before:{before_ms // 1000 + 1}",
            "maxResults": MAX_MESSAGES_PER_POLL,
            "pageToken": page_token,
            "fields": "messages(id),nextPageToken",
        }
    )


def _message_ids(rows: Any) -> list[str]:
    if not isinstance(rows, list):
        raise GmailExtensionError("provider_unavailable", "Invalid Gmail message list")
    message_ids = {}
    for row in rows:
        message_id = mapping(row).get("id")
        if not isinstance(message_id, str) or not message_id or len(message_id) > 128:
            raise GmailExtensionError("provider_unavailable", "Invalid Gmail message ID")
        message_ids[message_id] = None
    return list(message_ids)


def _added_message_ids(page: dict[str, Any]) -> list[str]:
    rows = page.get("history", [])
    if not isinstance(rows, list):
        raise GmailExtensionError("provider_unavailable", "Invalid Gmail history")
    messages = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("messagesAdded", []), list):
            raise GmailExtensionError("provider_unavailable", "Invalid Gmail history entry")
        messages.extend(mapping(added).get("message") for added in row.get("messagesAdded", []))
    return _message_ids(messages)


def _message_event(
    transport: GmailTransport,
    connection_id: str,
    message_id: str,
    *,
    received_after_ms: int | None,
) -> dict[str, Any] | None:
    try:
        message = transport.message_metadata(message_id)
    except GmailExtensionError as exc:
        if exc.code == "message_not_found":
            # A message deleted between the history and metadata calls is gone.
            return None
        raise
    labels = message.get("labelIds", [])
    if message.get("id") != message_id or not isinstance(labels, list):
        raise GmailExtensionError("provider_unavailable", "Invalid Gmail message metadata")
    if "INBOX" not in labels or any(label in labels for label in ("SPAM", "TRASH", "DRAFT")):
        return None
    headers = {}
    for row in mapping(message.get("payload")).get("headers") or []:
        header = mapping(row)
        name = str(header.get("name") or "").lower()
        headers.setdefault(name, str(header.get("value") or ""))
    received = _internal_date(message.get("internalDate"))
    if received_after_ms is not None and int(message["internalDate"]) < received_after_ms:
        return None
    sender = parseaddr(headers.get("from", "")[:2048])[1].lower()[:320]
    source_id = f"gmail:{connection_id}:{message_id}"
    return {
        "event_type": EVENT_TYPE,
        "source_id": source_id,
        "correlation_id": source_id,
        "source": "flowsteward.google-workspace",
        "severity": "info",
        "summary": "New Gmail email",
        "payload": {
            "connection_id": connection_id,
            "message_id": message_id,
            "thread_id": str(message.get("threadId") or "")[:128],
            "from": sender,
            "subject": headers.get("subject", "")[:512],
            "received_at": received,
        },
    }


def _history_id(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 32 or not value.isdecimal():
        raise GmailExtensionError("provider_unavailable", "Invalid Gmail history ID")
    return value


def _error(exc: GmailExtensionError) -> dict[str, Any]:
    return {
        "ok": False,
        "result": {},
        "error_code": exc.code,
        "error": exc.message,
        "errors": [{"code": exc.code, "message": exc.message}],
        **exc.retry_facts(),
    }
