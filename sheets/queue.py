"""Claim and complete Google Sheets queue rows across full-row sorts."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

from .errors import SheetsExtensionError
from .ranges import A1Bounds, cell_coordinates, column_number, read_ranges, selected_bounds
from .reads import column_headers, selected_sheet, values_page
from .transport import SheetsTransport
from .validation import effect_operation_id, enum, validated_read_input

_SELECTOR_FIELDS = {"connection_ref", "spreadsheet_id", "sheet_id", "sheet_title"}
_FIELDS = _SELECTOR_FIELDS | {"mode", "table_range", "status_column", "status_cell", "claim_marker"}
_MARKER = re.compile(r"Processing \[FS [0-9a-f]{32}\]")
_CONFLICT_MESSAGE = "The claimed status was removed, moved outside the table, or duplicated"


def validated_queue_input(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) - _FIELDS:
        raise SheetsExtensionError("invalid_payload", "Google Sheets queue input is invalid")
    mode = enum(value.get("mode"), {"claim", "complete"})
    selection = validated_read_input(
        {
            **{key: value[key] for key in _SELECTOR_FIELDS & value.keys()},
            "range": value.get("table_range"),
        }
    )
    table_range = selection["range"]
    endpoints = table_range.split(":")
    if (
        len(endpoints) != 2
        or cell_coordinates(endpoints[0])[1] == cell_coordinates(endpoints[1])[1]
    ):
        raise SheetsExtensionError(
            "invalid_payload", "Use a bounded table range with a header and data rows"
        )
    column = value.get("status_column")
    if not isinstance(column, str) or not re.fullmatch(r"[A-Za-z]{1,3}", column):
        raise SheetsExtensionError("invalid_payload", "Use an A1 status column such as B")

    if mode == "claim":
        if "claim_marker" in value:
            raise SheetsExtensionError(
                "invalid_payload", "Claim takes a status cell, without a marker"
            )
        coordinates = cell_coordinates(value.get("status_cell"))
        return {
            **selection,
            "mode": mode,
            "column": column_number(column),
            "status_cell": coordinates,
        }

    marker = value.get("claim_marker")
    if "status_cell" in value or not isinstance(marker, str) or not _MARKER.fullmatch(marker):
        raise SheetsExtensionError("invalid_payload", "Completion requires the claim marker")
    return {**selection, "mode": mode, "column": column_number(column), "claim_marker": marker}


def transition_google_sheet_row(
    transport: SheetsTransport, payload: Mapping[str, Any], operation_input: Mapping[str, Any]
) -> dict[str, Any]:
    document = operation_input["spreadsheet_id"]
    sheet = selected_sheet(transport.spreadsheet_metadata(document), operation_input)
    grid = {"grid_rows": sheet["row_count"], "grid_columns": sheet["column_count"]}
    bounds = selected_bounds(operation_input["range"], **grid)
    column = operation_input["column"]
    if not bounds.start_column <= column <= bounds.end_column:
        raise SheetsExtensionError("invalid_payload", "The status column must be inside the table")
    height = bounds.end_row - bounds.start_row + 1
    provider_range = next(
        read_ranges(
            sheet["title"], relative_range=operation_input["range"], batch_rows=height, **grid
        )
    )
    offset = column - bounds.start_column
    mode = operation_input["mode"]
    if mode == "complete":
        marker = operation_input["claim_marker"]
        current = _snapshot(transport, document, provider_range, bounds, offset, marker)
        if current is None or not _replace(
            transport,
            document,
            sheet["sheet_id"],
            column,
            bounds.start_row,
            bounds.end_row,
            marker,
            "Done",
        ):
            raise SheetsExtensionError("sheet_row_claim_conflict", _CONFLICT_MESSAGE)
        return {"changed": True, "claim_marker": marker, "row": {}}

    cell_column, cell_row = operation_input["status_cell"]
    if cell_column != column or not bounds.start_row < cell_row <= bounds.end_row:
        raise SheetsExtensionError(
            "invalid_payload", "The status cell must be in the table's data rows"
        )
    operation_id = effect_operation_id(payload)
    marker = f"Processing [FS {hashlib.sha256(operation_id.encode()).hexdigest()[:32]}]"
    current = _snapshot(transport, document, provider_range, bounds, offset, marker)
    if current is None:
        if not _replace(
            transport, document, sheet["sheet_id"], column, cell_row - 1, cell_row, "Ready", marker
        ):
            return {"changed": False, "claim_marker": "", "row": {}}
        # Use the row containing the claim in one snapshot, even if the table sorted
        # between the original queue scan and the conditional write at status_cell.
        current = _snapshot(transport, document, provider_range, bounds, offset, marker)
        if current is None:
            raise SheetsExtensionError("sheet_row_claim_conflict", _CONFLICT_MESSAGE)
    return {"changed": True, "claim_marker": marker, "row": current}


def _snapshot(
    transport: SheetsTransport,
    document: str,
    provider_range: str,
    bounds: A1Bounds,
    offset: int,
    marker: str,
) -> dict[str, str | None] | None:
    rows = values_page(transport.values_get(document, provider_range))
    matches = [row for row in rows[1:] if len(row) > offset and row[offset] == marker]
    if len(matches) > 1:
        raise SheetsExtensionError("sheet_row_claim_conflict", _CONFLICT_MESSAGE)
    if not matches:
        return None
    width = bounds.end_column - bounds.start_column + 1
    header, row = rows[0], matches[0]
    headers = column_headers((header + [None] * width)[:width])
    values = (row + [None] * width)[:width]
    values[offset] = "Ready"
    return dict(zip(headers, values, strict=True))


def _replace(
    transport: SheetsTransport,
    document: str,
    sheet_id: int,
    column: int,
    start_row_index: int,
    end_row_index: int,
    expected: str,
    replacement: str,
) -> bool:
    response = transport.batch_update(
        document,
        [
            {
                "findReplace": {
                    "find": expected,
                    "replacement": replacement,
                    "matchCase": True,
                    "matchEntireCell": True,
                    "includeFormulas": False,
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row_index,
                        "endRowIndex": end_row_index,
                        "startColumnIndex": column - 1,
                        "endColumnIndex": column,
                    },
                }
            }
        ],
    )
    replies = response.get("replies")
    if isinstance(replies, list) and len(replies) == 1 and isinstance(replies[0], dict):
        reply = replies[0].get("findReplace", {})
        changed = reply.get("valuesChanged", 0) if isinstance(reply, dict) else None
        if type(changed) is int and changed in (0, 1):
            return changed == 1
    raise SheetsExtensionError(
        "timeout_unknown", "Google Sheets did not confirm a single status update"
    )
