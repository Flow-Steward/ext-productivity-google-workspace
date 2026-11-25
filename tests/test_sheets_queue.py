from __future__ import annotations

import pytest
from conftest import HOST_CONTRACT_VERSION


class QueueSheet:
    def __init__(self) -> None:
        self.rows = [
            ["Request", "Status", "Notes"],
            ["Printer supplies", "Ready", "Keep this note"],
            ["Delivery quote", "Pending", "Manager reviewing"],
        ]
        self.sort_on_next_replace = False

    def spreadsheet_metadata(self, spreadsheet_id):
        assert spreadsheet_id == "sheet-doc"
        return {
            "sheets": [
                {
                    "properties": {
                        "sheetId": 0,
                        "title": "Queue",
                        "gridProperties": {"rowCount": 10, "columnCount": 3},
                    }
                }
            ]
        }

    def values_get(self, spreadsheet_id, provider_range):
        assert spreadsheet_id == "sheet-doc"
        assert provider_range == "'Queue'!A1:C10"
        return {"values": [row[:] for row in self.rows]}

    def batch_update(self, spreadsheet_id, requests):
        assert spreadsheet_id == "sheet-doc"
        assert len(requests) == 1
        replace = requests[0]["findReplace"]
        assert replace["matchEntireCell"] is True
        assert replace["matchCase"] is True
        assert replace["includeFormulas"] is False
        area = replace["range"]
        assert area["sheetId"] == 0
        assert (area["startColumnIndex"], area["endColumnIndex"]) == (1, 2)
        if self.sort_on_next_replace:
            self.rows[1:] = sorted(self.rows[1:])
            self.sort_on_next_replace = False
        changed = 0
        for row in self.rows[area["startRowIndex"] : area["endRowIndex"]]:
            if row[1] == replace["find"]:
                row[1] = replace["replacement"]
                changed += 1
        return {"replies": [{"findReplace": {"valuesChanged": changed}}]}


def _invoke(monkeypatch, sheet, mode, *, test_mode=False, **fields):
    from sheets import operations

    monkeypatch.setattr(operations, "SheetsTransport", lambda _token: sheet)
    return operations.handle_runtime(
        {
            "contract_version": HOST_CONTRACT_VERSION,
            "mode": "action",
            "runtime_context": {"external_effect_id": "queue-effect-1", "test_mode": test_mode},
            "action": {
                "action_id": "transition_google_sheet_row",
                "page_id": "workflow-builder",
                "component_id": "connector-step",
                "context": {},
                "target": {
                    "connection": {
                        "connection_id": "google-sheets-a",
                        "connection_type_id": "google_sheets_account",
                        "secrets": {"access_token": "test-only-token"},
                    }
                },
                "input": {
                    "connection_ref": "google-sheets-a",
                    "spreadsheet_id": "sheet-doc",
                    "sheet_id": 0,
                    "table_range": "A1:C10",
                    "status_column": "B",
                    "mode": mode,
                    **fields,
                },
            },
        }
    )


def test_completion_follows_the_claim_when_sort_happens_at_write_time(monkeypatch):
    sheet = QueueSheet()
    claimed = _invoke(monkeypatch, sheet, "claim", status_cell="B2")
    assert claimed["ok"] is True, claimed
    assert claimed["result"]["row"] == {
        "Request": "Printer supplies",
        "Status": "Ready",
        "Notes": "Keep this note",
    }
    sheet.sort_on_next_replace = True
    result = _invoke(monkeypatch, sheet, "complete", claim_marker=claimed["result"]["claim_marker"])

    assert result["ok"] is True, result
    assert sheet.rows == [
        ["Request", "Status", "Notes"],
        ["Delivery quote", "Pending", "Manager reviewing"],
        ["Printer supplies", "Done", "Keep this note"],
    ]


def test_claim_snapshot_describes_the_record_actually_claimed(monkeypatch):
    sheet = QueueSheet()
    sheet.rows[2][1] = "Ready"
    sheet.sort_on_next_replace = True

    claimed = _invoke(monkeypatch, sheet, "claim", status_cell="B2")

    assert claimed["ok"] is True, claimed
    assert claimed["result"]["row"]["Request"] == "Delivery quote"
    assert sheet.rows[2][1] == "Ready"


def test_retry_finds_its_claim_after_sort_without_claiming_another_ready_row(monkeypatch):
    sheet = QueueSheet()
    sheet.rows[2][1] = "Ready"
    initial = _invoke(monkeypatch, sheet, "claim", status_cell="B2")
    assert initial["ok"] is True, initial
    sheet.rows[1:] = sorted(sheet.rows[1:])

    retried = _invoke(monkeypatch, sheet, "claim", status_cell="B2")

    assert retried["result"] == initial["result"]
    assert sheet.rows[1][1] == "Ready"


@pytest.mark.parametrize("overwrite_marker", [True, False])
def test_completion_refuses_missing_or_duplicated_claim(monkeypatch, overwrite_marker):
    sheet = QueueSheet()
    initial = _invoke(monkeypatch, sheet, "claim", status_cell="B2")
    assert initial["ok"] is True, initial
    marker = initial["result"]["claim_marker"]
    if overwrite_marker:
        sheet.rows[1][1] = "On hold"
    else:
        sheet.rows[2][1] = marker
    before = [row[:] for row in sheet.rows]

    completed = _invoke(monkeypatch, sheet, "complete", claim_marker=marker)

    assert completed["error_code"] == "sheet_row_claim_conflict"
    assert sheet.rows == before


def test_claim_skips_nonready_row_and_never_overwrites_it(monkeypatch):
    sheet = QueueSheet()
    result = _invoke(monkeypatch, sheet, "claim", status_cell="B3")
    assert result["ok"] is True, result
    assert result["result"]["changed"] is False
    assert sheet.rows[2][1] == "Pending"


def test_queue_test_mode_never_claims_a_record(monkeypatch):
    sheet = QueueSheet()
    result = _invoke(monkeypatch, sheet, "claim", test_mode=True, status_cell="B2")
    assert result["ok"] is True, result
    assert result["external_effect_status"] == "suppressed"
    assert sheet.rows[1][1] == "Ready"
