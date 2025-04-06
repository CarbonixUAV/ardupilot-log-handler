# tests/test_extraction.py
import json
import os
from ardupilot_log_handler.ap_log_handler import ArduPilotLogHandler


def test_metadata_extraction():
    log_path = "tests/test_data/example1.BIN"
    expected_path = "tests/test_data/expected_values.json"

    with open(expected_path, "r") as f:
        expected = json.load(f)

    handler = ArduPilotLogHandler(log_path)
    handler.process_log()
    handler.extract_msg_format()

    assert handler.get_log_uid() == expected["log_uid"]
    assert handler.log_type == expected["log_type"]
    assert abs(handler.boot_time - expected["boot_time"]) < 1e-5
    assert abs(handler.start_time - expected["start_time"]) < 1e-3
    assert handler.boot_number == expected["boot_number"]
    assert handler.cube_id == expected["cube_id"]
    assert len(handler.log_format) == expected["log_format_size"]
