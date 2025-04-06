# tools/generate_output.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ardupilot_log_handler.ap_log_handler import ArduPilotLogHandler


def generate():
    log_path = "tests/test_data/example1.BIN"
    handler = ArduPilotLogHandler(log_path)
    handler.process_log()
    handler.extract_parquet()


if __name__ == "__main__":
    generate()
