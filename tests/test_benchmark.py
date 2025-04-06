#  tests/test_benchmark.py
import pytest
from ardupilot_log_handler.ap_log_handler import ArduPilotLogHandler

@pytest.fixture(scope="module")
def handler():
    log_path = "tests/test_data/example1.BIN"
    h = ArduPilotLogHandler(log_path)
    h.process_log()
    h.extract_msg_format()
    return h


def test_extract_bin_parquet_benchmark(benchmark, handler):
    benchmark(handler.extract_bin_parquet_ts)
