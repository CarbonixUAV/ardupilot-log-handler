# tests/test_performance.py
import os
import time
import tracemalloc
import threading
import matplotlib.pyplot as plt
from ardupilot_log_handler.ap_log_handler import ArduPilotLogHandler


def profile_function(func, *args, interval_sec=5, **kwargs):
    memory_log = []
    timestamps = []
    keep_running = True

    def memory_sampler():
        while keep_running:
            current, peak = tracemalloc.get_traced_memory()
            memory_log.append(current / 1024 / 1024)  # MB
            timestamps.append(time.time() - start_time)
            time.sleep(interval_sec)

    tracemalloc.start()
    start_time = time.time()

    thread = threading.Thread(target=memory_sampler)
    thread.start()

    func(*args, **kwargs)

    keep_running = False
    thread.join()

    exec_time = time.time() - start_time
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    plot_memory_usage(timestamps, memory_log, exec_time)

    return round(exec_time, 2), round(peak / 1024 / 1024, 2)


def plot_memory_usage(timestamps, memory_log, total_time):
    output_dir = "reports"
    os.makedirs(output_dir, exist_ok=True)  # ✅ Ensure the folder exists

    import matplotlib.pyplot as plt
    plt.figure(figsize=(10, 5))
    plt.plot(timestamps, memory_log, marker='o')
    plt.title("Memory Usage Over Time")
    plt.xlabel("Time (s)")
    plt.ylabel("Memory (MB)")
    plt.grid(True)
    plt.tight_layout()

    output_path = os.path.join(output_dir, "memory_usage_plot.png")
    plt.savefig(output_path)
    print(f"📊 Memory usage plot saved to {output_path}")


def test_extract_bin_parquet_ts_performance():
    handler = ArduPilotLogHandler("tests/test_data/example1.BIN")
    handler.process_log()
    handler.extract_msg_format()

    exec_time, peak_memory = profile_function(handler.extract_bin_parquet_ts, interval_sec=5)

    max_time = 350.0  # seconds
    max_memory = 1000.0  # MB

    print(f"\nExecution Time: {exec_time}s, Peak Memory: {peak_memory}MB")
    assert exec_time < max_time, f"Too slow: {exec_time}s > {max_time}s"
    assert peak_memory < max_memory, f"Too much memory used: {peak_memory}MB > {max_memory}MB"
