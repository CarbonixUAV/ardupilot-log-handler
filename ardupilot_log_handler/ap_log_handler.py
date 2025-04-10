# ap_log_handler.py
import argparse
import array
import hashlib
import logging
import os
import re
import time
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymavlink import mavutil

# Configure the root logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Silence verbose logging from other libraries
# Silence verbose logging from specific libraries
for lib in ('boto3', 'botocore', 'urllib3', 'pandas',
            'pyarrow', 'pymavlink', 's3transfer'):
    # Change from DEBUG to WARNING
    logging.getLogger(lib).setLevel(logging.WARNING)


class ArduPilotLogHandler:
    def __init__(self, log_file_path: str, output_path: str = "output"):
        logger.debug(f"Initializing log handler for {log_file_path}")
        self.log_file_path = log_file_path
        self.cube_id = None
        self.start_time = None
        self.boot_time = None
        self.boot_time_diff = None
        self.boot_number = 0
        self.log_uid = None
        self.log_type = None
        self.clock_offset = None
        self.log_format = None
        self.output_path = output_path
        self.file_name = os.path.basename(log_file_path)

        logger.info(f"Log handler initialized for {self.log_file_path}")
        self.detect_log_type()
        logger.debug(f"Log type: {self.log_type}")
        self.log_uid = self.calculate_sha256()
        logger.debug(f"Log UID: {self.log_uid}")

    def get_log_uid(self) -> str:
        """Returns the SHA256 hash of the log file."""
        return self.log_uid

    def detect_log_type(self):
        """Detects the log type based on the file extension."""
        _, file_extension = os.path.splitext(self.log_file_path)
        if file_extension.lower() == ".bin":
            self.log_type = "BIN"
        elif file_extension.lower() == ".tlog":
            self.log_type = "TLOG"
        else:
            logger.error(f"Unsupported log file type: {file_extension}")

    def get_clock_offset(self):
        """Calculates the average clock offset between GPS and PC."""
        average_offset = 0
        num_offsets = 0
        mavlog = mavutil.mavlink_connection(self.log_file_path)

        while True:
            msg = mavlog.recv_match(type=['SYSTEM_TIME', 'GPS_RAW_INT'])
            if not msg:
                break

            if (msg.get_srcComponent() != 1 or
                    (msg.get_type() == 'GPS_RAW_INT' and msg.fix_type < 3)):
                continue

            time_unix_usec = (msg.time_unix_usec if msg.get_type()
                              == 'SYSTEM_TIME' else msg.time_usec)

            # Ignore times before 2000
            if time_unix_usec < 946684800000000:
                continue

            offset = msg._timestamp - time_unix_usec / 1_000_000
            num_offsets += 1
            average_offset += (offset - average_offset) / num_offsets

        mavlog.rewind()
        self.clock_offset = average_offset
        logger.debug(f"Calculated clock offset: {self.clock_offset}")

    def process_log(self):
        """Processes the log file based on its type (TLOG or BIN)."""
        if self.log_type == "TLOG":
            logger.debug("Processing TLOG file on the fly.")
            self.process_tlog_on_the_fly()
        elif self.log_type == "BIN":
            logger.debug("Processing BIN file on the fly.")
            self.process_bin_on_the_fly()
        else:
            logger.error("Unsupported log type. Cannot process.")

    def process_tlog_on_the_fly(self):
        """Processes TLOG files and extracts necessary details on the fly."""
        self.get_clock_offset()
        mavlog = mavutil.mavlink_connection(self.log_file_path)
        while True:
            try:
                msg = mavlog.recv_match(type=['STATUSTEXT', 'PARAM_VALUE',
                                              'SYSTEM_TIME'])
                if msg is None or msg.get_type() is None:
                    logger.debug("End of file reached.")
                    break

                if msg.get_type() == 'STATUSTEXT' and not self.cube_id:
                    self.cube_id = self.extract_cube_id_from_msg(msg)
                    if self.cube_id:
                        logger.debug(f"Extracted cube_id: {self.cube_id}")

                if msg.get_type() == "SYSTEM_TIME" and not self.start_time:
                    self.start_time = ((msg.time_unix_usec -
                                        msg.time_boot_ms * 1000) / 1_000_000)
                    self.start_time -= self.clock_offset
                    logger.debug(f"Extracted timestamp: {self.start_time}")

                if (msg.get_type() == 'PARAM_VALUE' and
                    msg.param_id == 'STAT_BOOTCNT' and
                        not self.boot_number):
                    self.boot_number = int(msg.param_value)
                    logger.debug(f"Extracted boot_number: {self.boot_number}")

                if self.start_time and self.cube_id and self.boot_number:
                    logger.debug("All required info extracted. Exiting early.")
                    break

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                break
        mavlog.rewind()

    def process_bin_on_the_fly(self):
        """Processes BIN files and extracts necessary details on the fly."""
        mavlog = mavutil.mavlink_connection(self.log_file_path)
        self.boot_time = mavlog.clock.timebase  # in seconds
        logger.debug(f"Extracted boot time: {self.boot_time}")
        while True:
            msg = mavlog.recv_match(blocking=True)
            if not msg:
                break

            if self.start_time is None:
                self.start_time = self.extract_log_ts_ms(msg) / 1000
                logger.debug(f"Extracted start time: {self.start_time}")

            if msg.get_type() == 'MSG' and not self.cube_id:
                self.cube_id = self.extract_cube_id_from_msg(msg)
                if self.cube_id:
                    logger.debug(f"Extracted cube_id: {self.cube_id}")

            if (msg.get_type() == 'PARM' and msg.Name == 'STAT_BOOTCNT'
                    and not self.boot_number):
                self.boot_number = int(msg.Value)
                logger.debug(f"Extracted boot_number: {self.boot_number}")

            if self.cube_id and self.boot_number and self.start_time:
                logger.debug("All required info extracted. Exiting early.")
                break
        mavlog.rewind()

    def extract_cube_id_from_msg(self, msg):
        """Extracts the Cube ID from the message using predefined patterns."""
        search_patterns = [
            r"CarbonixCubeOrange\s+(\S.*)",
            r"CubeOrange\s+(\S.*)",
            r"CubeOrange-Volanti\s+(\S.*)",
            r"CubeOrange-Ottano\s+(\S.*)",
            r"CubeOrange-Octano\s+(\S.*)",
            r"CubeOrangePlus\s+(\S.*)",
            r"CubeOrangePlus-Volanti\s+(\S.*)",
            r"CubeOrangePlus-Ottano\s+(\S.*)",
            r"CubeOrangePlus-Octano\s+(\S.*)"
        ]
        for pattern in search_patterns:
            match = re.search(
                pattern, msg.text if self.log_type == "TLOG" else msg.Message)
            if match:
                return match.group(1).strip()
        return None

    def extract_log_ts_ms(self, msg) -> Optional[int]:
        """Extract timestamp in milliseconds from a log message."""
        if hasattr(msg, "_timestamp") and msg._timestamp is not None:
            return int(msg._timestamp * 1000)
        return None

    def calculate_sha256(self):
        """Calculates the SHA256 hash for the given file."""
        sha256_hash = hashlib.sha256()
        with open(self.log_file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def extract_parquet(self):
        """Extracts time-series data and saves it in Parquet format."""
        if self.log_type == "TLOG":
            self.extract_tlog_to_parquet()
        else:
            self.extract_msg_format()
            self.extract_bin_parquet_ts()

    def extract_bin_parquet_ts(self, batch_size=10000):
        """Final optimized version with path handling fix."""
        logger.debug(f"Converting binary log to Parquet: {self.log_file_path}")

        # 1. Initialize MAVLink reader with minimal overhead
        mavlog = mavutil.mavlink_connection(self.log_file_path)
        str_units = {"n", "N", "Z"}

        # 2. Schema definition
        schema = pa.schema([
            ("Timestamp", pa.int64()),
            ("LineNumber", pa.int64()),
            ("Value", pa.float32()),
            ("StringValue", pa.string()),
            ("BinaryValue", pa.binary())
        ])

        # 3. Output directory structure
        output_path_prefix = os.path.join(self.output_path, f"LogUID={self.log_uid}")
        os.makedirs(output_path_prefix, exist_ok=True)

        # 4. Optimized data structures
        writers = {}
        dir_cache = set()  # Track created directories
        line_number = 0
        start_time = time.time()

        # 5. Pre-build path templates for known message types
        path_templates = {}
        for msg_type, fmt in self.log_format.items():
            if msg_type == "UNIT":
                continue
            instance_key = fmt.get("InstanceKey", 0)
            base_path = os.path.join(output_path_prefix, f"MessageType={msg_type}")
            path_templates[msg_type] = (instance_key, base_path)

        try:
            while True:
                # 6. Bulk message reading
                msg = mavlog.recv_match(blocking=True)
                if not msg:
                    break

                line_number += 1
                msg_type = msg.get_type()

                if msg_type not in path_templates:
                    continue

                # 7. Get pre-computed path info
                instance_key, base_path = path_templates[msg_type]
                instance = 0

                # 8. Direct field access without getattr overhead
                fieldnames = msg._fieldnames
                values = [getattr(msg, f) for f in fieldnames]

                for field, value in zip(fieldnames, values):
                    if field in ["TimeUS", "MessageType", "mavpackettype"]:
                        continue

                    if field == instance_key:
                        instance = value
                        continue

                    # 9. Fast value conversion
                    format_type = self.log_format[msg_type].get(f"{field}_F", "")

                    if not format_type or format_type in str_units:
                        val, value_str, binary_value = None, str(value), None
                    elif isinstance(value, array.array):
                        val, value_str, binary_value = None, None, value.tobytes()
                        if value.typecode == 'h' and value:
                            val = float(value[0])
                    else:
                        try:
                            val, value_str, binary_value = float(value), None, None
                        except (TypeError, ValueError):
                            val, value_str, binary_value = None, str(value), None

                    # 10. Path construction with dynamic directory creation
                    dir_path = os.path.join(base_path, f"Instance={instance}", f"KeyName={field}")
                    file_path = os.path.join(dir_path, "file.parquet")

                    # 11. Create directory if needed (with cache)
                    if dir_path not in dir_cache:
                        os.makedirs(dir_path, exist_ok=True)
                        dir_cache.add(dir_path)

                    # 12. Batch collection
                    if file_path not in writers:
                        writers[file_path] = {
                            'timestamps': array.array('q'),
                            'linenums': array.array('q'),
                            'values': array.array('f'),
                            'str_values': [],
                            'bin_values': [],
                            'count': 0
                        }

                    data = writers[file_path]
                    data['timestamps'].append(int(msg._timestamp * 1000) if hasattr(msg, "_timestamp") else 0)
                    data['linenums'].append(line_number)
                    if val is not None:
                        data['values'].append(val)
                    else:
                        data['values'].append(float('nan'))
                    data['str_values'].append(value_str)
                    data['bin_values'].append(binary_value)
                    data['count'] += 1

                    # 13. Batch writing
                    if data['count'] >= batch_size:
                        self._write_parquet_batch_final(data, file_path, schema)
                        data['timestamps'] = array.array('q')
                        data['linenums'] = array.array('q')
                        data['values'] = array.array('f')
                        data['str_values'] = []
                        data['bin_values'] = []
                        data['count'] = 0

        except Exception as e:
            logger.error(f"Error processing message: {e}")
        finally:
            # Final writes
            for file_path, data in writers.items():
                if data['count'] > 0:
                    self._write_parquet_batch_final(data, file_path, schema)

        elapsed = time.time() - start_time
        rate = line_number / elapsed if elapsed > 0 else 0
        logger.debug(f"Processed {line_number} messages in {elapsed:.2f}s ({rate:.1f} msg/s)")

    def _write_parquet_batch_final(self, data, file_path, schema):
        """Optimized batch writing with error handling."""
        try:
            # Convert to pyarrow arrays directly
            timestamp_arr = pa.array(data['timestamps'], type=pa.int64())
            linenum_arr = pa.array(data['linenums'], type=pa.int64())
            value_arr = pa.array(data['values'], type=pa.float32())
            str_arr = pa.array(data['str_values'], type=pa.string())
            bin_arr = pa.array(data['bin_values'], type=pa.binary())

            # Create table and write
            table = pa.Table.from_arrays(
                [timestamp_arr, linenum_arr, value_arr, str_arr, bin_arr],
                schema=schema
            )

            # Write with optimal settings
            pq.write_table(
                table,
                file_path,
                compression='snappy',
                use_dictionary=False,
                write_statistics=False,
                data_page_size=2*1024*1024  # 2MB pages
            )
        except Exception as e:
            logger.error(f"Error writing to {file_path}: {e}")
            raise

    def extract_bin_parquet_ts_old(self, batch_size=500000):
        """Extract time-series data BIN log and save it in Parquet format."""
        logger.debug(f"Extracting timeseries data from {self.log_file_path}")
        mavlog = mavutil.mavlink_connection(self.log_file_path)
        str_units = {"n", "N", "Z"}

        schema = pa.schema([
            ("Timestamp", pa.int64()),
            ("LineNumber", pa.int64()),
            ("Value", pa.float32()),
            ("StringValue", pa.string()),
            ("BinaryValue", pa.binary())
        ])

        start_time = time.time()
        data_batches = {}
        batches_count = {}
        line_number = 0
        output_path_prefix = os.path.join(
            self.output_path, "LogUID=" + self.log_uid)
        while True:
            try:
                msg = mavlog.recv_match(blocking=True)
                if not msg:
                    break
                line_number += 1

                msg_type = msg.get_type()
                if msg_type in ["UNIT"] or msg_type not in self.log_format:
                    continue

                timestamp = self.extract_log_ts_ms(msg)
                instance_key = self.log_format[msg_type].get("InstanceKey", 0)
                instance = 0
                msg_path = f"MessageType={msg_type}"
                self.print_progress()

                for key, value in msg.to_dict().items():
                    if key in ["TimeUS", "MessageType", "mavpackettype"]:
                        continue
                    if key == instance_key:
                        instance = value
                        continue
                    format_type = self.log_format[msg_type].get(f"{key}_F")
                    value_str, val, binary_value = None, None, None
                    try:
                        if not format_type or format_type in str_units:
                            value_str = str(value)
                        elif isinstance(value, array.array):
                            if value.typecode == 'h':
                                float_values = [float(v) for v in value]
                                val = float_values[0]
                                binary_value = value.tobytes()
                            else:
                                binary_value = value.tobytes()
                        else:
                            val = float(value)
                    except Exception as e:
                        logger.error(
                            f"Error converting value {value} to float for key "
                            f"{key} in message type {msg_type}: {e}"
                        )
                    output_path = (f"{output_path_prefix}/{msg_path}"
                                   f"/Instance={instance}"
                                   f"/KeyName={key}/file.parquet")

                    if output_path not in data_batches:
                        os.makedirs(os.path.dirname(
                            output_path), exist_ok=True)
                        data_batches[output_path] = {
                            "Timestamp": [],
                            "LineNumber": [],
                            "Value": [],
                            "StringValue": [],
                            "BinaryValue": []
                        }
                        batches_count[output_path] = 0

                    data_batches[output_path]["Timestamp"].append(timestamp)
                    data_batches[output_path]["LineNumber"].append(line_number)
                    data_batches[output_path]["Value"].append(val)
                    data_batches[output_path]["StringValue"].append(value_str)
                    data_batches[output_path]["BinaryValue"].append(
                        binary_value)
                    batches_count[output_path] += 1

                    if batches_count[output_path] >= batch_size:
                        df = pd.DataFrame(data_batches[output_path])
                        table = pa.Table.from_pandas(df, schema=schema)
                        if os.path.exists(output_path):
                            existing_table = pq.read_table(output_path)
                            combined_df = pd.concat(
                                [existing_table.to_pandas(), df],
                                ignore_index=True)
                            table = pa.Table.from_pandas(
                                combined_df, schema=schema)
                        pq.write_table(table, output_path)
                        data_batches[output_path] = {key: [] for key in
                                                     data_batches[output_path]}
                        batches_count[output_path] = 0

            except Exception as e:
                logger.error(f"Error processing message {msg}: {e}")
                break

        for output_path, data in data_batches.items():
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df, schema=schema)
            pq.write_table(table, output_path)
        print('')
        logger.debug("Telemetry data extraction completed.")

        elapsed = time.time() - start_time
        rate = line_number / elapsed if elapsed > 0 else 0
        logger.debug(f"Processed {line_number} messages in {elapsed:.2f}s ({rate:.1f} msg/s)")
        mavlog.rewind()

    def extract_msg_format(self):
        """Extracts the log format from a binary log file."""
        logger.debug(f"Extracting log format from {self.log_file_path}")
        mavlog = mavutil.mavlink_connection(self.log_file_path)
        log_format = {}

        while True:
            try:
                msg = mavlog.recv_match(blocking=True)
                if not msg:
                    break
                if msg.get_type() == "FMT":
                    self.process_fmt(msg, log_format)
                elif msg.get_type() == "FMTU":
                    self.process_fmtu(msg, log_format)
                self.print_progress()
            except Exception as e:
                logger.error(f"Error processing {msg}: {e}")
                break
        print('')
        logger.debug(f"Extracted log format size: {len(log_format)}")
        self.log_format = log_format
        mavlog.rewind()

    def process_fmt(self, msg, log_format):
        """Processes FMT message to extract log format details."""
        if msg.Name not in log_format:
            columns = msg.Columns.split(",")
            formats = list(msg.Format)
            log_format[msg.Name] = {
                "Length": msg.Length,
                "Type": msg.Type,
                "Columns": msg.Columns,
                "Format": msg.Format
            }
            for i, col in enumerate(columns):
                log_format[msg.Name][f"{col}_F"] = formats[i]

    def process_fmtu(self, msg, log_format):
        """Processes FMTU message to extract unit and multiplier details."""
        for msg_type, details in log_format.items():
            if details["Type"] == msg.FmtType:
                details["UnitIds"] = msg.UnitIds
                details["MultIds"] = msg.MultIds
                columns = details["Columns"].split(",")
                for i, col in enumerate(columns):
                    details[f"{col}_U"] = msg.UnitIds[i]
                    details[f"{col}_M"] = msg.MultIds[i]
                    if msg.UnitIds[i] == "#" and "InstanceKey" not in details:
                        details["InstanceKey"] = col

    def extract_tlog_to_parquet(self, batch_size=500000):
        """Extracts telemetry data and saves it in Parquet format."""
        mavlog = mavutil.mavlink_connection(self.log_file_path)
        logger.debug(f"Extracting telemetry data from {self.log_file_path}")
        self.get_clock_offset()

        schema = pa.schema([
            ("Timestamp", pa.int64()),
            ("LineNumber", pa.int64()),
            ("Value", pa.float32()),
            ("StringValue", pa.string()),
            ("BinaryValue", pa.binary())
        ])

        data_batches = {}
        batches_count = {}
        line_number = 0
        output_path_prefix = os.path.join(
            self.output_path, "LogUID=" + self.log_uid)

        while True:
            try:
                msg = mavlog.recv_match()
                if msg is None or msg.get_type() is None:
                    print('')
                    logger.debug("No more messages to process.")
                    break
                line_number += 1

                timestamp = (self.extract_log_ts_ms(msg) -
                             (self.clock_offset*1000))

                msg_path = f"MessageType={msg.get_type()}"
                instance = msg.get_srcComponent()
                self.print_progress()
                for key, value in msg.to_dict().items():
                    value_str, val, binary_value = None, None, None

                    try:
                        if isinstance(value, str):
                            value_str = value
                        elif isinstance(value, (list, array.array, bytearray)):
                            binary_value = bytes(value) if isinstance(
                                value,
                                (bytearray, array.array)) else str(value)
                        else:
                            val = float(value)
                    except Exception as e:
                        logger.error(
                            f"Error converting value {value} to float for key "
                            f"{key} in message type {msg}: {e}")

                    output_path = (f"{output_path_prefix}/{msg_path}/Instance="
                                   f"{instance}/KeyName={key}/file.parquet")
                    if output_path not in data_batches:
                        os.makedirs(os.path.dirname(
                            output_path), exist_ok=True)
                        data_batches[output_path] = {
                            "Timestamp": [],
                            "LineNumber": [],
                            "Value": [],
                            "StringValue": [],
                            "BinaryValue": []
                        }
                        batches_count[output_path] = 0

                    data_batches[output_path]["Timestamp"].append(
                        int(timestamp))
                    data_batches[output_path]["LineNumber"].append(line_number)
                    data_batches[output_path]["Value"].append(val)
                    data_batches[output_path]["StringValue"].append(value_str)
                    data_batches[output_path]["BinaryValue"].append(
                        binary_value)
                    batches_count[output_path] += 1

                    # Once batch size limit is reached, save to Parquet file
                    if batches_count[output_path] >= batch_size:
                        df = pd.DataFrame(data_batches[output_path])
                        table = pa.Table.from_pandas(df, schema=schema)
                        if os.path.exists(output_path):
                            existing_table = pq.read_table(output_path)
                            combined_df = pd.concat(
                                [existing_table.to_pandas(),
                                 df], ignore_index=True)
                            table = pa.Table.from_pandas(
                                combined_df, schema=schema)
                        pq.write_table(table, output_path)
                        data_batches[output_path] = {
                            key: [] for key in data_batches[output_path]}
                        batches_count[output_path] = 0

            except Exception as e:
                logger.error(f"Error processing message {msg}: {e}")
                break

        for output_path, data in data_batches.items():
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df, schema=schema)
            pq.write_table(table, output_path)
        logger.debug("Telemetry data extraction completed.")
        mavlog.rewind()

    def print_progress(self, threshold=100000):
        """Prints progress message after every threshold messages."""
        # create a static variable to keep track of the count
        if not hasattr(self, "_progress_count"):
            self._progress_count = 0
        self._progress_count += 1
        if self._progress_count % threshold == 0:
            print('.', end='', flush=True)
            self._progress_count = 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", required=True,
                        help="Path to the log file")
    parsed_args = parser.parse_args()
    file_path = parsed_args.file_path
    ap_handler = ArduPilotLogHandler(file_path)
    log_uid = ap_handler.get_log_uid()

    ap_handler.process_log()
    cube_id = ap_handler.cube_id
    start_time = ap_handler.start_time
    boot_number = ap_handler.boot_number

    logger.debug(f"Log UID: {log_uid}, Cube ID: {cube_id}, "
                 f"Timestamp: {start_time}, "
                 f"Boot Number: {boot_number}")

    ap_handler.extract_parquet()
