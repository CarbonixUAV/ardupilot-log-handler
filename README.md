# ArduPilot Log Handler

**ArduPilot Log Handler** is a Python library designed to process `.bin` and `.tlog` files from ArduPilot systems. It provides tools to extract telemetry data, generate unique log identifiers, and save data in Parquet format for further analysis.

## Features

- **Support for `.bin` and `.tlog` files**: Automatically detects the log type and processes it accordingly.
- **Extract telemetry data**: Converts time-series data from logs into Parquet files for efficient storage and retrieval.
- **Log metadata extraction**: Identifies Cube ID, boot number, timestamps, and calculates unique log identifiers using SHA256.
- **Integrated logging**: Provides detailed logs for debugging and analysis.

## Installation

You can install the library directly from GitHub:

```bash
pip install git+https://github.com/CarbonixUAV/ardupilot-log-handler.git
```

## Usage

### Command Line

To process a log file and extract telemetry data, use the following command:

```bash
aploghandler --file_path path/to/your/log.bin
```

### Programmatic Usage

The library can also be used in Python scripts:

```python
from ardupilot_log_handler.ap_log_handler import ArduPilotLogHandler

# Initialize the handler
handler = ArduPilotLogHandler("path/to/log.bin")

# Process the log and extract data
handler.process_log()
handler.extract_parquet()
```

## Project Structure

```
ardupilot-log-handler/
├── ardupilot_log_handler/
│   ├── __init__.py
│   ├── ap_log_handler.py
├── tests/
|   |── test_data/
│   |   ├── example1.tlog
│   |   ├── example2.bin
│   ├── __init__.py
│   ├── test_ap_log_handler.py
├── README.md
├── LICENSE
├── setup.py
├── requirements.txt
```

## Development

Clone the repository and install the dependencies to start contributing:

```bash
git clone https://github.com/CarbonixUAV/ardupilot-log-handler.git
cd ardupilot-log-handler
pip install -r requirements.txt
```

Run tests using `pytest`:

```bash
pytest tests/
```
