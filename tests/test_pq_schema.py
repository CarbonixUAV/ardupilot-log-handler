# tests/test_pq_schema.py
import os
import pyarrow.parquet as pq
import pandas as pd


def test_compare_generated_to_expected_parquet():
    # Expected and generated file paths
    expected_file = (
        "tests/test_data/expected_output/"
        "LogUID=230dd76680f14b07ad942b9a5c312ed134863823e73836c7a40b61e464630415/"
        "MessageType=ATT/Instance=0/KeyName=Roll/file.parquet"
    )

    generated_file = (
        "output/LogUID=230dd76680f14b07ad942b9a5c312ed134863823e73836c7a40b61e464630415/"
        "MessageType=ATT/Instance=0/KeyName=Roll/file.parquet"
    )

    # Step 1: Validate files exist
    assert os.path.exists(expected_file), f"Missing expected file: {expected_file}"
    assert os.path.exists(generated_file), f"Missing generated file: {generated_file}"

    # Step 2: Load both files
    expected_table = pq.read_table(expected_file)
    generated_table = pq.read_table(generated_file)

    # Step 3: Compare schema
    expected_schema = set(expected_table.schema.names)
    generated_schema = set(generated_table.schema.names)

    assert expected_schema.issubset(generated_schema), (
        f"Generated file is missing columns."
        f"Expected at least: {expected_schema}, Found: {generated_schema}"
    )

    # Step 4: Compare row counts
    expected_rows = expected_table.num_rows
    generated_rows = generated_table.num_rows

    print(f"Expected rows: {expected_rows}, Generated rows: {generated_rows}")
    assert expected_rows == generated_rows, (
        f"Row count mismatch. Expected: {expected_rows}, Got: {generated_rows}"
    )

    # Step 5: Compare values (optional, strict)
    expected_df = expected_table.to_pandas()
    generated_df = generated_table.to_pandas()

    # Only compare required columns
    columns_to_check = ["Timestamp", "LineNumber", "Value", "StringValue", "BinaryValue"]
    for col in columns_to_check:
        if col in expected_df and col in generated_df:
            pd.testing.assert_series_equal(
                expected_df[col], generated_df[col],
                check_names=False,
                obj=f"Mismatch in column '{col}'"
            )

    print("✅ Parquet file matches expected output.")
