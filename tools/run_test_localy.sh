# tools/run_test_localy.sh
# Clean start (optional)
rm -rf output/

# Run non-dependent tests
pytest tests/test_extraction.py -s
pytest tests/test_performance.py -s

# Generate output
python tools/generate_output.py

# Run tests that depend on output
pytest tests/test_pq_schema.py -s
pytest tests/test_pq_generated_folders.py -s
