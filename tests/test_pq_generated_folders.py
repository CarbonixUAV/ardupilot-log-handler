# tests/test_pq_generated_folders.py
import os


def collect_relative_file_paths(base_dir):
    """
    Walks the directory and returns a set of all relative file paths.
    """
    relative_paths = set()
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, base_dir)
            relative_paths.add(relative_path.replace("\\", "/"))  # normalize
    return relative_paths


def test_folder_structure_and_file_match():
    expected_dir = "tests/test_data/expected_output"
    generated_dir = "output"

    expected_paths = collect_relative_file_paths(expected_dir)
    generated_paths = collect_relative_file_paths(generated_dir)

    # ✅ Compare missing and extra files
    missing_in_generated = expected_paths - generated_paths
    extra_in_generated = generated_paths - expected_paths

    if missing_in_generated:
        print("❌ Missing files in output:")
        for path in sorted(missing_in_generated):
            print(f"  - {path}")
    if extra_in_generated:
        print("⚠️ Extra files in output:")
        for path in sorted(extra_in_generated):
            print(f"  + {path}")

    assert not missing_in_generated, "Some expected files are missing in output."
    assert not extra_in_generated, "Extra files exist in output that are not in expected."
