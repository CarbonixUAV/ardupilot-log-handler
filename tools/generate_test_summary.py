# tools/generate_test_summary.py
import json
import os
from datetime import datetime

report_dir = "reports"
report_files = [
    "extraction.json",
    "performance.json",
    "schema.json",
    "folder.json"
]

summary_md = os.path.join(report_dir, "test_report.md")
summary_json = os.path.join(report_dir, "test_report.json")

results = []

for file in report_files:
    path = os.path.join(report_dir, file)
    if not os.path.exists(path):
        continue
    with open(path) as f:
        data = json.load(f)
        entry = {
            "name": data.get("summary", {}).get("name", os.path.splitext(file)[0]),
            "outcome": data.get("summary", {}).get("outcome", "unknown"),
            "duration": data.get("duration", 0),
            "tests": data.get("summary", {}).get("tests", 0),
            "failures": data.get("summary", {}).get("failed", 0),
        }
        results.append(entry)

# Save JSON summary
with open(summary_json, "w") as f:
    json.dump(results, f, indent=2)

# Save Markdown summary
with open(summary_md, "w") as f:
    f.write(f"# ✅ Test Report — {datetime.utcnow().isoformat()} UTC\n\n")
    f.write("| Test | Outcome | Duration (s) | Total | Failures |\n")
    f.write("|------|---------|---------------|--------|-----------|\n")
    for r in results:
        emoji = "✅" if r["outcome"] == "passed" else "❌"
        f.write(f"| {r['name']} | {emoji} {r['outcome']} | {r['duration']:.2f} | {r['tests']} | {r['failures']} |\n")

    # Append memory usage plot link to markdown
    plot_path = "memory_usage_plot.png"
    if os.path.exists(os.path.join(report_dir, plot_path)):
        f.write(f"\n## 📊 Memory Usage Over Time\n")
        f.write(f"![Memory Usage](./{plot_path})\n")

    f.write("\n---\n")
    f.write("✅ Tests are run using `pytest`, performance tracked via `pytest-benchmark`, and coverage via `pytest-cov`.\n")
    f.write("📊 Memory profiling every 5s using `tracemalloc`.\n")
