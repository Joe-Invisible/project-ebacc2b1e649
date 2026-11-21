"""Run the native submission pipeline using the dataset's manifest window."""
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    workers = os.environ.get("NCPU", str(len(os.sched_getaffinity(0))))
    directory = Path(__file__).resolve().parent
    manifest = json.loads((data / "manifest.json").read_text())
    result = subprocess.run([
        directory / "native_pipeline", data, output,
        manifest["window_start"], manifest["window_end"], workers,
    ])
    if result.returncode == 10:
        os.execv(sys.executable, [sys.executable, str(directory / "pipeline.py")])
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
