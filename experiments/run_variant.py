"""Run a sessionization benchmark variant through the complete pipeline."""
import argparse
import ctypes
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
from pipeline import main


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("method", choices=("baseline", "sql", "extension"))
    parser.add_argument("--extension", type=Path)
    args = parser.parse_args()
    if (args.method == "extension") != (args.extension is not None):
        parser.error("the extension method requires --extension")
    if args.extension:
        import _duckdb

        # The Python wheel loads DuckDB locally. Expose its C++ symbols so the
        # dynamically loaded window can resolve them from the same engine.
        ctypes.CDLL(_duckdb.__file__, mode=ctypes.RTLD_GLOBAL)

    main(args.method, args.extension)
