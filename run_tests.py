"""Tiny test runner — pytest is not installed in this env.

Discovers test_*.py modules under tests/, imports each in its own
process (so module-level CLUBRUNNER_DB env stays isolated), runs every
top-level callable named test_* under a try/except, prints a summary.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TESTS = ROOT / "tests"

RUNNER = r"""
import importlib.util, sys, traceback, types
from pathlib import Path

mod_path = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location(mod_path.stem, mod_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[mod_path.stem] = mod
try:
    spec.loader.exec_module(mod)
except Exception:
    traceback.print_exc()
    print("IMPORT_FAIL")
    sys.exit(1)

fails = 0
passes = 0
names = [n for n in dir(mod) if n.startswith("test_") and callable(getattr(mod, n))]
for name in names:
    try:
        getattr(mod, name)()
        print(f"PASS {name}")
        passes += 1
    except Exception:
        print(f"FAIL {name}")
        traceback.print_exc()
        fails += 1
print(f"SUMMARY {passes} passed, {fails} failed")
sys.exit(0 if fails == 0 else 1)
"""


def main():
    runner_path = ROOT / ".test_runner.py"
    files = sorted(TESTS.glob("test_*.py"))
    only = sys.argv[1] if len(sys.argv) > 1 else None
    if only:
        files = [f for f in files if only in f.stem]
    total_p = total_f = 0
    failed_files = []
    t0 = time.time()
    for f in files:
        # Re-write the runner before each subprocess. Something on the
        # host (suspected: an editor or watcher process) intermittently
        # drops files matching ".test_*" between iterations, which made
        # the suite flaky-fail random tests with "can't open file
        # .test_runner.py". Writing the script every iteration costs
        # microseconds and removes the dependency.
        runner_path.write_text(RUNNER)
        env = dict(os.environ)
        env.pop("CLUBRUNNER_DB", None)
        proc = subprocess.run(
            [sys.executable, str(runner_path), str(f)],
            cwd=str(ROOT), env=env, capture_output=True, text=True,
            timeout=60)
        out = proc.stdout
        err = proc.stderr
        # Parse SUMMARY line
        for line in out.splitlines():
            if line.startswith("SUMMARY"):
                parts = line.split()
                total_p += int(parts[1])
                total_f += int(parts[3])
        if proc.returncode != 0:
            failed_files.append(f.name)
            print(f"--- {f.name} ---")
            print(out)
            if err.strip():
                print("STDERR:", err)
        else:
            print(f"OK   {f.name}")
    dt = time.time() - t0
    print(f"\nTotals: {total_p} passed, {total_f} failed across "
          f"{len(files)} files in {dt:.1f}s")
    if failed_files:
        print("Failed files:", failed_files)
    runner_path.unlink(missing_ok=True)
    sys.exit(0 if total_f == 0 and not failed_files else 1)


if __name__ == "__main__":
    main()
