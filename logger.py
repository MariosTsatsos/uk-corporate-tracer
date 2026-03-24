"""
Run logger — captures all terminal output to a timestamped log file.

Usage in run.py:
    from logger import RunLogger
    rl = RunLogger()
    rl.step("Stage 1")
    ... call step function ...
    rl.done("Stage 1", companies=31)

Log files: logs/run_YYYYMMDD_HHMMSS.log

Each run gets its own file. To see the status of the latest run:
    python run.py --status
"""

import os
import sys
import glob
from datetime import datetime


# ---------------------------------------------------------------------------
# Tee: write to both terminal and log file simultaneously
# ---------------------------------------------------------------------------

class _Tee:
    """Redirect sys.stdout so every print() goes to screen AND log file."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)
            s.flush()

    def flush(self):
        for s in self.streams:
            s.flush()

    def fileno(self):
        # Some libraries need this — delegate to the real stdout
        return sys.__stdout__.fileno()


# ---------------------------------------------------------------------------
# RunLogger
# ---------------------------------------------------------------------------

class RunLogger:
    """
    Manages a single pipeline run log.

    - Creates logs/run_YYYYMMDD_HHMMSS.log
    - Tees sys.stdout so all print() output goes to both terminal and log
    - Tracks step start/finish times and DB counts
    - Writes a structured STEP markers that show_status() can parse
    """

    LOG_DIR = "logs"

    def __init__(self):
        os.makedirs(self.LOG_DIR, exist_ok=True)
        self._ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path  = os.path.join(self.LOG_DIR, f"run_{self._ts}.log")
        self._file = open(self.path, "w", buffering=1)   # line-buffered
        self._run_start    = datetime.now()
        self._step_start   = None
        self._current_step = None

        # Tee stdout — all print() calls now go to terminal + log file
        sys.stdout = _Tee(sys.__stdout__, self._file)

        self._banner(f"RUN STARTED  {self._ts}")

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def step(self, name: str):
        """Call before each pipeline step."""
        if self._current_step:
            # Previous step never got a done() call — mark it
            self._write_marker(f"STEP INTERRUPTED: {self._current_step}")
        self._current_step = name
        self._step_start   = datetime.now()
        self._write_marker(f"STEP START: {name}")

    def done(self, name: str = None, **counts):
        """Call after each pipeline step completes successfully."""
        name     = name or self._current_step or "?"
        elapsed  = self._elapsed(self._step_start)
        parts    = "  ".join(f"{k}={v}" for k, v in counts.items())
        self._write_marker(f"STEP DONE: {name}  [{elapsed}]  {parts}".rstrip())
        self._current_step = None
        self._step_start   = None

    def db_snapshot(self, label="DB snapshot"):
        """Log current DB counts — call before and after full run."""
        from database import get_conn
        try:
            conn = get_conn()
            companies  = conn.execute("SELECT COUNT(DISTINCT company_number) FROM director_companies").fetchone()[0]
            properties = conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
            charges    = conn.execute("SELECT COUNT(*) FROM charges").fetchone()[0]
            conn.close()
            self._write_marker(
                f"DB: {label}  companies={companies}  properties={properties}  charges={charges}"
            )
        except Exception as e:
            self._write_marker(f"DB snapshot failed: {e}")

    def error(self, msg: str):
        self._write_marker(f"ERROR: {msg}")

    def close(self):
        """Call at the end of the run."""
        elapsed = self._elapsed(self._run_start)
        self.db_snapshot("final")
        self._banner(f"RUN COMPLETE  [{elapsed}]")
        sys.stdout = sys.__stdout__   # restore
        self._file.close()
        print(f"\n[LOG] Run saved to: {self.path}")

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _write_marker(self, text: str):
        ts   = datetime.now().strftime("%H:%M:%S")
        line = f"\n### [{ts}] {text}\n"
        # Write to both (stdout is already teed, but markers go direct to file
        # so they stand out even inside a wall of other output)
        sys.__stdout__.write(line)
        self._file.write(line)
        self._file.flush()

    def _banner(self, text: str):
        line = f"\n{'='*60}\n{text}\n{'='*60}\n"
        sys.__stdout__.write(line)
        self._file.write(line)
        self._file.flush()

    @staticmethod
    def _elapsed(since) -> str:
        if not since:
            return "?"
        secs = int((datetime.now() - since).total_seconds())
        m, s = divmod(secs, 60)
        return f"{m}m{s:02d}s" if m else f"{s}s"


# ---------------------------------------------------------------------------
# show_status() — parse the latest log file and print a clean summary
# ---------------------------------------------------------------------------

def show_status():
    log_dir = RunLogger.LOG_DIR
    logs    = sorted(glob.glob(os.path.join(log_dir, "run_*.log")))

    if not logs:
        print("No run logs found. Logs are created when you run the pipeline.")
        print(f"Expected location: {log_dir}/")
        return

    latest = logs[-1]
    print(f"\nLatest log: {latest}")
    if len(logs) > 1:
        print(f"(+{len(logs)-1} older logs in {log_dir}/)")

    # Parse markers
    steps_started  = []
    steps_done     = {}
    db_snapshots   = []
    errors         = []
    run_start      = None
    run_complete   = False

    with open(latest, "r") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("### ["):
                if "RUN STARTED" in line:
                    run_start = line.strip("= \n")
                if "RUN COMPLETE" in line:
                    run_complete = True
                continue

            # Parse: ### [HH:MM:SS] MARKER TEXT
            body = line[line.index("]")+1:].strip()

            if body.startswith("STEP START:"):
                name = body[len("STEP START:"):].strip()
                steps_started.append(name)

            elif body.startswith("STEP DONE:"):
                rest = body[len("STEP DONE:"):].strip()
                parts = rest.split("  ")
                name  = parts[0].strip()
                steps_done[name] = rest

            elif body.startswith("STEP INTERRUPTED:"):
                name = body[len("STEP INTERRUPTED:"):].strip()
                steps_done[name] = f"{name}  [INTERRUPTED]"

            elif body.startswith("DB:"):
                db_snapshots.append(body)

            elif body.startswith("ERROR:"):
                errors.append(body)

    # Print summary
    print()
    if run_start:
        print(f"  Started : {run_start}")
    print(f"  Status  : {'COMPLETE' if run_complete else 'IN PROGRESS / CRASHED'}")
    print()

    print("  Steps:")
    all_steps = list(dict.fromkeys(steps_started))   # preserve order, dedupe
    for s in all_steps:
        if s in steps_done:
            info = steps_done[s].replace(s, "").strip(" []")
            print(f"    [DONE] {s:<35} {info}")
        else:
            print(f"    [ ?? ] {s:<35} (started but no completion recorded)")

    if db_snapshots:
        print()
        print("  DB Counts:")
        for snap in db_snapshots:
            print(f"    {snap}")

    if errors:
        print()
        print("  Errors:")
        for e in errors:
            print(f"    {e}")

    print()
    print(f"  Full log: {latest}")
    print()
