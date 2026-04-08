"""
Extract a subset of subset_P2_Q7.json for items whose QID appears in person_with_gnd.tsv.

Streams the large JSON file (~3.4GB) item by item to avoid loading it all into memory.
Reads QIDs from the first column of person_with_gnd.tsv (tab-separated, with header).
"""

import json
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")

FACTGRID_INPUT = os.path.join(DATA_DIR, "subset_P2_Q7.json")
QID_FILE = os.path.join(DATA_DIR, "person_with_gnd.tsv")
OUTPUT = os.path.join(DATA_DIR, "factgrid_with_gnd.json")


def load_qids(path):
    """Load QIDs from the first column of a TSV file (skip header)."""
    qids = set()
    with open(path, "r", encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if parts:
                qids.add(parts[0])
    print(f"Loaded {len(qids)} QIDs from {path}")
    return qids


def extract_subset(qids):
    """Stream FactGrid JSON and collect items whose id is in qids."""
    print(f"Streaming {FACTGRID_INPUT} ...")
    start = time.time()

    matched = []
    total = 0

    with open(FACTGRID_INPUT, "r", encoding="utf-8") as f:
        # Skip opening '['
        ch = ""
        while ch != "[":
            ch = f.read(1)

        buf = ""
        brace_depth = 0
        in_string = False
        escape = False
        while True:
            ch = f.read(1)
            if not ch:
                break
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = not in_string
            elif not in_string:
                if ch == "{":
                    brace_depth += 1
                elif ch == "}":
                    brace_depth -= 1
            if brace_depth > 0 or (ch == "}" and not in_string):
                buf += ch
            if brace_depth == 0 and buf:
                try:
                    item = json.loads(buf)
                except json.JSONDecodeError:
                    buf = ""
                    continue
                buf = ""
                total += 1
                if total % 10000 == 0:
                    print(f"  ... scanned {total} items, matched {len(matched)}", flush=True)

                if item.get("id") in qids:
                    matched.append(item)
                    # Early exit if we found all
                    if len(matched) == len(qids):
                        print(f"  Found all {len(matched)} items, stopping early.")
                        break

    elapsed = time.time() - start
    print(f"Scanned {total} items, matched {len(matched)} of {len(qids)} QIDs in {elapsed:.1f}s")
    return matched


if __name__ == "__main__":
    qids = load_qids(QID_FILE)
    items = extract_subset(qids)

    print(f"Writing {len(items)} items to {OUTPUT} ...")
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"Done. Saved to {OUTPUT}")
