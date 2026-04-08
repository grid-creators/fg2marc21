"""
Extract person items from subset_P2_Q7.json that have NO GND ID (property P76).

Streams the large JSON file (~3.4GB) item by item to avoid loading it all into memory.
Outputs a TSV with QID and label (German or English fallback).
"""

import json
import os
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")

FACTGRID_INPUT = os.path.join(DATA_DIR, "subset_P2_Q7.json")
OUTPUT = os.path.join(DATA_DIR, "person_without_gnd.tsv")


def get_label(item):
    """Get label: prefer German, fall back to English, then any available."""
    labels = item.get("labels", {})
    if "de" in labels:
        return labels["de"].get("value", "")
    if "en" in labels:
        return labels["en"].get("value", "")
    if labels:
        return next(iter(labels.values())).get("value", "")
    return ""


def has_gnd(item):
    """Check if item has a P76 (GND ID) claim."""
    claims = item.get("claims", {})
    return "P76" in claims


def stream_items(path):
    """Stream JSON array items one by one from a large file."""
    print(f"Streaming {path} ...")
    with open(path, "r", encoding="utf-8") as f:
        # Skip to opening '['
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
                yield item


if __name__ == "__main__":
    start = time.time()
    total = 0
    without_gnd = 0

    with open(OUTPUT, "w", encoding="utf-8") as out:
        out.write("QID\tLabel\n")

        for item in stream_items(FACTGRID_INPUT):
            total += 1
            if total % 10000 == 0:
                print(f"  ... scanned {total}, without GND: {without_gnd}", flush=True)

            if not has_gnd(item):
                qid = item.get("id", "")
                label = get_label(item)
                out.write(f"{qid}\t{label}\n")
                without_gnd += 1

    elapsed = time.time() - start
    print(f"\nDone. Scanned {total} items total.")
    print(f"  With GND (P76):    {total - without_gnd}")
    print(f"  Without GND (P76): {without_gnd}")
    print(f"  Saved to {OUTPUT} in {elapsed:.1f}s")
