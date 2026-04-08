"""
Extract GND MARC XML records whose GND ID appears in person_with_gnd.tsv (second column).

Streams the ~26GB XML file line by line, buffering one <record> at a time,
and writes matching records directly to the output file. Memory usage stays
minimal (only one record + the GND ID set in memory at any time).
"""

import os
import re
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")

GND_INPUT = os.path.join(DATA_DIR, "authorities-gnd-person_dnbmarc_20260217.mrc.xml")
QID_FILE = os.path.join(DATA_DIR, "person_with_gnd.tsv")
GND_OUTPUT = os.path.join(DATA_DIR, "gnd_matching_subset.xml")

# Regex to find GND ID in 024 field with $2 = gnd
RE_024_GND = re.compile(
    r'<datafield tag="024"[^>]*>.*?'
    r'<subfield code="a">([^<]+)</subfield>.*?'
    r'<subfield code="2">gnd</subfield>.*?'
    r"</datafield>",
    re.DOTALL,
)


def load_gnd_ids(path):
    """Load GND IDs from the second column of a TSV file (skip header)."""
    gnd_ids = set()
    with open(path, "r", encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                gnd_ids.add(parts[1])
    print(f"Loaded {len(gnd_ids)} GND IDs from {path}")
    return gnd_ids


def extract_gnd_subset(gnd_ids):
    """Stream GND MARC XML and extract records matching the given GND IDs."""
    print(f"Streaming {GND_INPUT} ...")
    start = time.time()

    matched = 0
    records_scanned = 0

    with open(GND_OUTPUT, "w", encoding="utf-8") as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        out.write('<collection xmlns="http://www.loc.gov/MARC21/slim">\n')

        buf = ""
        in_record = False
        with open(GND_INPUT, "r", encoding="utf-8") as f:
            for line in f:
                if "<record" in line:
                    in_record = True
                    buf = ""
                if in_record:
                    buf += line
                if "</record>" in line and in_record:
                    in_record = False
                    records_scanned += 1
                    if records_scanned % 500000 == 0:
                        print(
                            f"  ... scanned {records_scanned} records, matched {matched}",
                            flush=True,
                        )

                    m = RE_024_GND.search(buf)
                    if m and m.group(1) in gnd_ids:
                        out.write(buf)
                        matched += 1
                        if matched == len(gnd_ids):
                            print(f"  Found all {matched} matching records, stopping early.")
                            break

        out.write("</collection>\n")

    elapsed = time.time() - start
    print(f"Done. Scanned {records_scanned} records, matched {matched} of {len(gnd_ids)} GND IDs in {elapsed:.1f}s")
    print(f"Saved to {GND_OUTPUT}")

    if matched < len(gnd_ids):
        # Stream output to find which IDs were matched, without loading all into memory
        found_ids = set()
        with open(GND_OUTPUT, "r", encoding="utf-8") as f:
            buf = ""
            in_record = False
            for line in f:
                if "<record" in line:
                    in_record = True
                    buf = ""
                if in_record:
                    buf += line
                if "</record>" in line and in_record:
                    in_record = False
                    m = RE_024_GND.search(buf)
                    if m:
                        found_ids.add(m.group(1))
        missing = gnd_ids - found_ids
        print(f"{len(missing)} GND IDs not found in GND dump:")
        for gid in sorted(missing)[:20]:
            print(f"  {gid}")
        if len(missing) > 20:
            print(f"  ... and {len(missing) - 20} more")


if __name__ == "__main__":
    gnd_ids = load_gnd_ids(QID_FILE)
    extract_gnd_subset(gnd_ids)
