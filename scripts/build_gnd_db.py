"""
Build a SQLite database from the GND MARC XML dump for fast local lookups.

Streams the ~26GB XML file line by line (same approach as extract_gnd_by_id.py),
extracts key fields from each record, and inserts them into a SQLite database.

Extracted fields per record:
  - gnd_id:         GND ID from field 024 $a (where $2 = gnd)
  - preferred_name: Preferred name from field 100 $a (+ $d date if present)
  - country_code:   Country code from field 043 $c (truncated to country level)
  - entity_type:    Entity type code from field 075 $b

Usage:
    python build_gnd_db.py

Input:  authorities-gnd-person_dnbmarc_20260217.mrc.xml (~26GB)
Output: gnd_persons.db (~200-400MB SQLite database)
"""

import os
import re
import sqlite3
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.join(SCRIPT_DIR, "..")
DATA_DIR = os.path.join(ROOT_DIR, "data")

GND_INPUT = os.path.join(DATA_DIR, "authorities-gnd-person_dnbmarc_20260217.mrc.xml")
DB_OUTPUT = os.path.join(ROOT_DIR, "gnd_persons.db")

# Regex patterns for field extraction
RE_024_GND = re.compile(
    r'<datafield tag="024"[^>]*>.*?'
    r'<subfield code="a">([^<]+)</subfield>.*?'
    r'<subfield code="2">gnd</subfield>.*?'
    r"</datafield>",
    re.DOTALL,
)

RE_100_A = re.compile(
    r'<datafield tag="100"[^>]*>.*?'
    r'<subfield code="a">([^<]+)</subfield>',
    re.DOTALL,
)

RE_043_C = re.compile(
    r'<datafield tag="043"[^>]*>.*?'
    r'<subfield code="c">([^<]+)</subfield>',
    re.DOTALL,
)

RE_075_B = re.compile(
    r'<datafield tag="075"[^>]*>.*?'
    r'<subfield code="b">([^<]+)</subfield>',
    re.DOTALL,
)


def truncate_country_code(code):
    """Truncate a country code to country level, e.g. 'XA-DE-TH' -> 'XA-DE'."""
    parts = code.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return code


def create_database(db_path):
    """Create the SQLite database schema."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS gnd_records")
    c.execute("""
        CREATE TABLE gnd_records (
            gnd_id TEXT PRIMARY KEY,
            preferred_name TEXT,
            country_code TEXT,
            entity_type TEXT
        )
    """)
    conn.commit()
    return conn


def build_database():
    """Stream the GND XML dump and populate the SQLite database."""
    print(f"Creating database {DB_OUTPUT} ...")
    conn = create_database(DB_OUTPUT)
    cursor = conn.cursor()

    print(f"Streaming {GND_INPUT} ...")
    start = time.time()

    records_scanned = 0
    records_inserted = 0
    batch = []
    batch_size = 10000

    with open(GND_INPUT, "r", encoding="utf-8") as f:
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
                records_scanned += 1

                if records_scanned % 500000 == 0:
                    elapsed = time.time() - start
                    print(
                        f"  ... {records_scanned:,} records scanned, "
                        f"{records_inserted:,} inserted, "
                        f"{elapsed:.0f}s elapsed",
                        flush=True,
                    )

                # Extract GND ID
                m = RE_024_GND.search(buf)
                if not m:
                    continue
                gnd_id = m.group(1)

                # Extract preferred name from field 100 $a
                m100 = RE_100_A.search(buf)
                preferred_name = m100.group(1) if m100 else ""

                # Extract country code from field 043 $c
                m043 = RE_043_C.search(buf)
                country_code = ""
                if m043:
                    country_code = truncate_country_code(m043.group(1))

                # Extract entity type from field 075 $b
                m075 = RE_075_B.search(buf)
                entity_type = m075.group(1) if m075 else ""

                batch.append((gnd_id, preferred_name, country_code, entity_type))
                records_inserted += 1

                if len(batch) >= batch_size:
                    cursor.executemany(
                        "INSERT OR REPLACE INTO gnd_records VALUES (?, ?, ?, ?)",
                        batch,
                    )
                    conn.commit()
                    batch = []

    # Insert remaining batch
    if batch:
        cursor.executemany(
            "INSERT OR REPLACE INTO gnd_records VALUES (?, ?, ?, ?)",
            batch,
        )
        conn.commit()

    # Create index (already primary key, but add text index for name search)
    print("Creating indexes ...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_type ON gnd_records(entity_type)")
    conn.commit()

    elapsed = time.time() - start
    print(f"\nDone. Scanned {records_scanned:,} records, inserted {records_inserted:,} in {elapsed:.0f}s")
    print(f"Database saved to {DB_OUTPUT}")

    # Print stats
    cursor.execute("SELECT COUNT(*) FROM gnd_records")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM gnd_records WHERE preferred_name != ''")
    with_name = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM gnd_records WHERE country_code != ''")
    with_country = cursor.fetchone()[0]
    print(f"\nStatistics:")
    print(f"  Total records:        {total:,}")
    print(f"  With preferred name:  {with_name:,}")
    print(f"  With country code:    {with_country:,}")

    conn.close()


if __name__ == "__main__":
    build_database()
