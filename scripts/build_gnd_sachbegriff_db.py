"""
Build a SQLite database from the GND Sachbegriff MARC XML dump for fast local lookups.

Streams the XML file line by line (same approach as build_gnd_db.py),
extracts key fields from each record, and inserts them into a SQLite database.

Extracted fields per record:
  - gnd_id:         GND ID from field 024 $a (where $2 = gnd)
  - preferred_name: Preferred name from field 150 $a
  - entity_type:    Entity type code from field 075 $b

Usage:
    python build_gnd_sachbegriff_db.py

Input:  authorities-gnd-sachbegriff_dnbmarc_20260217.mrc.xml
Output: gnd_sachbegriffe.db
"""

import os
import re
import sqlite3
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.join(SCRIPT_DIR, "..")
DATA_DIR = os.path.join(ROOT_DIR, "data")

GND_INPUT = os.path.join(DATA_DIR, "authorities-gnd-sachbegriff_dnbmarc_20260217.mrc.xml")
DB_OUTPUT = os.path.join(ROOT_DIR, "gnd_sachbegriffe.db")

# Regex patterns for field extraction
RE_024_GND = re.compile(
    r'<datafield tag="024"[^>]*>.*?'
    r'<subfield code="a">([^<]+)</subfield>.*?'
    r'<subfield code="2">gnd</subfield>.*?'
    r"</datafield>",
    re.DOTALL,
)

RE_150_A = re.compile(
    r'<datafield tag="150"[^>]*>.*?'
    r'<subfield code="a">([^<]+)</subfield>',
    re.DOTALL,
)

RE_075_B = re.compile(
    r'<datafield tag="075"[^>]*>.*?'
    r'<subfield code="b">([^<]+)</subfield>',
    re.DOTALL,
)


def create_database(db_path):
    """Create the SQLite database schema."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS gnd_records")
    c.execute("""
        CREATE TABLE gnd_records (
            gnd_id TEXT PRIMARY KEY,
            preferred_name TEXT,
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

                # Extract preferred name from field 150 $a
                m150 = RE_150_A.search(buf)
                preferred_name = m150.group(1) if m150 else ""

                # Extract entity type from field 075 $b
                m075 = RE_075_B.search(buf)
                entity_type = m075.group(1) if m075 else ""

                batch.append((gnd_id, preferred_name, entity_type))
                records_inserted += 1

                if len(batch) >= batch_size:
                    cursor.executemany(
                        "INSERT OR REPLACE INTO gnd_records VALUES (?, ?, ?)",
                        batch,
                    )
                    conn.commit()
                    batch = []

    # Insert remaining batch
    if batch:
        cursor.executemany(
            "INSERT OR REPLACE INTO gnd_records VALUES (?, ?, ?)",
            batch,
        )
        conn.commit()

    # Create indexes
    print("Creating indexes ...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_type ON gnd_records(entity_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_preferred_name ON gnd_records(preferred_name)")
    conn.commit()

    elapsed = time.time() - start
    print(f"\nDone. Scanned {records_scanned:,} records, inserted {records_inserted:,} in {elapsed:.0f}s")
    print(f"Database saved to {DB_OUTPUT}")

    # Print stats
    cursor.execute("SELECT COUNT(*) FROM gnd_records")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM gnd_records WHERE preferred_name != ''")
    with_name = cursor.fetchone()[0]
    print(f"\nStatistics:")
    print(f"  Total records:        {total:,}")
    print(f"  With preferred name:  {with_name:,}")

    conn.close()


if __name__ == "__main__":
    build_database()
