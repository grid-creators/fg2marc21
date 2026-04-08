"""
Compare FactGrid→MARC21 conversion output against real GND records.

Reads items from factgrid_with_gnd.json, converts them using the backend
converter, then finds and compares against matching GND records from
gnd_matching_subset.xml.

Usage:
    python compare_conversion.py [N]
    where N = number of items to compare (default: 5)
"""

import sys
import os
import json
import re
from lxml import etree

# Fix encoding for Windows console
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Add backend to path so we can import the converter
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from converter import convert_entity_to_marc, records_to_marc_xml, validate_record
from utils import (
    resolve_labels,
    collect_referenced_entity_ids,
)

MARC_NS = "http://www.loc.gov/MARC21/slim"
NS = {"m": MARC_NS}

# Files
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
FACTGRID_JSON = os.path.join(DATA_DIR, "factgrid_with_gnd.json")
GND_XML = os.path.join(DATA_DIR, "gnd_matching_subset.xml")
GND_TSV = os.path.join(DATA_DIR, "person_with_gnd.tsv")


def load_gnd_id_map():
    """Load QID → GND ID mapping from TSV."""
    mapping = {}
    with open(GND_TSV, "r", encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                mapping[parts[0]] = parts[1]
    return mapping


def load_factgrid_items(n=5):
    """Load first N items from factgrid_with_gnd.json that have P76 (GND ID),
    P77 or P38 (dates), and P247 (family name) for a meaningful comparison."""
    items = []
    with open(FACTGRID_JSON, "r", encoding="utf-8") as f:
        f.readline()  # skip opening '['
        depth = 0
        buf = ""
        for line in f:
            s = line.strip()
            if not buf and not s.startswith("{"):
                continue
            buf += line
            depth += s.count("{") - s.count("}")
            if depth == 0 and buf.strip():
                text = buf.strip().rstrip(",")
                buf = ""
                depth = 0
                try:
                    item = json.loads(text)
                except json.JSONDecodeError:
                    continue
                claims = item.get("claims", {})
                if "P76" in claims and "P77" in claims and "P247" in claims:
                    items.append(item)
                    if len(items) >= n * 3:
                        break
    # Pick every 3rd for diversity
    selected = items[::3][:n]
    if len(selected) < n:
        selected = items[:n]
    return selected


def find_gnd_record(gnd_id):
    """Stream-search gnd_matching_subset.xml for a record with matching GND ID.
    Returns the parsed <record> element or None."""
    target_tag = f"<controlfield tag=\"001\">{gnd_id}</controlfield>"
    buf = []
    inside_record = False
    found_target = False

    with open(GND_XML, "r", encoding="utf-8") as f:
        for line in f:
            if "<record" in line and not inside_record:
                inside_record = True
                found_target = False
                buf = [line]
                continue
            if inside_record:
                buf.append(line)
                if target_tag in line:
                    found_target = True
                if "</record>" in line:
                    inside_record = False
                    if found_target:
                        record_xml = "".join(buf)
                        # Add namespace for parsing
                        record_xml = record_xml.replace(
                            "<record",
                            '<record xmlns="http://www.loc.gov/MARC21/slim"',
                            1,
                        )
                        try:
                            return etree.fromstring(record_xml.encode("utf-8"))
                        except Exception as e:
                            print(f"  XML parse error: {e}")
                            return None
    return None


def extract_fields(record_elem):
    """Extract a normalized dict of fields from a MARC XML <record> element."""
    fields = {}

    # Leader
    leader = record_elem.find("m:leader", NS)
    if leader is not None and leader.text:
        fields["leader"] = leader.text.strip()

    # Control fields
    for cf in record_elem.findall("m:controlfield", NS):
        tag = cf.get("tag", "")
        fields.setdefault(tag, []).append({"value": (cf.text or "").strip()})

    # Data fields
    for df in record_elem.findall("m:datafield", NS):
        tag = df.get("tag", "")
        ind1 = df.get("ind1", " ")
        ind2 = df.get("ind2", " ")
        subfields = []
        for sf in df.findall("m:subfield", NS):
            subfields.append(
                {"code": sf.get("code", ""), "value": (sf.text or "").strip()}
            )
        fields.setdefault(tag, []).append(
            {"ind1": ind1, "ind2": ind2, "subfields": subfields}
        )

    return fields


def extract_fields_from_record(record_dict):
    """Extract normalized fields from our converter's record dict."""
    fields = {}
    fields["leader"] = record_dict.get("leader", "")

    for cf in record_dict.get("controlfields", []):
        tag = cf["tag"]
        fields.setdefault(tag, []).append({"value": cf["value"]})

    for df in record_dict.get("datafields", []):
        tag = df["tag"]
        fields.setdefault(tag, []).append(
            {
                "ind1": df.get("ind1", " "),
                "ind2": df.get("ind2", " "),
                "subfields": [
                    {"code": s["code"], "value": s["value"]}
                    for s in df.get("subfields", [])
                ],
            }
        )

    return fields


def format_subfields(subfields):
    """Format subfields for display."""
    return " ".join(f"${s['code']}{s['value']}" for s in subfields)


def compare_records(qid, our_record, gnd_fields):
    """Compare our converted record against the real GND record."""
    our_fields = extract_fields_from_record(our_record)

    print(f"\n{'='*80}")
    print(f"  {qid}: {our_record.get('label', '?')}")
    gnd_id = ""
    for f in our_fields.get("024", []):
        for sf in f.get("subfields", []):
            if sf["code"] == "a":
                gnd_id = sf["value"]
    print(f"  GND ID: {gnd_id}")
    print(f"{'='*80}")

    # Tags to compare (skip dynamic fields like 005/008, and FactGrid-specific ones)
    skip_tags = {"005", "008", "leader"}
    # Tags only we have (FactGrid-specific)
    our_only_tags = {"003", "040", "042", "079", "667", "670"}
    # Tags to compare directly
    compare_tags = {"100", "400", "548", "550", "551", "075"}

    all_tags = set()
    for t in our_fields:
        if t not in skip_tags:
            all_tags.add(t)
    for t in gnd_fields:
        if t not in skip_tags:
            all_tags.add(t)

    issues = []
    matches = []
    info = []

    for tag in sorted(all_tags):
        in_ours = tag in our_fields
        in_gnd = tag in gnd_fields

        if tag in our_only_tags:
            # Expected to differ — these are our catalog-specific fields
            if in_ours:
                for f in our_fields[tag]:
                    sfs = format_subfields(f.get("subfields", []))
                    info.append(f"  [INFO] {tag} (nur FG): {sfs or f.get('value', '')}")
            continue

        if in_ours and not in_gnd:
            for f in our_fields[tag]:
                sfs = format_subfields(f.get("subfields", []))
                info.append(f"  [INFO] {tag} nur in FG-Export: {sfs or f.get('value', '')}")
            continue

        if in_gnd and not in_ours:
            for f in gnd_fields[tag]:
                sfs = format_subfields(f.get("subfields", []))
                issues.append(f"  [FEHLT] {tag} in GND vorhanden, fehlt bei uns: {sfs or f.get('value', '')}")
            continue

        # Both have this tag — compare content
        if tag in compare_tags:
            our_list = our_fields[tag]
            gnd_list = gnd_fields[tag]

            # Compare key subfields
            if tag == "100":
                # Compare preferred name
                our_name = ""
                our_dates = ""
                gnd_name = ""
                gnd_dates = ""
                for f in our_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            our_name = s["value"]
                        if s["code"] == "d":
                            our_dates = s["value"]
                for f in gnd_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            gnd_name = s["value"]
                        if s["code"] == "d":
                            gnd_dates = s["value"]

                if our_name == gnd_name:
                    matches.append(f"  [OK]    100 $a Name stimmt ueberein: {our_name}")
                else:
                    issues.append(f"  [DIFF]  100 $a Name: FG=\"{our_name}\" vs GND=\"{gnd_name}\"")

                if our_dates == gnd_dates:
                    matches.append(f"  [OK]    100 $d Daten stimmen ueberein: {our_dates}")
                elif our_dates and gnd_dates:
                    issues.append(f"  [DIFF]  100 $d Daten: FG=\"{our_dates}\" vs GND=\"{gnd_dates}\"")

            elif tag == "375":
                our_gender = ""
                gnd_gender = ""
                for f in our_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            our_gender = s["value"]
                for f in gnd_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            gnd_gender = s["value"]
                if our_gender == gnd_gender:
                    matches.append(f"  [OK]    375 Geschlecht stimmt ueberein: {our_gender}")
                else:
                    issues.append(f"  [DIFF]  375 Geschlecht: FG=\"{our_gender}\" vs GND=\"{gnd_gender}\"")

            elif tag == "548":
                # Compare date ranges
                our_dates_set = set()
                gnd_dates_set = set()
                for f in our_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            our_dates_set.add(s["value"])
                for f in gnd_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            gnd_dates_set.add(s["value"])
                common = our_dates_set & gnd_dates_set
                only_ours = our_dates_set - gnd_dates_set
                only_gnd = gnd_dates_set - our_dates_set
                if common:
                    matches.append(f"  [OK]    548 Gemeinsame Daten: {', '.join(common)}")
                if only_ours:
                    info.append(f"  [INFO]  548 Nur in FG: {', '.join(only_ours)}")
                if only_gnd:
                    issues.append(f"  [DIFF]  548 Nur in GND: {', '.join(only_gnd)}")

            elif tag == "551":
                # Compare places
                our_places = set()
                gnd_places = set()
                for f in our_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            our_places.add(s["value"])
                for f in gnd_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            gnd_places.add(s["value"])
                common = our_places & gnd_places
                only_ours = our_places - gnd_places
                only_gnd = gnd_places - our_places
                if common:
                    matches.append(f"  [OK]    551 Gemeinsame Orte: {', '.join(common)}")
                if only_ours:
                    info.append(f"  [INFO]  551 Nur in FG: {', '.join(only_ours)}")
                if only_gnd:
                    issues.append(f"  [DIFF]  551 Nur in GND: {', '.join(only_gnd)}")

            elif tag == "024":
                our_gnd = ""
                gnd_gnd = ""
                for f in our_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            our_gnd = s["value"]
                for f in gnd_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            gnd_gnd = s["value"]
                if our_gnd == gnd_gnd:
                    matches.append(f"  [OK]    024 GND-ID stimmt ueberein: {our_gnd}")
                else:
                    issues.append(f"  [DIFF]  024 GND-ID: FG=\"{our_gnd}\" vs GND=\"{gnd_gnd}\"")

            elif tag == "400":
                our_aliases = set()
                gnd_aliases = set()
                for f in our_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            our_aliases.add(s["value"])
                for f in gnd_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            gnd_aliases.add(s["value"])
                common = our_aliases & gnd_aliases
                only_ours = our_aliases - gnd_aliases
                only_gnd = gnd_aliases - our_aliases
                if common:
                    matches.append(f"  [OK]    400 Gemeinsame Namensformen: {', '.join(common)}")
                if only_ours:
                    info.append(f"  [INFO]  400 Nur in FG: {', '.join(only_ours)}")
                if only_gnd:
                    info.append(f"  [INFO]  400 Nur in GND: {', '.join(only_gnd)}")

            elif tag == "550":
                gnd_occupations = []
                for f in gnd_list:
                    for s in f.get("subfields", []):
                        if s["code"] == "a":
                            gnd_occupations.append(s["value"])
                if gnd_occupations:
                    issues.append(
                        f"  [FEHLT] 550 Berufe in GND: {', '.join(gnd_occupations)} "
                        f"(manuell im UI hinzufuegen)"
                    )
        else:
            # Tags present in both but not specifically compared
            info.append(f"  [INFO]  {tag} in beiden vorhanden (nicht detailliert verglichen)")

    # Check for 550 in GND but not in ours
    if "550" in gnd_fields and "550" not in our_fields:
        gnd_occupations = []
        for f in gnd_fields["550"]:
            for s in f.get("subfields", []):
                if s["code"] == "a":
                    gnd_occupations.append(s["value"])
        if gnd_occupations:
            issues.append(
                f"  [FEHLT] 550 Berufe in GND: {', '.join(gnd_occupations)} "
                f"(kein Berufs-Property in FactGrid)"
            )

    # Print results
    if matches:
        print("\n  Uebereinstimmungen:")
        for m in matches:
            print(m)
    if issues:
        print("\n  Abweichungen/Fehlende Felder:")
        for i in issues:
            print(i)
    if info:
        print("\n  Zusatzinfo:")
        for i in info:
            print(i)

    # Validation
    validation = validate_record(our_record)
    print(f"\n  Validierung: {validation['status'].upper()}")
    print(f"    Individualisierung: {validation['individualization_count']}/3 "
          f"(Gruppe 1: {validation['group1_count']}/1)")
    if validation["warnings"]:
        for w in validation["warnings"]:
            print(f"    ! {w}")

    return len(issues) == 0


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5

    print("Lade FactGrid Items...")
    items = load_factgrid_items(n)
    print(f"  {len(items)} Items geladen")

    # Build GND ID map for quick lookup
    qid_to_gnd = load_gnd_id_map()

    # Collect all referenced entity IDs for label resolution
    print("Resolve Labels via SPARQL...")
    all_ref_ids = set()
    for item in items:
        all_ref_ids.update(collect_referenced_entity_ids(item))
    resolved_labels = resolve_labels(list(all_ref_ids), lang="de")
    print(f"  {len(resolved_labels)} Labels aufgeloest")

    # Convert and compare
    total = 0
    ok_count = 0

    for item in items:
        qid = item["id"]
        gnd_id = qid_to_gnd.get(qid, "")
        if not gnd_id:
            # Try to get from claims
            claims = item.get("claims", {})
            p76 = claims.get("P76", [])
            if p76:
                snak = p76[0].get("mainsnak", {})
                dv = snak.get("datavalue", {})
                gnd_id = dv.get("value", "")

        if not gnd_id:
            print(f"\n  {qid}: Keine GND-ID gefunden, uebersprungen")
            continue

        # Convert
        try:
            record = convert_entity_to_marc(item, resolved_labels)
            record["validation"] = validate_record(record)
        except Exception as e:
            print(f"\n  {qid}: Konvertierungsfehler: {e}")
            continue

        # Find GND record
        print(f"\nSuche GND-Record fuer {gnd_id}...")
        gnd_elem = find_gnd_record(gnd_id)
        if gnd_elem is None:
            print(f"  GND-Record {gnd_id} nicht in gnd_matching_subset.xml gefunden")
            # Still show our output
            xml_out = records_to_marc_xml([record])
            print(f"\n  Unser Export:\n{xml_out[:2000]}")
            continue

        gnd_fields = extract_fields(gnd_elem)
        is_ok = compare_records(qid, record, gnd_fields)
        total += 1
        if is_ok:
            ok_count += 1

    print(f"\n{'='*80}")
    print(f"ZUSAMMENFASSUNG: {ok_count}/{total} Records ohne Abweichungen")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
