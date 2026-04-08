"""
Core conversion logic: FactGrid entity → GND MARC 21 record.
"""

from datetime import datetime

from lxml import etree
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import (
    fetch_entity,
    resolve_labels,
    resolve_gnd_ids,
    resolve_gnd_preferred_names,
    extract_claim_values,
    format_wikibase_date,
    format_date_range,
    format_exact_date_range,
    build_preferred_name,
    format_005_timestamp,
    format_008_field,
    collect_referenced_entity_ids,
    resolve_country_code_for_place,
)
from mappings_config import (
    ISIL,
    LEADER,
    CONSTANT_CONTROLFIELDS,
    CONSTANT_DATAFIELDS,
    PROP_GND_ID,
    PROP_BIRTH_DATE,
    PROP_DEATH_DATE,
    PROP_FAMILY_NAME,
    PROP_GIVEN_NAME,
    PROP_OCCUPATION,
    PROP_BIRTH_PLACE,
    PROP_DEATH_PLACE,
    PROP_PLACE_OF_ACTIVITY,
    PROP_PLACE_OF_ACTIVITY_2,
    MANDATORY_TAGS,
    INDIVIDUALIZATION_GROUP1,
    INDIVIDUALIZATION_GROUP2,
    MIN_INDIVIDUALIZATION_TOTAL,
    MIN_INDIVIDUALIZATION_GROUP1,
    FIELD_DESCRIPTIONS,
)

MARC_NS = "http://www.loc.gov/MARC21/slim"


def convert_entities(qids, source="server", field079q="d", field667a="Historisches Datenzentrum Sachsen-Anhalt", field400sources=None):
    """Convert a list of FactGrid QIDs to MARC 21 records.

    Returns: {"records": [...], "errors": [...]}
    """
    records = []
    errors = []
    for event in convert_entities_stream(qids, source=source, field079q=field079q, field667a=field667a, field400sources=field400sources):
        if event["type"] == "record":
            records.append(event["record"])
        elif event["type"] == "error":
            errors.append({"qid": event["qid"], "error": event["error"]})
    return {"records": records, "errors": errors}


def convert_entities_stream(qids, source="server", field079q="d", field667a="Historisches Datenzentrum Sachsen-Anhalt", field400sources=None):
    """Convert QIDs to MARC 21 records, yielding progress events.

    Yields dicts: {"type": "progress", "message": "..."} or
                  {"type": "record", "record": {...}} or
                  {"type": "error", "qid": "...", "error": "..."} or
                  {"type": "done"}
    """
    total = len(qids)
    source_label = "lokaler Datenbank" if source == "local" else "FactGrid"

    # Fetch all entities in parallel
    entities = {}
    yield {"type": "progress", "message": f"Lade {total} Entitaet(en) von {source_label}..."}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_entity, qid, source): qid for qid in qids}
        fetched = 0
        for future in as_completed(futures):
            qid = futures[future]
            fetched += 1
            try:
                entities[qid] = future.result()
                yield {"type": "progress", "message": f"Entitaet {qid} geladen ({fetched}/{total})"}
            except Exception as e:
                yield {"type": "error", "qid": qid, "error": str(e)}

    # Collect all referenced entity IDs for batch label resolution
    all_ref_ids = set()
    for entity in entities.values():
        all_ref_ids.update(collect_referenced_entity_ids(entity))

    yield {"type": "progress", "message": f"Loese {len(all_ref_ids)} Labels auf..."}
    resolved_labels = resolve_labels(list(all_ref_ids), lang="de", source=source)

    yield {"type": "progress", "message": f"Loese GND-IDs auf..."}
    resolved_gnd_ids = resolve_gnd_ids(list(all_ref_ids), source=source)

    yield {"type": "progress", "message": f"Lade GND-Vorzugsbenennungen..."}
    gnd_preferred_names = resolve_gnd_preferred_names(resolved_gnd_ids)

    # Convert each entity
    converted = 0
    for qid in qids:
        if qid not in entities:
            continue
        converted += 1
        yield {"type": "progress", "message": f"Konvertiere {qid} ({converted}/{len(entities)})..."}
        try:
            record = convert_entity_to_marc(entities[qid], resolved_labels, resolved_gnd_ids, gnd_preferred_names, source=source, field079q=field079q, field667a=field667a, field400sources=field400sources)
            record["validation"] = validate_record(record)
            record["validation"]["warnings"] = record.pop("warnings") + record["validation"]["warnings"]
            yield {"type": "record", "record": record}
        except Exception as e:
            yield {"type": "error", "qid": qid, "error": str(e)}

    yield {"type": "done"}


def _get_gnd_id(resolved_gnd_ids, ref_qid, warnings, tag="", display_name="", gnd_preferred_names=None):
    """Get the first GND ID for a referenced entity, adding a warning if multiple exist."""
    gnd_list = resolved_gnd_ids.get(ref_qid, [])
    if not gnd_list:
        return ""
    if len(gnd_list) > 1:
        if gnd_preferred_names:
            alt_labels = [f"{gid} ({gnd_preferred_names.get(gid, '?')})" for gid in gnd_list]
        else:
            alt_labels = gnd_list
        prefix = f"Feld {tag}" if tag else ref_qid
        name_part = f" \"{display_name}\"" if display_name else ""
        warnings.append(
            f"{prefix}{name_part} hat mehrere GND-IDs: {', '.join(alt_labels)} — verwende {gnd_list[0]}"
        )
    return gnd_list[0]


def convert_entity_to_marc(entity, resolved_labels, resolved_gnd_ids=None, gnd_preferred_names=None, source="server", field079q="d", field667a="Historisches Datenzentrum Sachsen-Anhalt", field400sources=None):
    """Convert a single FactGrid entity to a MARC 21 record dict."""
    qid = entity.get("id", "")
    gnd_warnings = []

    # Get label for display
    labels = entity.get("labels", {})
    display_label = ""
    for lang in ["de", "en"]:
        if lang in labels:
            display_label = labels[lang]["value"]
            break

    # Build controlfields
    controlfields = [
        {"tag": "001", "value": qid},
        {"tag": "003", "value": CONSTANT_CONTROLFIELDS["003"]},
        {"tag": "005", "value": format_005_timestamp()},
        {"tag": "008", "value": format_008_field()},
    ]

    # Build datafields
    datafields = []

    # --- GND system number (035) ---
    gnd_claims = extract_claim_values(entity, PROP_GND_ID)
    if gnd_claims:
        gnd_id = gnd_claims[0]["value"]
        gnd_warnings.append(
            f"Person hat bereits eine GND-ID: {gnd_id}"
        )
    else:
        gnd_id = "null"
    datafields.append(
        {
            "tag": "035",
            "ind1": " ",
            "ind2": " ",
            "subfields": [
                {"code": "a", "value": f"(DE-588){gnd_id}"},
            ],
        }
    )

    # --- Constant datafields (040, 042, 075, 079) ---
    for field in CONSTANT_DATAFIELDS:
        subfields = []
        for sf in field["subfields"]:
            value = sf["value"]
            # Override 079 $q with user-selected Teilbestandskennzeichen
            if field["tag"] == "079" and sf["code"] == "q":
                value = field079q
            subfields.append({"code": sf["code"], "value": value})
        datafields.append(
            {
                "tag": field["tag"],
                "ind1": field["ind1"],
                "ind2": field["ind2"],
                "subfields": subfields,
            }
        )

    # --- Helper: GND preferred name lookup ---
    if resolved_gnd_ids is None:
        resolved_gnd_ids = {}
    if gnd_preferred_names is None:
        gnd_preferred_names = {}

    def _gnd_name(ref_qid):
        """Get display name: prefer GND preferred name, fallback to FactGrid label."""
        gnd_list = resolved_gnd_ids.get(ref_qid, [])
        if gnd_list:
            name = gnd_preferred_names.get(gnd_list[0], "")
            if name:
                return name
        return resolved_labels.get(ref_qid, ref_qid)

    # --- Country code (043) ---
    # Priority: Wirkungsort (P1372) → Sterbeort (P168) → Geburtsort (P82)
    country_code = ""
    country_code_source = ""
    for prop, label in [
        (PROP_PLACE_OF_ACTIVITY_2, "Wirkungsort"),
        (PROP_PLACE_OF_ACTIVITY, "Wirkungsort"),
        (PROP_DEATH_PLACE, "Sterbeort"),
        (PROP_BIRTH_PLACE, "Geburtsort"),
    ]:
        if country_code:
            break
        place_claims = extract_claim_values(entity, prop)
        for claim in place_claims:
            place_qid = claim["value"]
            code = resolve_country_code_for_place(place_qid, resolved_gnd_ids or {}, source=source)
            if code:
                country_code = code
                place_name = _gnd_name(place_qid)
                country_code_source = f"{label} {place_name}"
                break

    if country_code:
        datafields.append(
            {
                "tag": "043",
                "ind1": " ",
                "ind2": " ",
                "subfields": [{"code": "c", "value": country_code}],
            }
        )
    else:
        gnd_warnings.append(
            "Ländercode (043) konnte nicht ermittelt werden — "
            "kein Ort mit GND-ID oder Koordinaten gefunden"
        )

    # --- Preferred name (100) ---
    preferred_name = build_preferred_name(entity, resolved_labels)
    birth_claims = extract_claim_values(entity, PROP_BIRTH_DATE)
    death_claims = extract_claim_values(entity, PROP_DEATH_DATE)
    birth_val = birth_claims[0]["value"] if birth_claims else None
    death_val = death_claims[0]["value"] if death_claims else None
    date_range = format_date_range(birth_val, death_val)

    field_100_subfields = [{"code": "a", "value": preferred_name}]
    if date_range:
        field_100_subfields.append({"code": "d", "value": date_range})
    datafields.append(
        {
            "tag": "100",
            "ind1": "1",
            "ind2": " ",
            "subfields": field_100_subfields,
        }
    )

    # --- Variant names (400) from selected sources ---
    if field400sources is None:
        field400sources = ["aliases", "labels", "p34"]
    seen_variants = set()

    def _add_variant(name):
        if name and name != preferred_name and name not in seen_variants:
            seen_variants.add(name)
            subfields_400 = [{"code": "a", "value": name}]
            if date_range:
                subfields_400.append({"code": "d", "value": date_range})
            datafields.append(
                {
                    "tag": "400",
                    "ind1": "1",
                    "ind2": " ",
                    "subfields": subfields_400,
                }
            )

    if "aliases" in field400sources:
        aliases = entity.get("aliases", {})
        for lang, alias_list in aliases.items():
            for alias_entry in alias_list:
                _add_variant(alias_entry.get("value", ""))

    if "labels" in field400sources:
        labels = entity.get("labels", {})
        for lang, label_entry in labels.items():
            _add_variant(label_entry.get("value", ""))

    if "p34" in field400sources:
        p34_claims = extract_claim_values(entity, "P34")
        for claim in p34_claims:
            _add_variant(claim["value"])

    # --- Life dates (548) ---
    if birth_val or death_val:
        # Approximate dates (datl)
        year_range = format_date_range(birth_val, death_val)
        if year_range:
            datafields.append(
                {
                    "tag": "548",
                    "ind1": " ",
                    "ind2": " ",
                    "subfields": [
                        {"code": "a", "value": year_range},
                        {"code": "4", "value": "datl"},
                        {
                            "code": "4",
                            "value": "https://d-nb.info/standards/elementset/gnd#dateOfBirthAndDeath",
                        },
                        {"code": "w", "value": "r"},
                        {"code": "i", "value": "Lebensdaten"},
                    ],
                }
            )

        # Exact dates (datx) if precision >= 11
        exact_range = format_exact_date_range(birth_val, death_val)
        if exact_range and exact_range != year_range:
            datafields.append(
                {
                    "tag": "548",
                    "ind1": " ",
                    "ind2": " ",
                    "subfields": [
                        {"code": "a", "value": exact_range},
                        {"code": "4", "value": "datx"},
                        {
                            "code": "4",
                            "value": "https://d-nb.info/standards/elementset/gnd#dateOfBirthAndDeath",
                        },
                        {"code": "w", "value": "r"},
                        {"code": "i", "value": "Exakte Lebensdaten"},
                    ],
                }
            )

    # --- Occupation (550) - only those with GND ID ---
    occupation_claims = extract_claim_values(entity, PROP_OCCUPATION)
    # Filter to only those with GND ID
    occ_with_gnd = []
    for claim in occupation_claims:
        occ_qid = claim["value"]
        occ_name_preview = _gnd_name(occ_qid)
        gnd_id = _get_gnd_id(resolved_gnd_ids, occ_qid, gnd_warnings,
                             tag="550", display_name=occ_name_preview,
                             gnd_preferred_names=gnd_preferred_names)
        if gnd_id:
            occ_with_gnd.append((claim, gnd_id))

    for claim, gnd_id in occ_with_gnd:
        occ_qid = claim["value"]
        occ_name = _gnd_name(occ_qid)
        gnd_list = resolved_gnd_ids.get(occ_qid, [])
        # berc if preferred rank, or if it's the only occupation
        is_berc = claim["rank"] == "preferred" or len(occ_with_gnd) == 1
        code4 = "berc" if is_berc else "beru"
        label_i = "Charakteristischer Beruf" if is_berc else "Beruf"
        field = {
            "tag": "550",
            "ind1": " ",
            "ind2": " ",
            "subfields": [
                {"code": "0", "value": f"(DE-588){gnd_id}"},
                {"code": "0", "value": f"https://d-nb.info/gnd/{gnd_id}"},
                {"code": "a", "value": occ_name},
                {"code": "4", "value": code4},
                {
                    "code": "4",
                    "value": "https://d-nb.info/standards/elementset/gnd#professionOrOccupation",
                },
                {"code": "w", "value": "r"},
                {"code": "i", "value": label_i},
            ],
        }
        if len(gnd_list) > 1:
            field["gnd_alternatives"] = [
                {"id": gid, "label": gnd_preferred_names.get(gid, gid)}
                for gid in gnd_list
            ]
        datafields.append(field)

    # --- Places (551): birth, death, activity ---

    place_configs = [
        (PROP_BIRTH_PLACE, "ortg", "https://d-nb.info/standards/elementset/gnd#placeOfBirth", "Geburtsort"),
        (PROP_DEATH_PLACE, "orts", "https://d-nb.info/standards/elementset/gnd#placeOfDeath", "Sterbeort"),
        (PROP_PLACE_OF_ACTIVITY_2, "ortw", "https://d-nb.info/standards/elementset/gnd#placeOfActivity", "Wirkungsort"),
    ]
    for prop, code4, url4, label_i in place_configs:
        place_claims = extract_claim_values(entity, prop)
        for claim in place_claims:
            place_qid = claim["value"]
            place_name = _gnd_name(place_qid)
            gnd_id = _get_gnd_id(resolved_gnd_ids, place_qid, gnd_warnings,
                                 tag="551", display_name=place_name,
                                 gnd_preferred_names=gnd_preferred_names)
            gnd_list = resolved_gnd_ids.get(place_qid, [])
            subfields_551 = []
            if gnd_id:
                subfields_551.append({"code": "0", "value": f"(DE-588){gnd_id}"})
                subfields_551.append({"code": "0", "value": f"https://d-nb.info/gnd/{gnd_id}"})
            subfields_551.extend([
                {"code": "a", "value": place_name},
                {"code": "4", "value": code4},
                {"code": "4", "value": url4},
                {"code": "w", "value": "r"},
                {"code": "i", "value": label_i},
            ])
            field = {
                "tag": "551",
                "ind1": " ",
                "ind2": " ",
                "subfields": subfields_551,
            }
            if len(gnd_list) > 1:
                field["gnd_alternatives"] = [
                    {"id": gid, "label": gnd_preferred_names.get(gid, gid)}
                    for gid in gnd_list
                ]
            datafields.append(field)

    # --- Source note (670) ---
    datafields.append(
        {
            "tag": "670",
            "ind1": " ",
            "ind2": " ",
            "subfields": [
                {"code": "a", "value": "FactGrid"},
                {"code": "b", "value": f"Stand: {datetime.now().strftime('%d.%m.%Y')}"},
                {
                    "code": "u",
                    "value": f"https://database.factgrid.de/wiki/Item:{qid}",
                },
            ],
        }
    )

    # --- Editorial note (667) ---
    if field667a:
        datafields.append(
            {
                "tag": "667",
                "ind1": " ",
                "ind2": " ",
                "subfields": [
                    {"code": "a", "value": field667a},
                    {"code": "5", "value": ISIL},
                ],
            }
        )

    # Sort datafields by tag
    datafields.sort(key=lambda f: f["tag"])

    return {
        "qid": qid,
        "label": display_label,
        "leader": LEADER,
        "controlfields": controlfields,
        "datafields": datafields,
        "warnings": gnd_warnings,
    }


def validate_record(record):
    """Validate a MARC 21 record against GND Level 1 requirements.

    Returns validation dict with status info.
    """
    # Collect all tags present
    present_tags = set()
    for cf in record.get("controlfields", []):
        present_tags.add(cf["tag"])
    for df in record.get("datafields", []):
        present_tags.add(df["tag"])

    # Check mandatory fields
    mandatory_missing = [tag for tag in MANDATORY_TAGS if tag not in present_tags]

    # Count individualization attributes
    group1_present = [
        tag for tag in INDIVIDUALIZATION_GROUP1 if tag in present_tags
    ]
    group2_present = [
        tag for tag in INDIVIDUALIZATION_GROUP2 if tag in present_tags
    ]
    total_indiv = len(group1_present) + len(group2_present)

    # Build warnings
    warnings = []
    for tag in mandatory_missing:
        desc = FIELD_DESCRIPTIONS.get(tag, tag)
        warnings.append(f"Pflichtfeld {tag} ({desc}) fehlt")

    if total_indiv < MIN_INDIVIDUALIZATION_TOTAL:
        warnings.append(
            f"Nur {total_indiv} von {MIN_INDIVIDUALIZATION_TOTAL} "
            f"Individualisierungsmerkmalen vorhanden"
        )
    # Check for 550/551 fields missing $0 (GND reference)
    for df in record.get("datafields", []):
        if df["tag"] in ("550", "551"):
            has_gnd_ref = any(sf["code"] == "0" for sf in df.get("subfields", []))
            if not has_gnd_ref:
                sf_a = next((sf["value"] for sf in df["subfields"] if sf["code"] == "a"), "?")
                desc = "Beruf/Beschäftigung" if df["tag"] == "550" else "Geografikum"
                warnings.append(f"Feld {df['tag']} ({desc} \"{sf_a}\") hat keine GND-Referenz ($0)")

    if len(group1_present) < MIN_INDIVIDUALIZATION_GROUP1:
        missing_g1 = [
            f"{tag} ({desc})"
            for tag, desc in INDIVIDUALIZATION_GROUP1.items()
            if tag not in present_tags
        ]
        warnings.append(
            f"Mindestens {MIN_INDIVIDUALIZATION_GROUP1} Merkmal(e) aus Gruppe 1 "
            f"erforderlich: {', '.join(missing_g1)}"
        )

    # Determine status
    if mandatory_missing:
        status = "error"
    elif warnings:
        status = "warning"
    else:
        status = "ok"

    return {
        "status": status,
        "mandatory_missing": mandatory_missing,
        "individualization_count": total_indiv,
        "group1_count": len(group1_present),
        "group2_count": len(group2_present),
        "group1_present": group1_present,
        "group2_present": group2_present,
        "warnings": warnings,
    }


def records_to_marc_xml(records):
    """Serialize a list of MARC 21 record dicts to XML string.

    Returns well-formed MARC 21 XML with proper namespace.
    """
    nsmap = {None: MARC_NS}

    if len(records) == 1:
        root = _build_record_element(records[0], nsmap)
    else:
        root = etree.Element("collection", nsmap=nsmap)
        for record in records:
            rec_elem = _build_record_element(record, nsmap)
            root.append(rec_elem)

    return etree.tostring(
        root,
        xml_declaration=True,
        encoding="UTF-8",
        pretty_print=True,
    ).decode("utf-8")


def _build_record_element(record, nsmap):
    """Build a single <record> element from a record dict."""
    rec = etree.Element("record", nsmap=nsmap, type="Authority")

    # Leader
    leader = etree.SubElement(rec, "leader")
    leader.text = record.get("leader", LEADER)

    # Control fields
    for cf in record.get("controlfields", []):
        elem = etree.SubElement(rec, "controlfield", tag=cf["tag"])
        elem.text = cf["value"]

    # Data fields
    for df in record.get("datafields", []):
        elem = etree.SubElement(
            rec,
            "datafield",
            tag=df["tag"],
            ind1=df.get("ind1", " "),
            ind2=df.get("ind2", " "),
        )
        for sf in df.get("subfields", []):
            sub = etree.SubElement(elem, "subfield", code=sf["code"])
            sub.text = sf["value"]

    return rec
