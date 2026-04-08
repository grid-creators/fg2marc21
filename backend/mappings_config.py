"""
Mapping configuration: FactGrid properties to GND MARC 21 fields.

Based on 'Anforderungen GND_FG_neu.xlsx', sheet 'Personen_GND'.
"""

# FactGrid ISIL
ISIL = "DE-4218"

# Leader for person authority records
LEADER = "00000nz  a2200000nc 4500"

# Constant control fields
CONSTANT_CONTROLFIELDS = {
    "003": ISIL,
}

# Constant data fields (always included)
CONSTANT_DATAFIELDS = [
    {
        "tag": "040",
        "ind1": " ",
        "ind2": " ",
        "subfields": [
            {"code": "a", "value": ISIL},
            {"code": "9", "value": "r:DE-101"},
            {"code": "b", "value": "ger"},
            {"code": "e", "value": "rda"},
        ],
    },
    {
        "tag": "042",
        "ind1": " ",
        "ind2": " ",
        "subfields": [
            {"code": "a", "value": "gnd4"},
        ],
    },
    {
        "tag": "075",
        "ind1": " ",
        "ind2": " ",
        "subfields": [
            {"code": "b", "value": "p"},
            {"code": "2", "value": "gndgen"},
        ],
    },
    {
        "tag": "075",
        "ind1": " ",
        "ind2": " ",
        "subfields": [
            {"code": "b", "value": "piz"},
            {"code": "2", "value": "gndspec"},
        ],
    },
    {
        "tag": "079",
        "ind1": " ",
        "ind2": " ",
        "subfields": [
            {"code": "a", "value": "g"},
            {"code": "q", "value": "d"},
        ],
    },
]

# FactGrid property to MARC field mappings
# Each entry describes which MARC fields to generate from a FactGrid property.

# Property IDs
PROP_GND_ID = "P76"
PROP_BIRTH_DATE = "P77"
PROP_DEATH_DATE = "P38"
PROP_INSTANCE_OF = "P2"
PROP_OCCUPATION = "P165"  # Beruf/Taetigkeit
PROP_FAMILY_NAME = "P247"
PROP_GIVEN_NAME = "P248"
PROP_BIRTH_PLACE = "P82"  # Geburtsort
PROP_DEATH_PLACE = "P168"  # Sterbeort
PROP_PLACE_OF_ACTIVITY = "P83"  # Ort der Adresse (Wirkungsort)
PROP_PLACE_OF_ACTIVITY_2 = "P1372"  # Wirkungsort (alternative property)

# Validation: mandatory MARC tags for a valid GND person record
# Based on Relevanz=Pflicht in new requirements table
MANDATORY_TAGS = ["001", "003", "005", "008", "035", "040", "043", "075", "079", "100", "548"]

# Individualization fields for GND Level 1
# Need at least 3 total, at least 1 from Group 1
# Stufe values from Anforderungen GND_FG_neu.xlsx
INDIVIDUALIZATION_GROUP1 = {
    "548": "Lebensdaten (datl)",
    "550": "Beruf/Beschaeftigung",
}

INDIVIDUALIZATION_GROUP2 = {
    "551": "Geografischer Bezug",
}

MIN_INDIVIDUALIZATION_TOTAL = 3
MIN_INDIVIDUALIZATION_GROUP1 = 1

# MARC field descriptions (German) for UI display
FIELD_DESCRIPTIONS = {
    "001": "Kontrollnummer",
    "003": "ISIL der einspielenden Einrichtung",
    "005": "Zeitstempel der letzten Transaktion",
    "008": "Datenelemente mit fester Laenge",
    "035": "Maschinelle Erzeugung GND Nummer",
    "040": "Katalogisierungsquelle",
    "042": "Authentifizierungscode",
    "043": "Laendercode",
    "075": "Entitaetentyp",
    "079": "Teilbestandskennzeichnung",
    "100": "Bevorzugter Personenname",
    "400": "Abweichender Name",
    "548": "Lebensdaten",
    "550": "Beruf/Taetigkeit",
    "551": "Orte",
    "667": "Redaktionelle Bemerkung",
    "670": "Quellenangabe",
}
