# FactGrid to GND Conversion Tool

Konvertiert Personendatensaetze aus [FactGrid](https://database.factgrid.de/) (Wikibase) in GND-Normdatensaetze im MARC 21 XML-Format (`.mrcx`) fuer die Deutsche Nationalbibliothek.

FactGrid ISIL: **DE-4218**

**Demo:** https://factgrid2gnd.grid-creators.com/

## Architektur

```
fg2marc21/
├── backend/              # Flask REST API (Python)
│   ├── app.py            # API-Endpunkte
│   ├── converter.py      # Kernlogik: FactGrid → MARC 21
│   ├── utils.py          # SPARQL-Abfragen, GND-Lookup (lokal + API)
│   ├── factgrid_local.py # Lokaler FactGrid-Zugriff via SQLite
│   ├── mappings_config.py # Feld-Mappings und Konstanten
│   └── lobid_cache.db    # Auto-generierter Cache (lobid.org)
├── frontend/             # Angular 21 Web-UI
│   └── src/app/
│       ├── conversion/   # Konvertierungs-Komponente (inkl. Datenquellen-Umschalter)
│       └── services/     # API-Service
├── scripts/              # Datenbank-Build-Skripte
│   ├── build_gnd_db.py             # GND-Personendatenbank erstellen
│   ├── build_gnd_sachbegriff_db.py # GND-Sachbegriffe-Datenbank erstellen
│   ├── build_factgrid_db.py        # FactGrid-Datenbank erstellen (Offline-Modus)
│   └── ...                         # Weitere Extraktions-/Vergleichsskripte
├── specs/                # Spezifikationen und Referenzdokumente
│   ├── Anforderungen GND_FG_neu.xlsx  # Anforderungsspezifikation
│   ├── GND_MARC_vollst_*.xlsx         # DNB PICA-zu-MARC Tabelle
│   └── *.pdf                          # MARC 21 Feldbeschreibungen
├── data/                 # Grosse Datendateien (nicht in Git)
├── gnd_persons.db        # Lokale GND-Personendatenbank (~470MB)
├── gnd_sachbegriffe.db   # Lokale GND-Sachbegriffe-Datenbank
└── factgrid.db           # FactGrid-Entitaeten fuer Offline-Modus
```

## Voraussetzungen

- Python 3.13+
- Node.js 20+
- Angular CLI 21+

## Installation

### Backend

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install flask flask-cors requests lxml
```

### GND-Datenbanken (optional, empfohlen)

Fuer schnelle lokale GND-Abfragen statt langsamer API-Aufrufe:

```bash
cd scripts
python build_gnd_sachbegriff_db.py  # Sachbegriffe (~207.000 Datensaetze)
python build_gnd_db.py              # Personen (~5 Mio. Datensaetze)
```

- **Sachbegriffe**: Erfordert `data/authorities-gnd-sachbegriff_dnbmarc_20260217.mrc.xml`. Erstellt `gnd_sachbegriffe.db`.
- **Personen**: Erfordert `data/authorities-gnd-person_dnbmarc_20260217.mrc.xml` (~26GB). Erstellt `gnd_persons.db` (~470MB).

Ohne diese Datenbanken werden GND-Daten ueber die lobid.org- und d-nb.info-APIs abgefragt (langsamer).

Zusaetzlich wird `backend/lobid_cache.db` automatisch beim ersten Konvertierungslauf erstellt und speichert GND-Vorzugsbenennungen fuer Entitaeten, die in keiner lokalen Datenbank gefunden wurden.

### FactGrid-Datenbank (fuer Offline-Modus)

Fuer die lokale Datenquelle (ohne FactGrid-Server):

```bash
cd scripts
python build_factgrid_db.py  # Erstellt factgrid.db aus data/subset_P2_Q7.json
```

Erfordert `data/subset_P2_Q7.json` (~3.4GB FactGrid-Personen-Export). Erstellt `factgrid.db` mit Tabellen fuer Entitaeten, Labels und GND-IDs.

### Frontend

```bash
cd frontend
npm install
```

## Starten

**Backend (Port 5000):**

```bash
cd backend
python app.py
```

**Frontend (Port 4200, Proxy auf Backend):**

```bash
cd frontend
ng serve
```

Anschliessend im Browser oeffnen: http://localhost:4200

Das Frontend leitet `/api`-Anfragen automatisch an das Backend weiter (konfiguriert in `frontend/proxy.conf.json`).

## Benutzung

1. **Datenquelle waehlen**: FactGrid Server (Live-Abfragen) oder Lokale Datenbank (Standard, Offline, erfordert `factgrid.db`).
2. **Konvertierungsoptionen einstellen** (vor dem Konvertieren):
   - **079 $q Teilbestandskennzeichen**: GND-Teilbestandskennzeichen auswaehlen (z.B. "d" = Dokumentationsbestand, "f" = Formalerschliessung).
   - **400 Quellen**: Quellen fuer abweichende Namensformen auswaehlen (Mehrfachauswahl): Aliases, Labels, P34.
   - **667 $a Redaktionelle Bemerkung**: Freitextfeld (Standard: "Historisches Datenzentrum Sachsen-Anhalt").
3. Eine oder mehrere FactGrid Q-IDs eingeben (z.B. `Q409`, `Q11298`), getrennt durch Komma, Leerzeichen oder Zeilenumbruch.
4. **Konvertieren** klicken.
5. In der Seitenleiste werden die konvertierten Datensaetze mit Statusanzeige aufgelistet:
   - Gruen = OK
   - Gelb = Warnungen (z.B. fehlende Individualisierungsmerkmale)
   - Rot = Fehler (Pflichtfelder fehlen)
6. Datensatz auswaehlen, um MARC-Felder zu bearbeiten, hinzuzufuegen oder zu entfernen.
7. Export als MARC 21 XML (`.mrcx`) -- einzeln oder alle zusammen.

## API-Endpunkte

| Methode | Pfad | Beschreibung |
|---------|------|--------------|
| `GET` | `/api/convert/<qid>?source=server` | Einzelne Q-ID konvertieren |
| `POST` | `/api/convert` | Mehrere Q-IDs konvertieren (`{"qids": [...], "source": "server"\|"local"}`) |
| `GET` | `/api/convert/stream?qids=Q1,Q2&source=local&field079q=d&field667a=...&field400sources=aliases,labels,p34` | SSE-Stream fuer Konvertierung mit Fortschritt |
| `POST` | `/api/convert/validate` | MARC-Record validieren |
| `POST` | `/api/convert/export` | Records als MARC 21 XML exportieren |

Parameter: `source` (`"server"` oder `"local"`, Standard: `"local"`), `field079q` (Teilbestandskennzeichen, Standard: `"d"`), `field667a` (Redaktionelle Bemerkung), `field400sources` (kommasepariert: `aliases`, `labels`, `p34`).

## Konvertierungslogik

### Verwendete FactGrid-Properties

| Property | Bedeutung |
|----------|-----------|
| P76 | GND-ID |
| P77 | Geburtsdatum |
| P38 | Sterbedatum |
| P82 | Geburtsort |
| P168 | Sterbeort |
| P83 | Ort der Adresse |
| P1372 | Wirkungsort |
| P165 | Beruf/Taetigkeit |
| P34 | Namensvariante (String) |
| P247 | Familienname |
| P248 | Vorname |

### Erzeugte MARC-Felder

| Feld | Inhalt |
|------|--------|
| 001 | FactGrid Q-Nummer |
| 003 | ISIL (DE-4218) |
| 005 | Zeitstempel |
| 008 | Feste Datenelemente |
| 035 | GND-Systemnummer |
| 040 | Katalogisierungsquelle |
| 042 | Authentifizierungscode |
| 043 | Laendercode (ISO 3166 / GND) |
| 075 | Entitaetentyp (piz) |
| 079 | Teilbestandskennzeichnung ($q waehlbar) |
| 100 | Bevorzugter Personenname (Nachname, Vorname Namenszusatz) |
| 400 | Abweichende Namensformen (aus Aliases, Labels, P34) |
| 548 | Lebensdaten |
| 550 | Beruf/Taetigkeit |
| 551 | Geografische Bezuege |
| 667 | Redaktionelle Bemerkung ($a frei eingebbar) |
| 670 | Quellenangabe (FactGrid) |

### Bevorzugter Name (Feld 100)

Format: `Nachname, Vorname Namenszusatz`

Namenszusaetze (von, van, de, zu, della usw.) werden automatisch aus dem Familiennamen-Label (P247) extrahiert und hinter die Vornamen (P248) gestellt:

| Familienname (P247) | Vornamen (P248) | Ergebnis (100 $a) |
|---|---|---|
| von Goethe | Johann Wolfgang | Goethe, Johann Wolfgang von |
| van der Waals | Johannes Diderik | Waals, Johannes Diderik van der |
| Mueller | Thomas | Mueller, Thomas |

### Abweichende Namensformen (Feld 400)

Feld 400 wird aus bis zu drei waehlbaren Quellen generiert (Mehrfachauswahl per Checkboxen):

- **Aliases** -- Alternative Bezeichnungen der Entitaet aus allen Sprachen
- **Labels** -- Labels der Entitaet aus allen Sprachen
- **P34** -- Namensvarianten (String-Property)

Duplikate und der bevorzugte Name (Feld 100) werden automatisch herausgefiltert. Lebensdaten ($d) werden angehaengt, sofern vorhanden.

### Laendercode-Ermittlung (Feld 043)

Der Laendercode wird ueber eine Prioritaetskette bestimmt:

1. Wirkungsort (P1372)
2. Ort der Adresse (P83)
3. Sterbeort (P168)
4. Geburtsort (P82)

Fuer jeden Ort wird zuerst versucht, den Code aus dem verknuepften GND-Datensatz zu extrahieren. Falls kein GND-Eintrag vorhanden ist, werden Geokoordinaten ueber Nominatim aufgeloest.

### GND-Anzeigenamen

Die Felder 550 (Beruf) und 551 (Orte) verwenden bevorzugte Namen aus der GND. Lookup-Reihenfolge:

1. Lokale GND-Sachbegriffe-Datenbank (`gnd_sachbegriffe.db`)
2. Lokale GND-Personendatenbank (`gnd_persons.db`)
3. Lobid-Cache (`backend/lobid_cache.db`)
4. lobid.org API (Fallback, Ergebnis wird im Cache gespeichert)

### Validierung

- Pflichtfelder: 001, 003, 005, 008, 035, 040, 043, 075, 079, 100, 548
- Individualisierung Level 1: mindestens 3 Merkmale, davon min. 1 aus Gruppe 1 (Lebensdaten, Beruf)

## Externe APIs

- **FactGrid SPARQL**: `https://database.factgrid.de/sparql` -- Entity-Daten
- **lobid.org**: `https://lobid.org/gnd/{id}.json` -- GND bevorzugte Namen
- **d-nb.info**: `https://d-nb.info/gnd/{id}/about/marcxml` -- GND MARC-Records
- **Nominatim**: Reverse Geocoding fuer Laendercode-Fallback
