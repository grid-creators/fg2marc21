# FactGrid to GND -- Frontend

Angular 21 Web-UI fuer die Konvertierung von FactGrid-Personendatensaetzen in GND MARC 21 XML.

## Entwicklung

```bash
npm install
ng serve
```

Oeffne http://localhost:4200. Das Backend muss unter http://127.0.0.1:5000 laufen.

## Projektstruktur

```
src/app/
├── conversion/       # Hauptkomponente: Konvertierung, Bearbeitung, Export
│   ├── conversion.ts
│   ├── conversion.html
│   └── conversion.css
├── services/
│   └── api.ts        # HTTP-Service fuer Backend-Kommunikation
├── app.ts            # Root-Komponente
├── app.routes.ts     # Routing
└── app.html
```

## Build

```bash
ng build
```

Artefakte werden in `dist/` abgelegt.

## Tests

```bash
ng test
```
