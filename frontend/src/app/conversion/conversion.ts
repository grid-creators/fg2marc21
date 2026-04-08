import { Component, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService, MarcRecord, MarcDataField, ValidationResult } from '../services/api';

@Component({
  selector: 'app-conversion',
  imports: [CommonModule, FormsModule],
  templateUrl: './conversion.html',
  styleUrl: './conversion.css',
})
export class Conversion {
  qidInput = '';
  dataSource: 'server' | 'local' = 'local';
  field079q = 'd';
  field667a = 'Historisches Datenzentrum Sachsen-Anhalt';
  field400Sources = { aliases: true, labels: true, p34: true };
  records: MarcRecord[] = [];
  errors: { qid: string; error: string }[] = [];
  selectedIndex = -1;
  loading = false;
  loadingProgress = '';

  // GND warnings per record (preserved across revalidation)
  gndWarnings: Map<string, string[]> = new Map();

  // For adding new fields
  showAddField = false;
  newFieldTag = '';
  newFieldInd1 = ' ';
  newFieldInd2 = ' ';
  newFieldSubCode = 'a';
  newFieldSubValue = '';

  // Allowed values for field 079 $q (Teilbestandskennzeichen)
  field079qOptions: { value: string; label: string }[] = [
    { value: 'a', label: 'a — Personennamen der Formalerschließung 1500–1850' },
    { value: 'd', label: 'd — Personennamen aus Dokumentationsbestand' },
    { value: 'e', label: 'e — Personennamen aus osteurop./islam. Kulturkreis' },
    { value: 'f', label: 'f — Formalerschließung' },
    { value: 'g', label: 'g — Gestaltungsmerkmal (DBSM)' },
    { value: 'h', label: 'h — Provenienzkennzeichen' },
    { value: 'l', label: 'l — Namen von Personen in Nachschlagewerken/Lexika' },
    { value: 'm', label: 'm — Musik (zusätzliches Kennzeichen)' },
    { value: 'n', label: 'n — Personennamen des Mittelalters (PMA, gedruckt)' },
    { value: 'o', label: 'o — Personennamen des Mittelalters (PMA, ungedruckt)' },
    { value: 'p', label: 'p — Personennamen der Antike (PAN)' },
    { value: 's', label: 's — Sacherschließung' },
    { value: 't', label: 't — Vorläufige Ansetzung' },
    { value: 'z', label: 'z — Zentralkartei der Autographen (ZKA)' },
  ];

  constructor(private apiService: ApiService, private cdr: ChangeDetectorRef) {}

  get selectedRecord(): MarcRecord | null {
    return this.selectedIndex >= 0 && this.selectedIndex < this.records.length
      ? this.records[this.selectedIndex]
      : null;
  }

  parseQids(): string[] {
    return this.qidInput
      .split(/[\n,;\s]+/)
      .map((q) => q.trim().toUpperCase())
      .filter((q) => /^Q\d+$/.test(q));
  }

  convert(): void {
    const qids = this.parseQids();
    if (qids.length === 0) return;

    this.loading = true;
    this.records = [];
    this.errors = [];
    this.gndWarnings.clear();
    this.selectedIndex = -1;
    this.loadingProgress = 'Starte Konvertierung...';

    const field400Sources = Object.entries(this.field400Sources)
      .filter(([_, v]) => v)
      .map(([k]) => k)
      .join(',');
    this.apiService.convertStream(qids, this.dataSource, this.field079q, this.field667a, field400Sources).subscribe({
      next: (event) => {
        switch (event.type) {
          case 'progress':
            this.loadingProgress = event.message || '';
            break;
          case 'record':
            if (event.record) {
              const gndW = event.record.validation.warnings.filter(
                (w: string) => !w.startsWith('Pflichtfeld') && !w.startsWith('Nur ') && !w.startsWith('Mindestens')
              );
              this.gndWarnings.set(event.record.qid, gndW);
              this.records.push(event.record);
              if (this.selectedIndex < 0) {
                this.selectedIndex = 0;
              }
            }
            break;
          case 'error':
            this.errors.push({ qid: event.qid || '?', error: event.error || 'Unbekannter Fehler' });
            break;
          case 'done':
            this.loading = false;
            this.loadingProgress = '';
            break;
        }
        this.cdr.detectChanges();
      },
      error: () => {
        this.loading = false;
        this.loadingProgress = '';
        this.errors.push({ qid: '', error: 'Verbindung zum Server fehlgeschlagen' });
        this.cdr.detectChanges();
      },
    });
  }

  selectRecord(index: number): void {
    this.selectedIndex = index;
    this.showAddField = false;
  }

  getStatusIcon(record: MarcRecord): string {
    if (record.validation.status === 'error') return 'status-error';
    if (record.validation.warnings.length > 0) return 'status-warn';
    return 'status-ok';
  }

  getStatusLabel(record: MarcRecord): string {
    if (record.validation.status === 'error') {
      return `${record.validation.warnings.length} Fehler`;
    }
    if (record.validation.warnings.length > 0) {
      return `${record.validation.warnings.length} Warn.`;
    }
    return 'OK';
  }

  // --- GND selection for fields with alternatives ---

  getSelectedGndId(df: MarcDataField): string {
    const sf = df.subfields.find(s => s.code === '0' && s.value.startsWith('(DE-588)'));
    if (!sf) return '';
    return sf.value.replace('(DE-588)', '');
  }

  onGndSelect(df: MarcDataField, gndId: string): void {
    // Update $0 subfields with new GND ID
    for (const sf of df.subfields) {
      if (sf.code === '0' && sf.value.startsWith('(DE-588)')) {
        sf.value = `(DE-588)${gndId}`;
      } else if (sf.code === '0' && sf.value.startsWith('https://d-nb.info/gnd/')) {
        sf.value = `https://d-nb.info/gnd/${gndId}`;
      }
    }
    // Update $a subfield with the label of the selected GND alternative
    const alt = df.gnd_alternatives?.find(a => a.id === gndId);
    if (alt) {
      const sfA = df.subfields.find(s => s.code === 'a');
      if (sfA) {
        sfA.value = alt.label;
      }
    }
    this.cdr.detectChanges();
    this.revalidate();
  }

  // --- Warning navigation ---

  getWarningTag(warning: string): string | null {
    // "Pflichtfeld 043 (...)" or "Feld 551 (...)"
    const m = warning.match(/^(?:Pflichtfeld|Feld)\s+(\d{3})/);
    return m ? m[1] : null;
  }

  scrollToWarning(warning: string): void {
    const tag = this.getWarningTag(warning);
    if (!tag || !this.selectedRecord) return;

    // Extract quoted name from warning to find exact field, e.g. "Erfurt"
    const nameMatch = warning.match(/["\u201e]([^"\u201c\u201d]+)["\u201c\u201d]/);
    const name = nameMatch ? nameMatch[1] : null;

    // Try control fields first
    const cfEl = document.getElementById('field-' + tag);
    if (cfEl) {
      cfEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
      cfEl.classList.add('highlight-flash');
      setTimeout(() => cfEl.classList.remove('highlight-flash'), 1500);
      return;
    }

    // Find matching datafield index
    const dfs = this.selectedRecord.datafields;
    let targetIndex = -1;
    if (name) {
      targetIndex = dfs.findIndex(df =>
        df.tag === tag && df.subfields.some(sf => sf.code === 'a' && sf.value === name)
      );
    }
    if (targetIndex < 0) {
      targetIndex = dfs.findIndex(df => df.tag === tag);
    }
    if (targetIndex < 0) return;

    const el = document.getElementById('datafield-' + targetIndex);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      el.classList.add('highlight-flash');
      setTimeout(() => el.classList.remove('highlight-flash'), 1500);
    }
  }

  // --- Editing ---

  onControlFieldChange(): void {
    this.revalidate();
  }

  onSubfieldChange(): void {
    this.revalidate();
  }

  removeDataField(fieldIndex: number): void {
    if (!this.selectedRecord) return;
    this.selectedRecord.datafields.splice(fieldIndex, 1);
    this.revalidate();
  }

  removeSubfield(fieldIndex: number, subIndex: number): void {
    if (!this.selectedRecord) return;
    const field = this.selectedRecord.datafields[fieldIndex];
    field.subfields.splice(subIndex, 1);
    if (field.subfields.length === 0) {
      this.selectedRecord.datafields.splice(fieldIndex, 1);
    }
    this.revalidate();
  }

  addSubfield(fieldIndex: number): void {
    if (!this.selectedRecord) return;
    this.selectedRecord.datafields[fieldIndex].subfields.push({
      code: 'a',
      value: '',
    });
  }

  toggleAddField(): void {
    this.showAddField = !this.showAddField;
    this.newFieldTag = '';
    this.newFieldInd1 = ' ';
    this.newFieldInd2 = ' ';
    this.newFieldSubCode = 'a';
    this.newFieldSubValue = '';
  }

  addNewField(): void {
    if (!this.selectedRecord || !this.newFieldTag) return;

    const newField: MarcDataField = {
      tag: this.newFieldTag,
      ind1: this.newFieldInd1 || ' ',
      ind2: this.newFieldInd2 || ' ',
      subfields: [
        {
          code: this.newFieldSubCode || 'a',
          value: this.newFieldSubValue,
        },
      ],
    };

    this.selectedRecord.datafields.push(newField);
    // Sort by tag
    this.selectedRecord.datafields.sort((a, b) => a.tag.localeCompare(b.tag));
    this.showAddField = false;
    this.revalidate();
  }

  // --- Validation ---

  revalidate(): void {
    if (!this.selectedRecord) return;
    const qid = this.selectedRecord.qid;
    this.apiService.validateRecord(this.selectedRecord).subscribe({
      next: (validation) => {
        const gndW = this.gndWarnings.get(qid) || [];
        validation.warnings = [...gndW, ...validation.warnings];
        this.selectedRecord!.validation = validation;
        this.cdr.detectChanges();
      },
    });
  }

  // --- Export ---

  exportCurrent(): void {
    if (!this.selectedRecord) return;
    this.apiService.exportRecords([this.selectedRecord]).subscribe({
      next: (blob) => this.downloadBlob(blob, `${this.selectedRecord!.qid}_gnd.mrcx`),
      error: () => alert('Export fehlgeschlagen'),
    });
  }

  exportAll(): void {
    if (this.records.length === 0) return;
    this.apiService.exportRecords(this.records).subscribe({
      next: (blob) => this.downloadBlob(blob, 'gnd_export.mrcx'),
      error: () => alert('Export fehlgeschlagen'),
    });
  }

  private downloadBlob(blob: Blob, filename: string): void {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }
}
