import { Routes } from '@angular/router';
import { Conversion } from './conversion/conversion';

export const routes: Routes = [
  { path: '', component: Conversion },
  { path: '**', redirectTo: '' }
];
