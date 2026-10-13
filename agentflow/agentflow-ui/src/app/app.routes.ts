import { Routes } from '@angular/router';

export const routes: Routes = [
  { path: '', loadComponent: () => import('./pages/dashboard').then((m) => m.DashboardPage) },
  { path: 'runs', loadComponent: () => import('./pages/runs').then((m) => m.RunsPage) },
  { path: 'runs/:id', loadComponent: () => import('./pages/run-detail').then((m) => m.RunDetailPage) },
  { path: 'workflows', loadComponent: () => import('./pages/workflows').then((m) => m.WorkflowsPage) },
  { path: 'generate', loadComponent: () => import('./pages/generate').then((m) => m.GeneratePage) },
  {
    path: 'workflows/:id',
    loadComponent: () => import('./pages/workflow-detail').then((m) => m.WorkflowDetailPage),
  },
  { path: 'agents', loadComponent: () => import('./pages/agents').then((m) => m.AgentsPage) },
  { path: 'testbench', loadComponent: () => import('./pages/testbench').then((m) => m.TestbenchPage) },
  { path: 'hitl', loadComponent: () => import('./pages/hitl').then((m) => m.HitlPage) },
  { path: 'observe', loadComponent: () => import('./pages/observe').then((m) => m.ObservePage) },
  { path: 'examples', loadComponent: () => import('./pages/examples').then((m) => m.ExamplesPage) },
  { path: 'settings', loadComponent: () => import('./pages/settings').then((m) => m.SettingsPage) },
  { path: '**', redirectTo: '' },
];
