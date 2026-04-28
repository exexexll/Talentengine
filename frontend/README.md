# Frontend - Figwork Geographic Intelligence

React + TypeScript + Vite + MapLibre GL JS map dashboard.

## Setup

```bash
npm install
npm run dev     # dev server on http://localhost:3000
npm run build   # production build to dist/
```

Requires the backend API running on port 8000 (Vite proxies `/api/*` requests).

## Components

- `src/pages/map-dashboard.tsx` - Main map dashboard with MapLibre GL
- `src/components/layer-picker.tsx` - Map layer toggle controls
- `src/components/scenario-controls.tsx` - Scenario selector
- `src/components/profile-drawer.tsx` - Geography profile drawer
- `src/components/compare-panel.tsx` - Geography comparison panel
