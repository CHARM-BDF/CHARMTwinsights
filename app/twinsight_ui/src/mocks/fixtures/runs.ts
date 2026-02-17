import type { ModelRunRecord } from '../../lib/contracts/types';

export const runFixtures: ModelRunRecord[] = [
  {
    runId: 'run-20260217-001',
    imageTag: 'coxcopdmodel:latest',
    modelTitle: 'COPD Survival Risk (Cox PH)',
    scope: 'cohort',
    status: 'completed',
    createdAt: '2026-02-17T08:19:00Z',
    resultPreview: 'High-risk segment detected for 18 of 124 patients.',
    inputSnapshot: [{ cohort_id: 'copd-risk-2026-q1', record_count: 124 }],
    outputSnapshot: [{ high_risk_count: 18, median_survival_probability_5_years: 0.78 }],
  },
  {
    runId: 'run-20260216-013',
    imageTag: 'reachablefrommodel:latest',
    modelTitle: 'Ontology Reachable-From Demo',
    scope: 'single-patient',
    status: 'completed',
    createdAt: '2026-02-16T14:27:00Z',
    resultPreview: 'Normalized biological sex and adulthood flag returned.',
    inputSnapshot: [{ biological_sex: 'PATO:0000383', age_years: 34 }],
    outputSnapshot: [{ normalized_sex: 'female', is_adult: true }],
  },
  {
    runId: 'run-20260217-014',
    imageTag: 'coxcopdmodel:latest',
    modelTitle: 'COPD Survival Risk (Cox PH)',
    scope: 'single-patient',
    status: 'running',
    createdAt: '2026-02-17T09:01:00Z',
    resultPreview: 'Execution in progress.',
    inputSnapshot: [{ patient_id: 'pt-1002' }],
    outputSnapshot: [],
  },
];
