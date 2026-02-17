import type { CohortSummary, GenerationJob, PatientSummary } from '../../lib/contracts/types';

export const cohortFixtures: CohortSummary[] = [
  {
    cohortId: 'copd-risk-2026-q1',
    patientCount: 124,
    source: 'synthetic',
    createdAt: '2026-01-08T09:30:00Z',
    status: 'active',
  },
  {
    cohortId: 'mobile-external-pilot',
    patientCount: 42,
    source: 'external',
    createdAt: '2026-01-14T12:15:00Z',
    status: 'active',
  },
  {
    cohortId: 'asthma-regression-baseline',
    patientCount: 310,
    source: 'synthetic',
    createdAt: '2025-12-19T16:45:00Z',
    status: 'archived',
  },
];

export const generationJobFixtures: GenerationJob[] = [
  {
    jobId: 'job-a1b2c3d4',
    cohortId: 'copd-risk-2026-q1',
    status: 'running',
    progress: 0.56,
    currentPhase: 'Chunk 7/12: uploading generated bundles',
    createdAt: '2026-02-17T08:03:00Z',
    estimatedRemainingSeconds: 210,
  },
  {
    jobId: 'job-e5f6g7h8',
    cohortId: 'mobile-external-pilot',
    status: 'completed',
    progress: 1,
    currentPhase: 'completed',
    createdAt: '2026-02-16T18:41:00Z',
    estimatedRemainingSeconds: 0,
  },
];

export const cohortPatientFixtures: Record<string, PatientSummary[]> = {
  'copd-risk-2026-q1': [
    {
      patientId: 'pt-1001',
      givenName: 'Marta',
      familyName: 'Lewis',
      gender: 'female',
      birthDate: '1974-03-16',
      cohortId: 'copd-risk-2026-q1',
    },
    {
      patientId: 'pt-1002',
      givenName: 'Jon',
      familyName: 'Park',
      gender: 'male',
      birthDate: '1968-09-08',
      cohortId: 'copd-risk-2026-q1',
    },
  ],
  'mobile-external-pilot': [
    {
      patientId: 'ext-2001',
      givenName: 'Asha',
      familyName: 'Moran',
      gender: 'female',
      birthDate: '1985-11-04',
      cohortId: 'mobile-external-pilot',
    },
  ],
};
