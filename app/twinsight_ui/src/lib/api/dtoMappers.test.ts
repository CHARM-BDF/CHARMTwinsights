import { mapCohortSummary, mapModelDescriptor, mapPatientSummary, mapRunRecord } from './dtoMappers';

describe('dtoMappers', () => {
  it('maps cohort summary from snake_case payloads', () => {
    const result = mapCohortSummary({ cohort_id: 'cohort-a', patient_count: 8, status: 'active' });
    expect(result.cohortId).toBe('cohort-a');
    expect(result.patientCount).toBe(8);
  });

  it('maps model descriptor with defaults', () => {
    const result = mapModelDescriptor({ image: 'model:latest', title: 'Model title' });
    expect(result.imageTag).toBe('model:latest');
    expect(result.title).toBe('Model title');
  });

  it('maps run record shape', () => {
    const result = mapRunRecord({ run_id: 'run-1', image: 'model:latest', status: 'queued' });
    expect(result.runId).toBe('run-1');
    expect(result.imageTag).toBe('model:latest');
    expect(result.status).toBe('queued');
  });

  it('maps patients without forcing Unknown name placeholders', () => {
    const result = mapPatientSummary({ id: 'patient-1', birth_date: '2000-01-01' });
    expect(result.patientId).toBe('patient-1');
    expect(result.givenName).toBe('');
    expect(result.familyName).toBe('');
    expect(result.birthDate).toBe('2000-01-01');
  });
});
