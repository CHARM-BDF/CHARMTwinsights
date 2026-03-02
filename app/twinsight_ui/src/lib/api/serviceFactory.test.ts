import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createApiServices } from './serviceFactory';

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
    },
  });
}

describe('serviceFactory cohort service (direct mode)', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('maps cohort generation intent payload to backend async job contract', async () => {
    const fetchMock = vi.mocked(fetch);
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        job_id: 'job-123',
        status: 'queued',
        created_at: '2026-02-24T12:00:00Z',
      }),
    );

    const services = createApiServices('direct');
    const result = await services.cohortService.createGenerationIntent({
      cohortId: 'test-cohort',
      numPatients: 37,
      numYears: 3,
      minAge: 18,
      maxAge: 80,
      gender: 'both',
      state: '',
      city: '',
      usePopulationSampling: true,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain('/synthetic/synthea/synthetic-patients');
    expect(init?.method).toBe('POST');
    const body = JSON.parse(String(init?.body));
    expect(body).toEqual({
      cohort_id: 'test-cohort',
      num_patients: 37,
      num_years: 3,
      exporter: 'fhir',
      min_age: 18,
      max_age: 80,
      gender: 'both',
      use_population_sampling: true,
    });
    expect(result.jobId).toBe('job-123');
    expect(result.cohortId).toBe('test-cohort');
    expect(result.currentPhase).toBe('queued');
  });

  it('maps direct listGenerationJobs responses that return arrays', async () => {
    const fetchMock = vi.mocked(fetch);
    fetchMock.mockResolvedValueOnce(
      jsonResponse([
        {
          job_id: 'job-1',
          status: 'running',
          progress: 25,
          current_phase: 'chunk 1/4',
          created_at: '2026-02-24T11:00:00Z',
          request_data: { cohort_id: 'test-cohort' },
        },
      ]),
    );

    const services = createApiServices('direct');
    const jobs = await services.cohortService.listGenerationJobs();

    expect(jobs).toHaveLength(1);
    expect(jobs[0].jobId).toBe('job-1');
    expect(jobs[0].cohortId).toBe('test-cohort');
    expect(jobs[0].progress).toBe(0.25);
  });
});
