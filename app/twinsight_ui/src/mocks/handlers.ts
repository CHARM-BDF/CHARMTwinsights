import { delay, http, HttpResponse } from 'msw';
import { cohortFixtures, cohortPatientFixtures, generationJobFixtures } from './fixtures/cohorts';
import { modelFixtures } from './fixtures/models';
import { runFixtures } from './fixtures/runs';
import { copilotMessagesFixture, copilotStatusFixture } from './fixtures/copilot';

const withLatency = async () => {
  await delay(120);
};

function escapeRegex(path: string) {
  return path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function matchPath(path: string) {
  return new RegExp(`.*${escapeRegex(path)}$`);
}

const cohortListPaths = ['/mock-api/cohorts', '/synthetic/synthea/list-all-cohorts', '/list-all-cohorts'];
const jobsPaths = ['/mock-api/generation-jobs', '/synthetic/synthea/synthetic-patients/jobs', '/synthetic-patients/jobs'];
const modelsPaths = ['/mock-api/models', '/modeling/models', '/models'];
const runsPaths = ['/mock-api/runs', '/modeling/runs', '/runs'];
const copilotStatusPaths = ['/mock-api/copilot/status', '/mcp/health', '/health'];
const copilotMessagesPaths = ['/mock-api/copilot/messages', '/mcp/messages'];
const patientsListPaths = ['/stats/patients', '/patients'];

export const handlers = [
  ...cohortListPaths.map((path) =>
    http.get(matchPath(path), async () => {
      await withLatency();
      return HttpResponse.json({ cohorts: cohortFixtures });
    }),
  ),

  ...jobsPaths.map((path) =>
    http.get(matchPath(path), async () => {
      await withLatency();
      return HttpResponse.json({ jobs: generationJobFixtures });
    }),
  ),

  ...jobsPaths.map((path) =>
    http.post(matchPath(path), async ({ request }) => {
      const body = (await request.json()) as Record<string, unknown>;
      await withLatency();
      return HttpResponse.json({
        jobId: `job-${Math.random().toString(16).slice(2, 10)}`,
        cohortId: String(body.cohortId ?? body.cohort_id ?? 'new-cohort'),
        status: 'queued',
        progress: 0,
        currentPhase: 'queued',
        createdAt: new Date().toISOString(),
        estimatedRemainingSeconds: null,
      });
    }),
  ),

  http.get('/mock-api/cohorts/:cohortId/patients', async ({ params }) => {
    await withLatency();
    const cohortId = String(params.cohortId);
    return HttpResponse.json({ patients: cohortPatientFixtures[cohortId] ?? [] });
  }),

  ...patientsListPaths.map((path) =>
    http.get(matchPath(path), async ({ request }) => {
      await withLatency();
      const cohortId = new URL(request.url).searchParams.get('cohort_id');
      if (!cohortId) {
        return HttpResponse.json({ patients: Object.values(cohortPatientFixtures).flat() });
      }
      return HttpResponse.json({ patients: cohortPatientFixtures[cohortId] ?? [] });
    }),
  ),

  http.get('/mock-api/patients/:patientId', async ({ params }) => {
    await withLatency();
    const patientId = String(params.patientId);
    const patient = Object.values(cohortPatientFixtures)
      .flat()
      .find((item) => item.patientId === patientId);

    if (!patient) {
      return HttpResponse.json({ message: 'not found' }, { status: 404 });
    }

    return HttpResponse.json({
      ...patient,
      conditions: ['Chronic obstructive pulmonary disease', 'Hypertension'],
      observations: ['Body Mass Index', 'Tobacco smoking status'],
      medications: ['Albuterol 90 MCG/ACT inhaler'],
      lastEncounterDate: '2026-02-01',
    });
  }),

  http.get(/.*\/stats\/patients\/.+$/, async ({ request }) => {
    await withLatency();
    const patientId = new URL(request.url).pathname.split('/').pop() ?? '';
    const patient = Object.values(cohortPatientFixtures)
      .flat()
      .find((item) => item.patientId === patientId);
    if (!patient) {
      return HttpResponse.json({ message: 'not found' }, { status: 404 });
    }
    return HttpResponse.json({
      ...patient,
      conditions: ['Chronic obstructive pulmonary disease', 'Hypertension'],
      observations: ['Body Mass Index', 'Tobacco smoking status'],
      medications: ['Albuterol 90 MCG/ACT inhaler'],
      lastEncounterDate: '2026-02-01',
    });
  }),

  http.get(/.*\/patients\/[^/]+$/, async ({ request }) => {
    await withLatency();
    const patientId = new URL(request.url).pathname.split('/').pop() ?? '';
    const patient = Object.values(cohortPatientFixtures)
      .flat()
      .find((item) => item.patientId === patientId);
    if (!patient) {
      return HttpResponse.json({ message: 'not found' }, { status: 404 });
    }
    return HttpResponse.json({
      ...patient,
      conditions: ['Chronic obstructive pulmonary disease', 'Hypertension'],
      observations: ['Body Mass Index', 'Tobacco smoking status'],
      medications: ['Albuterol 90 MCG/ACT inhaler'],
      lastEncounterDate: '2026-02-01',
    });
  }),

  ...modelsPaths.map((path) =>
    http.get(matchPath(path), async () => {
      await withLatency();
      return HttpResponse.json({ models: modelFixtures });
    }),
  ),

  http.get('/mock-api/models/:imageTag', async ({ params }) => {
    await withLatency();
    const imageTag = decodeURIComponent(String(params.imageTag));
    const model = modelFixtures.find((item) => item.imageTag === imageTag);
    if (!model) {
      return HttpResponse.json({ message: 'not found' }, { status: 404 });
    }
    return HttpResponse.json(model);
  }),

  http.get(/.*\/modeling\/models\/.+$/, async ({ request }) => {
    await withLatency();
    const imageTag = decodeURIComponent(new URL(request.url).pathname.split('/').pop() ?? '');
    const model = modelFixtures.find((item) => item.imageTag === imageTag);
    if (!model) {
      return HttpResponse.json({ message: 'not found' }, { status: 404 });
    }
    return HttpResponse.json(model);
  }),

  ...runsPaths.map((path) =>
    http.get(matchPath(path), async () => {
      await withLatency();
      return HttpResponse.json({ runs: runFixtures });
    }),
  ),

  ...runsPaths.map((path) =>
    http.post(matchPath(path), async ({ request }) => {
      const body = (await request.json()) as Record<string, unknown>;
      await withLatency();
      return HttpResponse.json({
        runId: `run-${Math.random().toString(16).slice(2, 10)}`,
        imageTag: String(body.imageTag ?? body.image ?? 'coxcopdmodel:latest'),
        modelTitle: 'Pending model execution',
        scope: body.scope ?? 'cohort',
        status: 'queued',
        createdAt: new Date().toISOString(),
        resultPreview: 'Queued for execution.',
        inputSnapshot: body.input ?? [],
        outputSnapshot: [],
      });
    }),
  ),

  http.get('/mock-api/runs/:runId', async ({ params }) => {
    await withLatency();
    const runId = String(params.runId);
    const run = runFixtures.find((item) => item.runId === runId);
    if (!run) {
      return HttpResponse.json({ message: 'not found' }, { status: 404 });
    }
    return HttpResponse.json(run);
  }),

  http.get(/.*\/modeling\/runs\/.+$/, async ({ request }) => {
    await withLatency();
    const runId = new URL(request.url).pathname.split('/').pop() ?? '';
    const run = runFixtures.find((item) => item.runId === runId);
    if (!run) {
      return HttpResponse.json({ message: 'not found' }, { status: 404 });
    }
    return HttpResponse.json(run);
  }),

  ...copilotStatusPaths.map((path) =>
    http.get(matchPath(path), async () => {
      await withLatency();
      return HttpResponse.json(copilotStatusFixture);
    }),
  ),

  ...copilotMessagesPaths.map((path) =>
    http.get(matchPath(path), async () => {
      await withLatency();
      return HttpResponse.json({ messages: copilotMessagesFixture });
    }),
  ),
];
