import { cohortFixtures, cohortPatientFixtures, generationJobFixtures } from './fixtures/cohorts';
import { modelFixtures } from './fixtures/models';
import { runFixtures } from './fixtures/runs';
import { copilotMessagesFixture, copilotStatusFixture } from './fixtures/copilot';

let fallbackInstalled = false;

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
    },
  });
}

export function installMockFetchFallback() {
  if (fallbackInstalled) {
    return;
  }

  const nativeFetch = window.fetch.bind(window);

  window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const requestUrl = typeof input === 'string' ? input : input.toString();
    const url = new URL(requestUrl, window.location.origin);
    const method = (init?.method ?? 'GET').toUpperCase();

    if (url.pathname === '/mock-api/cohorts' && method === 'GET') {
      return jsonResponse({ cohorts: cohortFixtures });
    }

    if (url.pathname === '/mock-api/generation-jobs' && method === 'GET') {
      return jsonResponse({ jobs: generationJobFixtures });
    }

    if (url.pathname === '/mock-api/generation-jobs' && method === 'POST') {
      const body = init?.body ? JSON.parse(String(init.body)) : {};
      return jsonResponse({
        jobId: `job-${Math.random().toString(16).slice(2, 10)}`,
        cohortId: body.cohortId ?? 'new-cohort',
        status: 'queued',
        progress: 0,
        currentPhase: 'queued',
        createdAt: new Date().toISOString(),
        estimatedRemainingSeconds: null,
      });
    }

    if (url.pathname.startsWith('/mock-api/cohorts/') && url.pathname.endsWith('/patients') && method === 'GET') {
      const cohortId = url.pathname.split('/')[3] ?? '';
      return jsonResponse({ patients: cohortPatientFixtures[cohortId] ?? [] });
    }

    if (url.pathname.startsWith('/mock-api/patients/') && method === 'GET') {
      const patientId = url.pathname.split('/')[3] ?? '';
      const patient = Object.values(cohortPatientFixtures)
        .flat()
        .find((item) => item.patientId === patientId);
      if (!patient) {
        return jsonResponse({ message: 'not found' }, 404);
      }
      return jsonResponse({
        ...patient,
        conditions: ['Chronic obstructive pulmonary disease', 'Hypertension'],
        observations: ['Body Mass Index', 'Tobacco smoking status'],
        medications: ['Albuterol 90 MCG/ACT inhaler'],
        lastEncounterDate: '2026-02-01',
      });
    }

    if (url.pathname === '/mock-api/models' && method === 'GET') {
      return jsonResponse({ models: modelFixtures });
    }

    if (url.pathname.startsWith('/mock-api/models/') && method === 'GET') {
      const imageTag = decodeURIComponent(url.pathname.replace('/mock-api/models/', ''));
      const model = modelFixtures.find((item) => item.imageTag === imageTag);
      if (!model) {
        return jsonResponse({ message: 'not found' }, 404);
      }
      return jsonResponse(model);
    }

    if (url.pathname === '/mock-api/runs' && method === 'GET') {
      return jsonResponse({ runs: runFixtures });
    }

    if (url.pathname === '/mock-api/runs' && method === 'POST') {
      const body = init?.body ? JSON.parse(String(init.body)) : {};
      return jsonResponse({
        runId: `run-${Math.random().toString(16).slice(2, 10)}`,
        imageTag: body.imageTag ?? 'coxcopdmodel:latest',
        modelTitle: 'Pending model execution',
        scope: body.scope ?? 'cohort',
        status: 'queued',
        createdAt: new Date().toISOString(),
        resultPreview: 'Queued for execution.',
        inputSnapshot: body.input ?? [],
        outputSnapshot: [],
      });
    }

    if (url.pathname.startsWith('/mock-api/runs/') && method === 'GET') {
      const runId = url.pathname.replace('/mock-api/runs/', '');
      const run = runFixtures.find((item) => item.runId === runId);
      if (!run) {
        return jsonResponse({ message: 'not found' }, 404);
      }
      return jsonResponse(run);
    }

    if (url.pathname === '/mock-api/copilot/status' && method === 'GET') {
      return jsonResponse(copilotStatusFixture);
    }

    if (url.pathname === '/mock-api/copilot/messages' && method === 'GET') {
      return jsonResponse({ messages: copilotMessagesFixture });
    }

    return nativeFetch(input, init);
  };

  fallbackInstalled = true;
}
