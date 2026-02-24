export const serviceModes = ['mock', 'direct'] as const;
export type ServiceMode = (typeof serviceModes)[number];

type EndpointRegistry = {
  cohortsBaseUrl: string;
  patientsBaseUrl: string;
  modelsBaseUrl: string;
  runsBaseUrl: string;
  copilotBaseUrl: string;
};

function trimSlash(input: string) {
  return input.endsWith('/') ? input.slice(0, -1) : input;
}

const routerBase = trimSlash(import.meta.env.VITE_ROUTER_BASE ?? 'http://localhost:8000');

export function isServiceMode(value: unknown): value is ServiceMode {
  return value === 'mock' || value === 'direct';
}

export function getEndpointRegistry(mode: ServiceMode): EndpointRegistry {
  if (mode === 'mock') {
    return {
      cohortsBaseUrl: '/mock-api',
      patientsBaseUrl: '/mock-api',
      modelsBaseUrl: '/mock-api',
      runsBaseUrl: '/mock-api',
      copilotBaseUrl: '/mock-api',
    };
  }

  return {
    cohortsBaseUrl: routerBase,
    patientsBaseUrl: routerBase,
    modelsBaseUrl: routerBase,
    runsBaseUrl: routerBase,
    copilotBaseUrl: routerBase,
  };
}

export const endpointPathMap = {
  listCohorts: {
    mock: '/cohorts',
    direct: '/synthetic/synthea/list-all-cohorts',
  },
  listGenerationJobs: {
    mock: '/generation-jobs',
    direct: '/synthetic/synthea/synthetic-patients/jobs',
  },
  createGenerationIntent: {
    mock: '/generation-jobs',
    direct: '/synthetic/synthea/synthetic-patients',
  },
  listPatientsByCohort: {
    mock: '/cohorts/:cohortId/patients',
    direct: '/synthetic/synthea/list-all-patients',
  },
  listModels: {
    mock: '/models',
    direct: '/modeling/models',
  },
  listRuns: {
    mock: '/runs',
    direct: '/runs',
  },
  createRunIntent: {
    mock: '/runs',
    direct: '/runs',
  },
  copilotStatus: {
    mock: '/copilot/status',
    direct: '/health',
  },
  copilotMessages: {
    mock: '/copilot/messages',
    direct: '/mcp/messages',
  },
} as const;
