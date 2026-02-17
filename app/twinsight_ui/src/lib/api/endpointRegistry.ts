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

const syntheaBase = trimSlash(import.meta.env.VITE_SYNTHEA_BASE ?? 'http://localhost:8003');
const statBase = trimSlash(import.meta.env.VITE_STAT_BASE ?? 'http://localhost:8001');
const modelBase = trimSlash(import.meta.env.VITE_MODEL_BASE ?? 'http://localhost:8004');
const mcpBase = trimSlash(import.meta.env.VITE_MCP_BASE ?? 'http://localhost:8006');

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
    cohortsBaseUrl: syntheaBase,
    patientsBaseUrl: statBase,
    modelsBaseUrl: modelBase,
    runsBaseUrl: modelBase,
    copilotBaseUrl: mcpBase,
  };
}

export const endpointPathMap = {
  listCohorts: {
    mock: '/cohorts',
    direct: '/list-all-cohorts',
  },
  listGenerationJobs: {
    mock: '/generation-jobs',
    direct: '/synthetic-patients/jobs',
  },
  createGenerationIntent: {
    mock: '/generation-jobs',
    direct: '/synthetic-patients',
  },
  listPatientsByCohort: {
    mock: '/cohorts/:cohortId/patients',
    direct: '/patients',
  },
  listModels: {
    mock: '/models',
    direct: '/models',
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
