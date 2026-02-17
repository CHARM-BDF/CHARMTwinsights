export type ServiceMode = 'mock' | 'router' | 'direct' | 'hybrid';

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
const syntheaBase = trimSlash(import.meta.env.VITE_SYNTHEA_BASE ?? 'http://localhost:8003');
const statBase = trimSlash(import.meta.env.VITE_STAT_BASE ?? 'http://localhost:8001');
const modelBase = trimSlash(import.meta.env.VITE_MODEL_BASE ?? 'http://localhost:8004');
const mcpBase = trimSlash(import.meta.env.VITE_MCP_BASE ?? 'http://localhost:8006');

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

  if (mode === 'router') {
    return {
      cohortsBaseUrl: routerBase,
      patientsBaseUrl: routerBase,
      modelsBaseUrl: routerBase,
      runsBaseUrl: routerBase,
      copilotBaseUrl: mcpBase,
    };
  }

  if (mode === 'direct') {
    return {
      cohortsBaseUrl: syntheaBase,
      patientsBaseUrl: statBase,
      modelsBaseUrl: modelBase,
      runsBaseUrl: modelBase,
      copilotBaseUrl: mcpBase,
    };
  }

  return {
    cohortsBaseUrl: routerBase,
    patientsBaseUrl: statBase,
    modelsBaseUrl: modelBase,
    runsBaseUrl: routerBase,
    copilotBaseUrl: mcpBase,
  };
}

export const endpointPathMap = {
  listCohorts: {
    mock: '/cohorts',
    router: '/synthetic/synthea/list-all-cohorts',
    direct: '/list-all-cohorts',
    hybrid: '/synthetic/synthea/list-all-cohorts',
  },
  listGenerationJobs: {
    mock: '/generation-jobs',
    router: '/synthetic/synthea/synthetic-patients/jobs',
    direct: '/synthetic-patients/jobs',
    hybrid: '/synthetic/synthea/synthetic-patients/jobs',
  },
  createGenerationIntent: {
    mock: '/generation-jobs',
    router: '/synthetic/synthea/synthetic-patients',
    direct: '/synthetic-patients',
    hybrid: '/synthetic/synthea/synthetic-patients',
  },
  listPatientsByCohort: {
    mock: '/cohorts/:cohortId/patients',
    router: '/stats/patients',
    direct: '/patients',
    hybrid: '/patients',
  },
  listModels: {
    mock: '/models',
    router: '/modeling/models',
    direct: '/models',
    hybrid: '/models',
  },
  listRuns: {
    mock: '/runs',
    router: '/modeling/runs',
    direct: '/runs',
    hybrid: '/modeling/runs',
  },
  createRunIntent: {
    mock: '/runs',
    router: '/modeling/runs',
    direct: '/runs',
    hybrid: '/modeling/runs',
  },
  copilotStatus: {
    mock: '/copilot/status',
    router: '/mcp/health',
    direct: '/health',
    hybrid: '/health',
  },
  copilotMessages: {
    mock: '/copilot/messages',
    router: '/mcp/messages',
    direct: '/mcp/messages',
    hybrid: '/mcp/messages',
  },
} as const;
