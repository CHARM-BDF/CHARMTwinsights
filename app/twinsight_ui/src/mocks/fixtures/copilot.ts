import type { CopilotMessage, CopilotStatus } from '../../lib/contracts/types';

export const copilotStatusFixture: CopilotStatus = {
  enabled: false,
  transport: 'disabled',
  endpoint: 'http://localhost:8006/mcp',
};

export const copilotMessagesFixture: CopilotMessage[] = [
  {
    id: 'msg-1',
    role: 'system',
    content: 'Copilot integration is currently disabled in this environment.',
    createdAt: '2026-02-17T09:05:00Z',
  },
];
