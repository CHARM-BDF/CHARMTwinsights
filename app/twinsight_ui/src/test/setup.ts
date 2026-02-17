import '@testing-library/jest-dom/vitest';
import { toHaveNoViolations } from 'jest-axe';
import type { MatchersObject } from '@vitest/expect';
import { expect, afterAll, afterEach, beforeAll } from 'vitest';
import { server } from '../mocks/server';

expect.extend(toHaveNoViolations as unknown as MatchersObject);

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  localStorage.clear();
});
afterAll(() => server.close());
