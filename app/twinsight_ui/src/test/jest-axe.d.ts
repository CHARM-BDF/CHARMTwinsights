declare module 'jest-axe' {
  export type AxeViolation = {
    id: string;
    impact: string | null;
    description: string;
    help: string;
    nodes: unknown[];
  };

  export type AxeResults = {
    violations: AxeViolation[];
    passes: unknown[];
    incomplete: unknown[];
    inapplicable: unknown[];
  };

  export function axe(
    node: Element | Document | DocumentFragment,
    options?: Record<string, unknown>,
  ): Promise<AxeResults>;

  export function toHaveNoViolations(...args: unknown[]): {
    pass: boolean;
    message: () => string;
  };
}
