export class HttpError extends Error {
  status: number;
  responseBody: string;

  constructor(status: number, responseBody: string) {
    super(`Request failed with status ${status}`);
    this.status = status;
    this.responseBody = responseBody;
  }
}

const DEFAULT_FETCH_TIMEOUT_MS = 20_000;

function mergeAbortSignals(signalA: AbortSignal, signalB?: AbortSignal): AbortSignal {
  if (!signalB) {
    return signalA;
  }

  if (signalA.aborted || signalB.aborted) {
    const controller = new AbortController();
    controller.abort();
    return controller.signal;
  }

  const controller = new AbortController();
  const abort = () => controller.abort();
  signalA.addEventListener('abort', abort, { once: true });
  signalB.addEventListener('abort', abort, { once: true });
  return controller.signal;
}

export async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const timeoutController = new AbortController();
  const timeoutId = window.setTimeout(() => timeoutController.abort(), DEFAULT_FETCH_TIMEOUT_MS);

  let response: Response;
  try {
    response = await fetch(url, {
      ...init,
      signal: mergeAbortSignals(timeoutController.signal, init?.signal ?? undefined),
      headers: {
        'Content-Type': 'application/json',
        ...(init?.headers ?? {}),
      },
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new Error(`Request timed out after ${Math.round(DEFAULT_FETCH_TIMEOUT_MS / 1000)} seconds`);
    }
    throw error;
  } finally {
    window.clearTimeout(timeoutId);
  }

  if (!response.ok) {
    const body = await response.text();
    throw new HttpError(response.status, body);
  }

  if (response.status === 204) {
    return {} as T;
  }

  return (await response.json()) as T;
}
