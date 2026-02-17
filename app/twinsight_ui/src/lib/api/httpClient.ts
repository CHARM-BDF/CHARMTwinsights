export class HttpError extends Error {
  status: number;
  responseBody: string;

  constructor(status: number, responseBody: string) {
    super(`Request failed with status ${status}`);
    this.status = status;
    this.responseBody = responseBody;
  }
}

export async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new HttpError(response.status, body);
  }

  if (response.status === 204) {
    return {} as T;
  }

  return (await response.json()) as T;
}
