import { installMockFetchFallback } from './fallback';

export async function initializeMocking() {
  const mode = import.meta.env.VITE_SERVICE_MODE ?? 'mock';
  if (mode !== 'mock') {
    return;
  }

  if (typeof window === 'undefined') {
    return;
  }

  const { worker } = await import('./worker');
  const startupTimeoutMs = 1500;

  try {
    await Promise.race([
      worker.start({
        onUnhandledRequest: 'bypass',
        serviceWorker: {
          url: '/mockServiceWorker.js',
        },
      }),
      new Promise((_, reject) => {
        setTimeout(() => reject(new Error('MSW startup timeout')), startupTimeoutMs);
      }),
    ]);
  } catch {
    installMockFetchFallback();
  }
}
