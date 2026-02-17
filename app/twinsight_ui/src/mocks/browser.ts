import { installMockFetchFallback } from './fallback';

export async function initializeMocking() {
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
