/* eslint-disable */
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', () => {
  // Placeholder worker script. The app starts MSW defensively and falls back if worker init fails.
});
