const CACHE_NAME = "hira-os-v45";
const ASSETS = [
  "/",
  "/styles.css?v=20260501-6",
  "/app.js?v=20260501-6",
  "/static/icon.svg",
  "/manifest.webmanifest"
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(ASSETS)));
  self.skipWaiting();
});

self.addEventListener("message", (event) => {
  if (event.data?.type === "SKIP_WAITING") self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request).then((cached) => cached || caches.match("/")))
  );
});

self.addEventListener("push", (event) => {
  const payload = event.data ? event.data.json() : {};
  const title = payload.title || "H.I.R.A";
  const body = payload.body || "";
  const data = {
    ...(payload.data || {}),
    title,
    body,
  };
  const options = {
    body,
    icon: payload.icon || "/static/icon.svg",
    badge: payload.badge || "/static/icon.svg",
    tag: payload.data?.id ? `hira-${payload.data.id}` : "hira",
    data,
  };
  event.waitUntil(
    Promise.all([
      self.registration.showNotification(title, options),
      self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
        for (const client of clients) {
          client.postMessage({ type: "hira-notification", item: data });
        }
      }),
    ])
  );
});

self.addEventListener("notificationclick", (event) => {
  const data = event.notification.data || {};
  const params = new URLSearchParams();
  if (data.id) params.set("notification_id", data.id);
  if (data.kind) params.set("notification_kind", data.kind);
  if (data.source) params.set("notification_source", data.source);
  if (data.title) params.set("notification_title", data.title);
  if (data.body) params.set("notification_body", data.body);
  const targetUrl = params.toString() ? `/?${params.toString()}` : "/";
  event.notification.close();
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
      for (const client of clients) {
        client.postMessage({ type: "hira-notification", item: data });
        if ("focus" in client) return client.focus();
      }
      if (self.clients.openWindow) return self.clients.openWindow(targetUrl);
      return undefined;
    })
  );
});
