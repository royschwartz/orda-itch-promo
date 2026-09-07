/* Full-session diagnostic storage. No entry-count or message-length truncation. */
(function (scope) {
  "use strict";
  const kinds = ["entries", "lifeChanges", "aiDecisions"];
  class OrdaDiagnosticArchive {
    constructor(factory = scope.indexedDB, name = "orda-browser-diagnostics-v4") {
      this.offsets = new Map();
      this.queue = Promise.resolve();
      this.database = new Promise((resolve, reject) => {
        if (!factory) { reject(new Error("Persistent browser storage is unavailable")); return; }
        const request = factory.open(name, 1);
        request.onupgradeneeded = () => {
          const db = request.result;
          db.createObjectStore("sessions", { keyPath: "id" });
          const chunks = db.createObjectStore("chunks", { keyPath: ["sessionId", "kind", "start"] });
          chunks.createIndex("session", "sessionId");
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error || new Error("Could not open diagnostic storage"));
        request.onblocked = () => reject(new Error("Diagnostic storage is blocked by another tab"));
      });
      // Keep an unavailable archive from causing an unhandled page rejection.
      this.database.catch(() => {});
    }

    save(session) {
      const pending = this.queue.then(() => this.write(session));
      this.queue = pending.catch(() => {});
      return pending;
    }

    async write(session) {
      const db = await this.database;
      const previous = this.offsets.get(session.id) || {};
      const ends = Object.fromEntries(kinds.map(kind => [kind, (session[kind] || []).length]));
      const { entries, lifeChanges, aiDecisions, ...metadata } = session;
      await new Promise((resolve, reject) => {
        const tx = db.transaction(["sessions", "chunks"], "readwrite");
        const sessions = tx.objectStore("sessions");
        const chunks = tx.objectStore("chunks");
        sessions.put({ ...metadata, recordCounts: ends });
        for (const kind of kinds) {
          // Chunks are append-only; later flushes never rewrite an entire long game.
          for (let start = previous[kind] || 0; start < ends[kind]; start += 500) {
            chunks.put({ sessionId: session.id, kind, start,
              records: session[kind].slice(start, Math.min(start + 500, ends[kind])) });
          }
        }
        const listing = sessions.getAll();
        listing.onsuccess = () => {
          const old = listing.result.sort((a, b) => b.startedAt.localeCompare(a.startedAt)).slice(3);
          for (const expired of old) {
            sessions.delete(expired.id);
            const cursorRequest = chunks.index("session").openCursor(expired.id);
            cursorRequest.onsuccess = () => {
              const cursor = cursorRequest.result;
              if (cursor) { cursor.delete(); cursor.continue(); }
            };
          }
        };
        tx.oncomplete = resolve;
        tx.onerror = () => reject(tx.error || new Error("Could not store diagnostic log"));
        tx.onabort = () => reject(tx.error || new Error("Diagnostic storage transaction aborted"));
      });
      this.offsets.set(session.id, ends);
    }

    async loadRecent() {
      await this.queue;
      const db = await this.database;
      const sessions = await new Promise((resolve, reject) => {
        const request = db.transaction("sessions").objectStore("sessions").getAll();
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      const recent = sessions.sort((a, b) => b.startedAt.localeCompare(a.startedAt)).slice(0, 3);
      return Promise.all(recent.map(async metadata => {
        const chunks = await new Promise((resolve, reject) => {
          const request = db.transaction("chunks").objectStore("chunks").index("session").getAll(metadata.id);
          request.onsuccess = () => resolve(request.result);
          request.onerror = () => reject(request.error);
        });
        const session = { ...metadata, entries: [], lifeChanges: [], aiDecisions: [] };
        chunks.sort((a, b) => a.start - b.start);
        for (const chunk of chunks) {
          if (kinds.includes(chunk.kind)) {
            for (const record of chunk.records) session[chunk.kind].push(record);
          }
        }
        return session;
      }));
    }
  }
  scope.OrdaDiagnosticArchive = OrdaDiagnosticArchive;
})(globalThis);
