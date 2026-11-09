// Public read-only copy of the local page. The Mac only pushes outward over one WebSocket; nothing here reaches back.
import { DurableObject } from "cloudflare:workers";

const KEEP = 30, MAX_VIEWERS = 20;
const JSON_HEADERS = { "Content-Type": "application/json", "Cache-Control": "no-store", "X-Robots-Tag": "noindex", "X-Hub": "3" };

export class StateHub extends DurableObject {
  current = null;

  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/push") {
      const [client, server] = Object.values(new WebSocketPair());
      for (const old of this.ctx.getWebSockets("push")) old.close(1000, "replaced");
      this.ctx.acceptWebSocket(server, ["push"]);
      return new Response(null, { status: 101, webSocket: client });
    }
    if (url.pathname === "/live") {             // pages listen here; a full house just keeps polling /state
      if (this.ctx.getWebSockets("view").length >= MAX_VIEWERS) return new Response("busy", { status: 503 });
      const [client, server] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(server, ["view"]);
      return new Response(null, { status: 101, webSocket: client });
    }
    if (!this.current) return new Response('{"waiting":true}', { status: 503, headers: JSON_HEADERS });
    const t = url.searchParams.get("t");
    if (t && /^\d+$/.test(t)) {
      const past = await this.ctx.storage.get("w:" + t);
      if (past) {
        // A stored window keeps its own chart and result; the clock and banners stay live.
        const { view, ptb, settled, points, cur, edge, bars } = past;
        return new Response(JSON.stringify({ ...this.current, view, ptb, settled, points, cur, edge, bars }), { headers: JSON_HEADERS });
      }
    }
    return new Response(JSON.stringify(this.current), { headers: JSON_HEADERS });
  }

  webSocketClose(ws, code) {
    try { ws.close(code === 1005 || code === 1006 ? 1000 : code); } catch (e) { /* already gone */ }
  }

  async webSocketMessage(ws, raw) {
    if (this.ctx.getTags(ws).includes("view")) return;   // pages only listen
    const msg = JSON.parse(raw);
    if (msg.kind === "cur") {
      if (msg.state) this.current = msg.state;   // the whole state comes every few seconds, the tick every second
      if (msg.tick) {
        const text = JSON.stringify(msg.tick);
        for (const v of this.ctx.getWebSockets("view")) { try { v.send(text); } catch (e) { /* gone; its close event cleans up */ } }
      }
    } else if (msg.kind === "past") {
      await this.ctx.storage.put("w:" + msg.t, msg.state);
      const old = await this.ctx.storage.list({ prefix: "w:" });
      const keys = [...old.keys()].sort();
      if (keys.length > KEEP) await this.ctx.storage.delete(keys.slice(0, keys.length - KEEP));
    }
  }
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const hub = env.HUB.get(env.HUB.idFromName("main"));
    if (url.pathname === "/state" && req.method === "GET") return hub.fetch(req);
    if (url.pathname === "/live" && req.headers.get("Upgrade") === "websocket") return hub.fetch(req);
    if (url.pathname === "/push") {
      if (!env.PUSH_TOKEN || req.headers.get("Upgrade") !== "websocket" || req.headers.get("Authorization") !== "Bearer " + env.PUSH_TOKEN) {
        return new Response("forbidden", { status: 403 });
      }
      return hub.fetch(req);
    }
    return new Response("not found", { status: 404, headers: { "X-Robots-Tag": "noindex" } });
  },
};
