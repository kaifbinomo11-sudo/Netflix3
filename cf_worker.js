/**
 * Netflix Cookie Checker — Cloudflare Worker Relay
 *
 * Relays Netflix HTTP requests through Cloudflare's global edge network.
 * The bot sends cookie-check requests here; the Worker fetches Netflix and
 * returns the result as JSON. No CONNECT tunnelling needed — all traffic
 * goes over Cloudflare's backbone.
 *
 * Deploy:
 *   npm install -g wrangler
 *   wrangler login
 *   wrangler deploy
 *
 * Optional: set a secret key so only your bot can use it
 *   wrangler secret put PROXY_KEY
 */

const ALLOWED_HOSTS = [
  "www.netflix.com",
  "netflix.com",
  "api-global.netflix.com",
  "uiboot.netflix.com",
  "shakti.api.netflix.com",
];

export default {
  async fetch(request, env) {

    /* ── Health check ──────────────────────────────────────────────────── */
    if (request.method === "GET") {
      return new Response(
        JSON.stringify({ ok: true, service: "netflix-proxy" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    /* ── Auth ──────────────────────────────────────────────────────────── */
    if (env.PROXY_KEY) {
      const key = request.headers.get("X-Proxy-Key") || "";
      if (key !== env.PROXY_KEY) {
        return new Response(
          JSON.stringify({ error: "Forbidden — invalid proxy key" }),
          { status: 403, headers: { "Content-Type": "application/json" } }
        );
      }
    }

    /* ── Parse request body ────────────────────────────────────────────── */
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({ error: "Use POST" }),
        { status: 405, headers: { "Content-Type": "application/json" } }
      );
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response(
        JSON.stringify({ error: "Invalid JSON body" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    const {
      url,
      cookies = {},
      headers: extraHeaders = {},
      method = "GET",
    } = body;

    if (!url) {
      return new Response(
        JSON.stringify({ error: "Missing 'url' field" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    /* ── Host whitelist — only Netflix domains ─────────────────────────── */
    let parsedUrl;
    try {
      parsedUrl = new URL(url);
    } catch {
      return new Response(
        JSON.stringify({ error: "Invalid URL" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    if (!ALLOWED_HOSTS.some(h => parsedUrl.hostname === h || parsedUrl.hostname.endsWith("." + h))) {
      return new Response(
        JSON.stringify({ error: "Host not allowed: " + parsedUrl.hostname }),
        { status: 403, headers: { "Content-Type": "application/json" } }
      );
    }

    /* ── Build forwarded headers ───────────────────────────────────────── */
    const fwdHeaders = new Headers({
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif," +
        "image/webp,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "Accept-Encoding": "gzip, deflate, br",
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
      "Sec-Fetch-User": "?1",
      "Upgrade-Insecure-Requests": "1",
    });

    /* Apply any extra headers sent by the bot */
    for (const [k, v] of Object.entries(extraHeaders)) {
      if (!["host", "content-length", "transfer-encoding"].includes(k.toLowerCase())) {
        fwdHeaders.set(k, v);
      }
    }

    /* Build Cookie header */
    if (cookies && typeof cookies === "object") {
      const cookieStr = Object.entries(cookies)
        .map(([k, v]) => `${encodeURIComponent(k)}=${v}`)
        .join("; ");
      if (cookieStr) fwdHeaders.set("Cookie", cookieStr);
    }

    /* ── Fetch from Netflix ────────────────────────────────────────────── */
    const t0 = Date.now();
    let netflixResp;
    try {
      netflixResp = await fetch(url, {
        method,
        headers: fwdHeaders,
        redirect: "follow",
      });
    } catch (err) {
      return new Response(
        JSON.stringify({ error: "Fetch failed: " + String(err) }),
        { status: 502, headers: { "Content-Type": "application/json" } }
      );
    }

    const latency = Date.now() - t0;
    let text = "";
    try {
      text = await netflixResp.text();
    } catch {
      text = "";
    }

    /* ── Return structured response ────────────────────────────────────── */
    return new Response(
      JSON.stringify({
        status:  netflixResp.status,
        url:     netflixResp.url,
        body:    text,
        latency: latency,
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }
    );
  },
};
