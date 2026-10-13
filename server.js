/* Sandbox Fauna multiplayer relay.
   - Rooms: the first player into a room code is the host; everyone else is a guest.
   - The server does not simulate anything. It just forwards messages between the players in a room,
     stamps them with the sender's id, and keeps the room tidy (host hand-over, rate limits, size limits).
   - It also serves the game itself at "/" if sandbox_fauna.html sits next to this file, so friends can
     just open your Render link and play, with no file to install. */
const http = require("http");
const fs = require("fs");
const zlib = require("zlib");
const path = require("path");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || "0.0.0.0";
const GAME = process.env.GAME_FILE || path.join(__dirname, "sandbox_fauna.html");
const MAX_ROOM = Number(process.env.MAX_ROOM || 8);
const MAX_BYTES = 1.5 * 1024 * 1024;          // one message
const RATE_PER_SEC = 160, RATE_BURST = 400;   // messages

const rooms = new Map();                      // code -> { code, members: Map(id -> ws), host, created, emptyAt }
let nextId = 1;

const clean = (s, n, fb) => String(s == null ? fb : s).replace(/[\u0000-\u001f<>]/g, "").trim().slice(0, n) || fb;
const cleanRoom = s => String(s || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 12);
const send = (ws, obj) => { if (ws.readyState === WebSocket.OPEN) ws.send(typeof obj === "string" ? obj : JSON.stringify(obj)); };

function roster(room) {
  return [...room.members.entries()].map(([id, w]) => ({ id, name: w._name, color: w._color, host: id === room.host }));
}
function broadcast(room, obj, exceptId) {
  const text = typeof obj === "string" ? obj : JSON.stringify(obj);
  for (const [id, w] of room.members) if (id !== exceptId) send(w, text);
}
function leave(ws) {
  const room = ws._room; if (!room) return;
  room.members.delete(ws._id);
  ws._room = null;
  if (!room.members.size) { room.emptyAt = Date.now(); return; }
  if (room.host === ws._id) room.host = [...room.members.keys()][0];     // oldest remaining player takes over
  broadcast(room, { t: "peer-leave", id: ws._id, host: room.host });
}

/* ── static: the game file, gzipped once ── */
let gameGz = null, gameStamp = 0;
function loadGame() {
  try {
    const st = fs.statSync(GAME);
    if (gameGz && st.mtimeMs === gameStamp) return gameGz;
    gameGz = zlib.gzipSync(fs.readFileSync(GAME), { level: 6 }); gameStamp = st.mtimeMs;
    return gameGz;
  } catch (_) { return null; }
}

const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
  if (url === "/health") { res.writeHead(200, { "content-type": "text/plain" }); res.end("ok " + rooms.size + " rooms"); return; }
  if (url === "/" || url === "/index.html" || url === "/sandbox_fauna.html") {
    const gz = loadGame();
    if (!gz) {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end("<h3>Sandbox Fauna relay is running.</h3><p>Put <code>sandbox_fauna.html</code> next to server.js to serve the game from here, " +
        "or open your own copy of the game and type this address into its Online panel: <code>wss://" + (req.headers.host || "this-server") + "/ws</code></p>");
      return;
    }
    const gzOk = /\bgzip\b/.test(req.headers["accept-encoding"] || "");
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-cache", ...(gzOk ? { "content-encoding": "gzip" } : {}) });
    res.end(gzOk ? gz : zlib.gunzipSync(gz));
    return;
  }
  res.writeHead(404); res.end("Not found");
});

const wss = new WebSocket.Server({ server, path: "/ws", maxPayload: MAX_BYTES, perMessageDeflate: { threshold: 1024 } });

wss.on("connection", ws => {
  ws._id = String(nextId++); ws._room = null; ws._name = "Player " + ws._id; ws._color = 0;
  ws._alive = true; ws._tokens = RATE_BURST; ws._tAt = Date.now();
  ws.on("pong", () => { ws._alive = true; });

  ws.on("message", (raw, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    ws._tokens = Math.min(RATE_BURST, ws._tokens + (now - ws._tAt) / 1000 * RATE_PER_SEC); ws._tAt = now;
    if (--ws._tokens < 0) return;                                        // flooding: drop silently

    let m; try { m = JSON.parse(raw.toString()); } catch (_) { return; }
    if (!m || typeof m.t !== "string") return;

    if (m.t === "ping") { send(ws, { t: "pong", c: m.c }); return; }

    if (m.t === "join") {
      if (ws._room) leave(ws);
      const code = cleanRoom(m.room);
      if (code.length < 3) { send(ws, { t: "error", msg: "Room code needs 3-12 letters or numbers." }); return; }
      ws._name = clean(m.name, 24, ws._name); ws._color = (Number(m.color) | 0) & 0xffffff;
      let room = rooms.get(code);
      if (room && !room.members.size && Date.now() - room.emptyAt > 120000) { rooms.delete(code); room = null; }
      if (m.mustExist && !room) { send(ws, { t: "error", msg: "No room called " + code + " yet. Use Create to start it." }); return; }
      if (m.mustCreate && room && room.members.size) { send(ws, { t: "error", msg: "Room " + code + " already exists. Use Join, or pick another code." }); return; }
      if (!room) { room = { code, members: new Map(), host: null, created: Date.now(), emptyAt: 0 }; rooms.set(code, room); }
      if (room.members.size >= MAX_ROOM) { send(ws, { t: "error", msg: "That room is full (" + MAX_ROOM + " players)." }); return; }
      ws._room = room; room.members.set(ws._id, ws);
      if (!room.host || !room.members.has(room.host)) room.host = ws._id;
      send(ws, { t: "joined", id: ws._id, room: code, host: room.host, players: roster(room) });
      broadcast(room, { t: "peer-join", id: ws._id, name: ws._name, color: ws._color, host: room.host }, ws._id);
      return;
    }

    const room = ws._room; if (!room) return;
    if (m.t === "hello") {                                               // rename / recolour
      ws._name = clean(m.name, 24, ws._name); ws._color = (Number(m.color) | 0) & 0xffffff;
      broadcast(room, { t: "peer-info", id: ws._id, name: ws._name, color: ws._color }, ws._id);
      return;
    }
    if (m.t === "joined" || m.t === "peer-join" || m.t === "peer-leave" || m.t === "error" || m.t === "pong") return;   // server-only types

    m.from = ws._id;
    if (m.to) {                                                          // addressed to one player
      const target = room.members.get(String(m.to));
      if (target) send(target, m);
      return;
    }
    broadcast(room, m, ws._id);
  });

  ws.on("close", () => leave(ws));
  ws.on("error", () => { try { ws.terminate(); } catch (_) {} });
});

const beat = setInterval(() => {
  for (const ws of wss.clients) { if (!ws._alive) { ws.terminate(); continue; } ws._alive = false; try { ws.ping(); } catch (_) {} }
  for (const [code, r] of rooms) if (!r.members.size && Date.now() - r.emptyAt > 600000) rooms.delete(code);
}, 25000);
server.on("close", () => clearInterval(beat));

server.listen(PORT, HOST, () => console.log("Sandbox Fauna relay on :" + PORT + (fs.existsSync(GAME) ? "  (serving the game at /)" : "  (no game file next to server.js)")));
