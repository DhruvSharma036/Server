// Relay: rooms hold at most 2 people. Plain HTTP + WebSocket on one port, so it runs
// behind any host's TLS (Render, Fly, Railway, a reverse proxy).
// The server also remembers the room's "current page", so a partner who joins late
// or reconnects always learns what everyone is watching.
const http = require('http');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('Duet relay is up'); });
const wss = new WebSocketServer({ server, maxPayload: 20000 });
const rooms = new Map();   // code -> Set of sockets, with set.page = current shared page
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const presence = set => set.forEach(c => send(c, { type: 'presence', count: set.size, names: [...set].map(x => x.name) }));

wss.on('connection', ws => {
  ws.alive = true; ws.on('pong', () => { ws.alive = true; });
  let budget = 40; const refill = setInterval(() => { budget = 40; }, 1000);   // basic flood guard
  ws.on('message', raw => {
    if (--budget < 0) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'ping') return;
    if (m.type === 'join') {
      const code = String(m.room || '').toUpperCase().slice(0, 20);
      const set = rooms.get(code) || new Set();
      if (set.size >= 2 && !set.has(ws)) return send(ws, { type: 'error', text: 'That room already has two people.' });
      set.forEach(c => c !== ws && send(c, { type: 'peer-joined' }));
      set.add(ws); rooms.set(code, set); ws.room = code;
      ws.name = String(m.name || 'Partner').slice(0, 20); ws.cid = String(m.cid || '').slice(0, 24);
      presence(set);
      send(ws, { type: 'page-state', page: set.page || null });
      return;
    }
    const set = ws.room && rooms.get(ws.room); if (!set) return;
    if (m.type === 'page') {
      if (typeof m.url !== 'string' || m.url.length > 2000 || !/^https?:\/\//i.test(m.url)) return;
      set.page = { url: m.url, key: String(m.key || '').slice(0, 300), ts: Date.now(), by: ws.name, cid: ws.cid };
      set.forEach(c => send(c, { type: 'page', ...set.page }));   // everyone, including the sender, gets the stamped version
      return;
    }
    set.forEach(c => c !== ws && send(c, { ...m, from: ws.name }));
  });
  ws.on('close', () => {
    clearInterval(refill);
    const set = rooms.get(ws.room); if (!set) return;
    set.delete(ws);
    if (!set.size) rooms.delete(ws.room); else presence(set);
  });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 30000);
server.listen(PORT, () => console.log('Duet relay on port', PORT));
