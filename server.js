// Relay: rooms hold at most 2 people. Plain HTTP + WebSocket on one port, so it runs
// behind any host's TLS (Render, Fly, Railway, a reverse proxy) and exposes /health.
const http = require('http');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('Duet relay is up'); });
const wss = new WebSocketServer({ server, maxPayload: 20000 });
const rooms = new Map();
const send = (ws, o) => ws.readyState === 1 && ws.send(JSON.stringify(o));
const presence = set => set.forEach(c => send(c, { type: 'presence', count: set.size, names: [...set].map(x => x.name) }));

wss.on('connection', ws => {
  ws.alive = true; ws.on('pong', () => { ws.alive = true; });
  let budget = 40, refill = setInterval(() => { budget = 40; }, 1000);   // basic flood guard
  ws.on('message', raw => {
    if (--budget < 0) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'ping') return;
    if (m.type === 'join') {
      const code = String(m.room || '').toUpperCase().slice(0, 20);
      const set = rooms.get(code) || new Set();
      if (set.size >= 2 && !set.has(ws)) return send(ws, { type: 'error', text: 'That room already has two people.' });
      set.forEach(c => c !== ws && send(c, { type: 'peer-joined' }));
      set.add(ws); rooms.set(code, set); ws.room = code; ws.name = String(m.name || 'Partner').slice(0, 20);
      presence(set);
    } else if (ws.room && rooms.has(ws.room)) {
      rooms.get(ws.room).forEach(c => c !== ws && send(c, { ...m, from: ws.name }));
    }
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
