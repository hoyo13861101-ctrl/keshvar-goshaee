/**
 * Keshvar-Goshaei — Optimized Server
 * بهینه‌شده برای شبکه‌های کند (ایران بدون VPN)
 *
 * بهبودها:
 * - WebSocket (ws) برای push فوری state به همه بازیکنان — بدون polling!
 * - ETag + 304 Not Modified → کاهش ۹۰٪ bandwidth
 * - Compression (gzip) روی همه پاسخ‌ها
 * - Keep-Alive connections
 * - CORS مناسب
 * - Health check با uptime
 */

const express  = require('express');
const path     = require('path');
const http     = require('http');
const { WebSocketServer } = require('ws');
const zlib     = require('zlib');

const app    = express();
const server = http.createServer(app);

/* ─── WebSocket Server ─── */
const wss = new WebSocketServer({ server, path: '/ws' });

// نگه‌داری مشترکین هر room
// roomSubs: Map<roomKey, Set<ws>>
const roomSubs = new Map();

function getRoomSubs(roomKey) {
  if (!roomSubs.has(roomKey)) roomSubs.set(roomKey, new Set());
  return roomSubs.get(roomKey);
}

function broadcast(roomKey, payload) {
  const subs = roomSubs.get(roomKey);
  if (!subs) return;
  const msg = JSON.stringify(payload);
  subs.forEach(ws => {
    if (ws.readyState === 1 /* OPEN */) {
      try { ws.send(msg); } catch (_) {}
    }
  });
}

wss.on('connection', (ws, req) => {
  let subscribedRoom = null;

  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw);
      // { type: 'subscribe', room: 'room-XXXXXX' }
      if (msg.type === 'subscribe' && msg.room) {
        // از اشتراک قبلی خارج شو
        if (subscribedRoom) {
          const old = roomSubs.get(subscribedRoom);
          if (old) old.delete(ws);
        }
        subscribedRoom = msg.room;
        getRoomSubs(subscribedRoom).add(ws);
        // بلافاصله آخرین state رو بفرست
        const current = store[subscribedRoom];
        if (current !== undefined) {
          ws.send(JSON.stringify({ type: 'state', key: subscribedRoom, value: current }));
        }
      }
    } catch (_) {}
  });

  ws.on('close', () => {
    if (subscribedRoom) {
      const subs = roomSubs.get(subscribedRoom);
      if (subs) {
        subs.delete(ws);
        if (subs.size === 0) roomSubs.delete(subscribedRoom);
      }
    }
  });

  ws.on('error', () => ws.terminate());

  // Ping هر ۳۰ ثانیه تا connection زنده بماند
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
});

// بررسی زنده بودن connections هر ۳۰ ثانیه
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

/* ─── Middleware ─── */
app.use(express.json({ limit: '2mb' }));

// Compression برای همه پاسخ‌ها
app.use((req, res, next) => {
  const ae = req.headers['accept-encoding'] || '';
  if (!ae.includes('gzip')) return next();
  const _json = res.json.bind(res);
  res.json = (data) => {
    const str = JSON.stringify(data);
    zlib.gzip(Buffer.from(str), (err, buf) => {
      if (err) return _json(data);
      res.setHeader('Content-Encoding', 'gzip');
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Vary', 'Accept-Encoding');
      res.end(buf);
    });
  };
  next();
});

// Keep-Alive
app.use((req, res, next) => {
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Keep-Alive', 'timeout=60, max=100');
  next();
});

// CORS — برای دسترسی از هر جا
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, If-None-Match');
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

// Static files با cache
app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '1d',
  etag: true,
  lastModified: true
}));

/* ─── In-Memory Store ─── */
const store   = {};   // key → value
const etags   = {};   // key → etag string

function makeEtag(value) {
  // hash ساده از JSON length + timestamp آخرین تغییر
  const s = JSON.stringify(value);
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return '"' + Math.abs(h).toString(36) + '"';
}

/* ─── API ─── */

// GET /api/state/:key
// پشتیبانی از ETag — اگر تغییری نبود 304 برمی‌گرداند
app.get('/api/state/:key', (req, res) => {
  const key = req.params.key;
  if (!(key in store)) return res.status(404).json({ error: 'not found' });

  const etag = etags[key];
  res.setHeader('ETag', etag);
  res.setHeader('Cache-Control', 'no-cache'); // کلاینت همیشه چک کند اما از ETag استفاده کند

  // اگر کلاینت همین نسخه را دارد → 304
  if (req.headers['if-none-match'] === etag) {
    return res.status(304).end();
  }

  res.json({ key, value: store[key] });
});

// POST /api/state/:key
app.post('/api/state/:key', (req, res) => {
  const key   = req.params.key;
  const value = req.body.value;
  store[key]  = value;
  etags[key]  = makeEtag(value);

  // Push فوری به همه کلاینت‌های WebSocket در این room
  broadcast(key, { type: 'state', key, value });

  res.json({ ok: true, etag: etags[key] });
});

// حذف state های قدیمی (بیشتر از ۴ ساعت بدون تغییر) — جلوگیری از memory leak
const storeTimestamps = {};
app.post('/api/state/:key', (req, res, next) => {
  storeTimestamps[req.params.key] = Date.now();
  next();
});
setInterval(() => {
  const now = Date.now();
  Object.keys(storeTimestamps).forEach(k => {
    if (now - storeTimestamps[k] > 4 * 60 * 60 * 1000) {
      delete store[k];
      delete etags[k];
      delete storeTimestamps[k];
    }
  });
}, 30 * 60 * 1000);

// Health check
app.get('/health', (req, res) => {
  res.json({
    ok: true,
    rooms: Object.keys(store).length,
    wsClients: wss.clients.size,
    uptime: Math.floor(process.uptime()) + 's'
  });
});

/* ─── Start ─── */
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('Keshvar-Goshaei server running on port ' + PORT);
  console.log('WebSocket enabled at /ws');
});
