const express = require('express');
const path = require('path');

const app = express();
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Simple in-memory key/value store shared by all connected players.
// Good enough for a casual game room; state resets if the server restarts/redeploys.
const store = {};

app.get('/api/state/:key', (req, res) => {
  const key = req.params.key;
  if (!(key in store)) return res.status(404).json({ error: 'not found' });
  res.json({ key, value: store[key] });
});

app.post('/api/state/:key', (req, res) => {
  const key = req.params.key;
  store[key] = req.body.value;
  res.json({ ok: true });
});

app.get('/health', (req, res) => res.send('ok'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Keshvar-Goshaei server running on port ' + PORT));
