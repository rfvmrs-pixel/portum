// Portum API - navios em tempo real por área (AISStream.io), sem dependências (Node 22: fetch/WebSocket nativos).
// GET /api/navios?bbox=latS,lonO,latN,lonL  -> navios vistos na área (escuta o AIS ~20 s e guarda em memória)
// GET /api/saude
'use strict';
const http = require('http');
const CHAVE = process.env.AISSTREAM_API_KEY || '';
const PORTA = Number(process.env.PORT || 8080);
const ESCUTA_MS = 20000;          // quanto tempo escuta o AIS por pedido
const CACHE_MS = 90000;           // pedido repetido da mesma área dentro disso usa o que já tem
const VALIDADE_MS = 45 * 60000;   // navio sem sinal há mais que isso some da lista
const MAX_GRAUS = 4;              // área máxima por pedido (lado)
const MAX_CONEXOES = 3;

const navios = new Map();         // mmsi -> dados
const areas = new Map();          // chave da área -> { quando, promessa }
let conexoes = 0;

function limpar() {
  const lim = Date.now() - VALIDADE_MS;
  for (const [k, n] of navios) if (n.visto < lim) navios.delete(k);
}
setInterval(limpar, 5 * 60000).unref();

const STATUS = ['Em navegação (motor)', 'Fundeado', 'Sem comando', 'Manobra restrita', 'Restrito pelo calado', 'Atracado', 'Encalhado', 'Pescando', 'Em navegação (vela)'];

function guardar(msg) {
  const md = msg.MetaData || {}; const mmsi = String(md.MMSI || '');
  if (!mmsi) return;
  const n = navios.get(mmsi) || { mmsi };
  n.nome = (md.ShipName || n.nome || '').trim() || n.nome;
  if (typeof md.latitude === 'number') { n.lat = md.latitude; n.lon = md.longitude; }
  const m = msg.Message || {};
  const pos = m.PositionReport || m.StandardClassBPositionReport;
  if (pos) {
    n.lat = pos.Latitude; n.lon = pos.Longitude;
    n.sog = pos.Sog; n.cog = pos.Cog; n.rumo = pos.TrueHeading !== 511 ? pos.TrueHeading : null;
    if (typeof pos.NavigationalStatus === 'number') n.status = STATUS[pos.NavigationalStatus] || null;
    n.classe = m.PositionReport ? 'A' : 'B';
  }
  const st = m.ShipStaticData;
  if (st) {
    n.tipo = st.Type; n.destino = (st.Destination || '').trim() || null; n.imo = st.ImoNumber || null;
    n.indicativo = (st.CallSign || '').trim() || null;
    if (st.Dimension) n.comp = (st.Dimension.A || 0) + (st.Dimension.B || 0) || null;
    if (st.MaximumStaticDraught) n.calado = st.MaximumStaticDraught;
  }
  n.visto = Date.now();
  navios.set(mmsi, n);
}

function escutar(box) {
  return new Promise((resolve) => {
    if (!CHAVE) return resolve({ erro: 'sem chave do AIS' });
    if (conexoes >= MAX_CONEXOES) return resolve({ erro: 'ocupado' });
    conexoes++;
    let recebidas = 0, fim = false;
    const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');
    const terminar = (r) => { if (fim) return; fim = true; conexoes--; try { ws.close(); } catch (e) {} resolve(r); };
    const t = setTimeout(() => terminar({ recebidas }), ESCUTA_MS);
    ws.onopen = () => ws.send(JSON.stringify({ APIKey: CHAVE, BoundingBoxes: [[[box[0], box[1]], [box[2], box[3]]]], FilterMessageTypes: ['PositionReport', 'StandardClassBPositionReport', 'ShipStaticData'] }));
    ws.onmessage = async (ev) => {
      try { const txt = typeof ev.data === 'string' ? ev.data : Buffer.from(await ev.data.arrayBuffer()).toString(); const j = JSON.parse(txt); if (j.error) { clearTimeout(t); return terminar({ erro: String(j.error).slice(0, 120) }); } guardar(j); recebidas++; } catch (e) {}
    };
    ws.onerror = () => { clearTimeout(t); terminar({ erro: 'falha na conexão com o AIS', recebidas }); };
    ws.onclose = () => { clearTimeout(t); terminar({ recebidas }); };
  });
}

function naArea(box) {
  const [s, o, nn, l] = box;
  return [...navios.values()].filter(n => typeof n.lat === 'number' && n.lat >= s && n.lat <= nn && n.lon >= o && n.lon <= l)
    .map(({ mmsi, nome, lat, lon, sog, cog, rumo, status, tipo, destino, imo, indicativo, comp, calado, classe, visto }) => ({ mmsi, nome, lat, lon, sog, cog, rumo, status, tipo, destino, imo, indicativo, comp, calado, classe, visto }));
}

async function navioRota(q) {
  const p = String(q.get('bbox') || '').split(',').map(Number);
  if (p.length !== 4 || p.some(v => !isFinite(v))) return [400, { erro: 'bbox=latS,lonO,latN,lonL' }];
  let [s, o, n, l] = p; if (s > n) [s, n] = [n, s]; if (o > l) [o, l] = [l, o];
  if (n - s > MAX_GRAUS || l - o > MAX_GRAUS) return [400, { erro: 'área grande demais (máx. ' + MAX_GRAUS + '° de lado)' }];
  const box = [s, o, n, l].map(v => Math.round(v * 20) / 20);
  const chave = box.join(',');
  let a = areas.get(chave);
  if (!a || Date.now() - a.quando > CACHE_MS) { a = { quando: Date.now(), promessa: escutar(box) }; areas.set(chave, a); }
  const r = await a.promessa;
  return [200, { area: box, escutou_s: ESCUTA_MS / 1000, mensagens: r.recebidas || 0, aviso: r.erro || null, atualizado: new Date(a.quando).toISOString(), navios: naArea(box) }];
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  try {
    if (u.pathname === '/api/saude') return res.end(JSON.stringify({ ok: true, chave: !!CHAVE, navios: navios.size, conexoes }));
    if (u.pathname === '/api/navios') { const [st, j] = await navioRota(u.searchParams); res.statusCode = st; return res.end(JSON.stringify(j)); }
    res.statusCode = 404; res.end('{"erro":"não encontrado"}');
  } catch (e) { res.statusCode = 500; res.end(JSON.stringify({ erro: 'falha interna' })); }
}).listen(PORTA, () => console.log('portum-api na porta ' + PORTA));
