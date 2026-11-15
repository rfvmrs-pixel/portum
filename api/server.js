// Portum API - navios em tempo real (AISStream.io), sem dependências (Node 22: WebSocket nativo).
// Escuta CONTÍNUA das áreas buscadas: cada área pedida entra numa assinatura única e fica ativa
// por 48 h desde o último pedido. Navios ficam guardados (atracado/fundeado manda posição a cada ~3 min,
// nome a cada ~6 min - escuta curta perde a maioria).
// GET /api/navios?bbox=latS,lonO,latN,lonL   GET /api/saude
'use strict';
const http = require('http');
const CHAVE = process.env.AISSTREAM_API_KEY || '';
const PORTA = Number(process.env.PORT || 8080);
const ESPERA_NOVA_MS = 25000;       // área nova: espera isso na 1ª resposta
const AREA_VIVA_MS = 48 * 3600e3;   // área sai da escuta 48 h depois do último pedido
const VALIDADE_MS = 12 * 3600e3;    // navio sem sinal há mais que isso some
const MAX_GRAUS = 4;
const MAX_AREAS = 40;
const FIXAS = [[-23.10, -43.40, -22.70, -42.95]]; // Baía de Guanabara sempre escutada

const navios = new Map();
const areas = new Map();            // chave -> { box, criada, ultimoPedido }
let ws = null, conectado = false, assinadas = '', reconectar = null, msgs = 0, desde = Date.now(), ultimaMsg = 0;

const STATUS = ['Em navegação (motor)', 'Fundeado', 'Sem comando', 'Manobra restrita', 'Restrito pelo calado', 'Atracado', 'Encalhado', 'Pescando', 'Em navegação (vela)'];

function guardar(msg) {
  const md = msg.MetaData || {}; const mmsi = String(md.MMSI || '');
  if (!mmsi) return;
  const n = navios.get(mmsi) || { mmsi };
  const nome = (md.ShipName || '').trim(); if (nome) n.nome = nome;
  if (typeof md.latitude === 'number' && md.latitude !== 0) { n.lat = md.latitude; n.lon = md.longitude; }
  const m = msg.Message || {};
  const pos = m.PositionReport || m.StandardClassBPositionReport;
  if (pos) {
    n.lat = pos.Latitude; n.lon = pos.Longitude;
    // AIS: 102.3 nós = velocidade não informada; 360° = rumo não informado; 511 = proa não informada
    n.sog = pos.Sog >= 102.2 ? null : pos.Sog; n.cog = pos.Cog >= 360 ? null : pos.Cog; n.rumo = pos.TrueHeading !== 511 ? pos.TrueHeading : null;
    if (typeof pos.NavigationalStatus === 'number' && pos.NavigationalStatus < 15) n.status = STATUS[pos.NavigationalStatus] || null;
    n.classe = m.PositionReport ? 'A' : 'B';
    n.posicao = Date.now();
  }
  const st = m.ShipStaticData;
  if (st) {
    n.tipo = st.Type; n.destino = (st.Destination || '').trim() || null; n.imo = st.ImoNumber || null;
    n.indicativo = (st.CallSign || '').trim() || null;
    if (st.Dimension) { const L = (st.Dimension.A || 0) + (st.Dimension.B || 0), B = (st.Dimension.C || 0) + (st.Dimension.D || 0); if (L) n.comp = L; if (B) n.boca = B; }
    if (st.MaximumStaticDraught) n.calado = st.MaximumStaticDraught;
  }
  n.visto = Date.now();
  navios.set(mmsi, n);
}

function boxesAtivos() {
  const agora = Date.now();
  for (const [k, a] of areas) if (agora - a.ultimoPedido > AREA_VIVA_MS) areas.delete(k);
  return FIXAS.concat([...areas.values()].map(a => a.box));
}

function conectar() {
  clearTimeout(reconectar); reconectar = null;
  if (!CHAVE) return;
  try { if (ws) { ws.onclose = null; ws.close(); } } catch (e) {}
  const boxes = boxesAtivos();
  assinadas = JSON.stringify(boxes);
  ws = new WebSocket('wss://stream.aisstream.io/v0/stream');
  ws.onopen = () => {
    conectado = true;
    ws.send(JSON.stringify({ APIKey: CHAVE, BoundingBoxes: boxes.map(b => [[b[0], b[1]], [b[2], b[3]]]), FilterMessageTypes: ['PositionReport', 'StandardClassBPositionReport', 'ShipStaticData'] }));
  };
  ws.onmessage = async (ev) => {
    try { const txt = typeof ev.data === 'string' ? ev.data : Buffer.from(await ev.data.arrayBuffer()).toString(); const j = JSON.parse(txt); if (j.error) { console.log('AIS erro:', String(j.error).slice(0, 120)); return; } guardar(j); msgs++; ultimaMsg = Date.now(); } catch (e) {}
  };
  ws.onerror = () => {};
  ws.onclose = () => { conectado = false; if (!reconectar) reconectar = setTimeout(conectar, 5000); };
}

// troca a assinatura quando a lista de áreas mudar (no máximo a cada 10 s) e vigia conexão parada
let trocaPendente = null;
function pedirTroca() { if (trocaPendente) return; trocaPendente = setTimeout(() => { trocaPendente = null; if (JSON.stringify(boxesAtivos()) !== assinadas) conectar(); }, 1500); }
setInterval(() => {
  for (const [k, n] of navios) if (Date.now() - n.visto > VALIDADE_MS) navios.delete(k);
  if (JSON.stringify(boxesAtivos()) !== assinadas) conectar();
  else if (conectado && ultimaMsg && Date.now() - ultimaMsg > 5 * 60000) conectar(); // 5 min sem mensagem = reconecta
}, 60000).unref();

function dentro(n, b) { return typeof n.lat === 'number' && n.lat >= b[0] && n.lat <= b[2] && n.lon >= b[1] && n.lon <= b[3]; }
function coberto(b) { return FIXAS.concat([...areas.values()].map(a => a.box)).some(a => a[0] <= b[0] && a[1] <= b[1] && a[2] >= b[2] && a[3] >= b[3]); }

async function navioRota(q) {
  const p = String(q.get('bbox') || '').split(',').map(Number);
  if (p.length !== 4 || p.some(v => !isFinite(v))) return [400, { erro: 'bbox=latS,lonO,latN,lonL' }];
  let [s, o, n, l] = p; if (s > n) [s, n] = [n, s]; if (o > l) [o, l] = [l, o];
  if (n - s > MAX_GRAUS || l - o > MAX_GRAUS) return [400, { erro: 'área grande demais (máx. ' + MAX_GRAUS + '° de lado)' }];
  const pedido = [s, o, n, l];
  // área de escuta: arredonda para fora (0,25°) e dá folga, para pequenos movimentos do mapa caírem na mesma área
  const box = [Math.floor((s - .1) * 4) / 4, Math.floor((o - .1) * 4) / 4, Math.ceil((n + .1) * 4) / 4, Math.ceil((l + .1) * 4) / 4];
  let nova = false;
  if (!coberto(pedido)) {
    const k = box.join(',');
    if (!areas.has(k)) { if (areas.size >= MAX_AREAS) { const velha = [...areas.entries()].sort((a, b) => a[1].ultimoPedido - b[1].ultimoPedido)[0]; areas.delete(velha[0]); } areas.set(k, { box, criada: Date.now(), ultimoPedido: Date.now() }); nova = true; pedirTroca(); }
  }
  for (const a of areas.values()) if (a.box[0] <= s && a.box[1] <= o && a.box[2] >= n && a.box[3] >= l) a.ultimoPedido = Date.now();
  const area = [...areas.values()].find(a => a.box[0] <= s && a.box[1] <= o && a.box[2] >= n && a.box[3] >= l);
  const escutandoHa = area ? Date.now() - area.criada : Date.now() - desde;
  if (nova) await new Promise(r => setTimeout(r, ESPERA_NOVA_MS));
  const lista = [...navios.values()].filter(x => dentro(x, pedido)).map(({ mmsi, nome, lat, lon, sog, cog, rumo, status, tipo, destino, imo, indicativo, comp, boca, calado, classe, visto }) => ({ mmsi, nome, lat, lon, sog, cog, rumo, status, tipo, destino, imo, indicativo, comp, boca, calado, classe, visto }));
  return [200, { area_nova: nova, escutando_min: Math.round((nova ? ESPERA_NOVA_MS : escutandoHa) / 60000), conectado, navios: lista }];
}

http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  try {
    if (u.pathname === '/api/saude') return res.end(JSON.stringify({ ok: true, chave: !!CHAVE, conectado, areas: areas.size + FIXAS.length, navios: navios.size, mensagens: msgs, ultima_msg_s: ultimaMsg ? Math.round((Date.now() - ultimaMsg) / 1000) : null }));
    if (u.pathname === '/api/navios') { const [st, j] = await navioRota(u.searchParams); res.statusCode = st; return res.end(JSON.stringify(j)); }
    res.statusCode = 404; res.end('{"erro":"não encontrado"}');
  } catch (e) { res.statusCode = 500; res.end(JSON.stringify({ erro: 'falha interna' })); }
}).listen(PORTA, () => { console.log('portum-api na porta ' + PORTA); conectar(); });
