const WebSocket = require('ws');
const dgram = require('dgram');
const os = require('os');

const WS_PORT = process.env.PORT || 3000;
const DISCOVERY_PORT = 41234;
const DISCOVERY_INTERVAL = 1500;
const ROUND_DURATION = 10000; // 10 секунд на ответ

function getLocalIPs() {
  const nets = os.networkInterfaces();
  const ips = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) ips.push(net.address);
    }
  }
  return ips;
}

const udpSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
udpSocket.bind(DISCOVERY_PORT, () => {
  udpSocket.setBroadcast(true);
  console.log(`[UDP] Broadcast слушает порт ${DISCOVERY_PORT}`);
});

setInterval(() => {
  const payload = JSON.stringify({ service: 'quiz-game', version: 1, port: WS_PORT, name: 'Quiz Host', ips: getLocalIPs() });
  const buf = Buffer.from(payload);
  udpSocket.send(buf, 0, buf.length, DISCOVERY_PORT, '255.255.255.255', () => {});
}, DISCOVERY_INTERVAL);

udpSocket.on('message', (msg, rinfo) => {
  if (msg.toString() === 'DISCOVER') {
    const payload = JSON.stringify({ service: 'quiz-game', version: 1, port: WS_PORT, name: 'Quiz Host', ips: getLocalIPs() });
    const buf = Buffer.from(payload);
    udpSocket.send(buf, 0, buf.length, rinfo.port, rinfo.address);
  }
});

const wss = new WebSocket.Server({ port: WS_PORT, host: '0.0.0.0' });
console.log(`[WS] WebSocket сервер на порту ${WS_PORT}`);

let room = { players: new Map(), answers: [], roundActive: false, timer: null };
let nextPlayerId = 1;

// Вспомогательная функция для получения списка игроков
function getPlayersList() {
  return [...room.players.values()]
    .sort((a, b) => b.score - a.score)
    .map(p => ({ id: p.id, name: p.name, score: p.score }));
}

function broadcastToAll(obj) {
  const data = JSON.stringify(obj);
  wss.clients.forEach(c => c.readyState === WebSocket.OPEN && c.send(data));
}

function endRound() {
  if (!room.roundActive) return;
  room.roundActive = false;
  
  const sorted = room.answers.slice().sort((a, b) => a.time - b.time);
  
  sorted.forEach((ans, index) => {
    const player = [...room.players.values()].find(p => p.id === ans.playerId);
    if (player) {
      if (index === 0) player.score += 10;
      else if (index === 1) player.score += 5;
      else if (index === 2) player.score += 3;
    }
  });

  const winner = sorted[0] || null;
  const leaderboard = getPlayersList();
  
  broadcastToAll({
    type: 'roundResult',
    winner: winner ? { playerId: winner.playerId, name: winner.name } : null,
    leaderboard: leaderboard
  });
  
  // Дополнительно рассылаем обновленный список игроков всем
  broadcastToAll({ type: 'players', players: leaderboard });
  
  if (room.timer) clearTimeout(room.timer);
  console.log(`[WS] Раунд завершен. Победитель: ${winner ? winner.name : 'нет'}`);
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'join') {
      const id = nextPlayerId++;
      room.players.set(ws, { id, name: msg.name || `Игрок ${id}`, score: 0 });
      ws.send(JSON.stringify({ type: 'joined', playerId: id }));
      
      // Отправляем обновленный список ВСЕМ (и ведущему, и игрокам)
      broadcastToAll({ type: 'players', players: getPlayersList() });
    }

    if (msg.type === 'registerHost') { 
      ws.isHost = true; 
      ws.send(JSON.stringify({ type: 'hostRegistered' })); 
      ws.send(JSON.stringify({ type: 'players', players: getPlayersList() }));
    }

    if (msg.type === 'startRound') {
      room.answers = []; 
      room.roundActive = true;
      
      broadcastToAll({ 
        type: 'roundStarted', 
        question: msg.question, 
        duration: ROUND_DURATION 
      });
      
      room.timer = setTimeout(endRound, ROUND_DURATION);
    }

    if (msg.type === 'buzz') {
      if (!room.roundActive) return;
      const p = room.players.get(ws);
      if (!p || room.answers.some(a => a.playerId === p.id)) return;
      
      room.answers.push({ playerId: p.id, name: p.name, time: Date.now() });
      ws.send(JSON.stringify({ type: 'buzzAccepted' }));
      
      broadcastToAll({ type: 'buzzReceived', name: p.name });
    }

    if (msg.type === 'stopRound') {
      endRound();
    }
  });

  ws.on('close', () => {
    const p = room.players.get(ws);
    if (p) {
      room.players.delete(ws);
      broadcastToAll({ type: 'players', players: getPlayersList() });
    }
  });
});

console.log('\n=== Quiz Server запущен ===');
getLocalIPs().forEach(ip => console.log(`  ws://${ip}:${WS_PORT}`));
console.log(`UDP: порт ${DISCOVERY_PORT}`);
console.log('==========================\n');