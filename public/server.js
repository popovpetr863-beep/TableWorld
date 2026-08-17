
const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
app.use(express.static("public"));

const rooms = new Map();
const wss = new WebSocketServer({ server });

function code() {
  let c;
  do c = crypto.randomBytes(3).toString("hex").toUpperCase();
  while (rooms.has(c));
  return c;
}
function send(ws, type, data={}) {
  if (ws.readyState === 1) ws.send(JSON.stringify({type, ...data}));
}
function broadcast(room) {
  const payload = JSON.stringify({
    type: "state",
    room: {
      code: room.code, game: room.game, host: room.host,
      turn: room.turn, started: room.started,
      players: [...room.players.values()].map(p => ({id:p.id,name:p.name,score:p.score}))
    }
  });
  for (const p of room.players.values()) send(p.ws, "state", {
    room: {
      code: room.code, game: room.game, host: room.host,
      turn: room.turn, started: room.started,
      players: [...room.players.values()].map(p => ({id:p.id,name:p.name,score:p.score}))
    }
  });
}

function createRoom(game) {
  const r = {code:code(), game, host:null, turn:0, started:false, players:new Map()};
  rooms.set(r.code,r); return r;
}

wss.on("connection", ws => {
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === "create") {
      const room = createRoom(m.game || "Переворот");
      const id = crypto.randomUUID();
      room.host = id;
      room.players.set(id, {id,name:(m.name||"Игрок").slice(0,24),score:0,ws});
      ws.roomCode=room.code; ws.playerId=id;
      send(ws,"created",{code:room.code, id});
      broadcast(room);
      return;
    }

    if (m.type === "join") {
      const room = rooms.get(String(m.code||"").toUpperCase());
      if (!room) return send(ws,"error",{message:"Комната не найдена"});
      if (room.players.size >= 8) return send(ws,"error",{message:"В комнате максимум 8 игроков"});
      const id=crypto.randomUUID();
      room.players.set(id,{id,name:(m.name||"Игрок").slice(0,24),score:0,ws});
      ws.roomCode=room.code; ws.playerId=id;
      send(ws,"joined",{code:room.code,id});
      broadcast(room);
      return;
    }

    const room = rooms.get(ws.roomCode);
    if (!room) return;

    if (m.type === "chat") {
      const p=room.players.get(ws.playerId);
      for (const q of room.players.values()) send(q.ws,"chat",{name:p.name,text:String(m.text||"").slice(0,300)});
    }

    if (m.type === "start") {
      if (ws.playerId !== room.host) return;
      room.started=true; room.turn=0; broadcast(room);
      for (const q of room.players.values()) send(q.ws,"gameStarted",{game:room.game});
    }

    if (m.type === "nextTurn") {
      if (!room.started) return;
      const ids=[...room.players.keys()];
      if (ids.length) room.turn=(room.turn+1)%ids.length;
      broadcast(room);
    }

    if (m.type === "score") {
      const p=room.players.get(ws.playerId);
      if (!p) return;
      p.score=Math.max(0,p.score+Math.max(-20,Math.min(20,Number(m.delta)||0)));
      broadcast(room);
    }
  });

  ws.on("close",()=>{
    const room=rooms.get(ws.roomCode);
    if (!room) return;
    room.players.delete(ws.playerId);
    if (room.host===ws.playerId) {
      const next=room.players.keys().next().value;
      room.host=next||null;
    }
    if (!room.players.size) rooms.delete(room.code);
    else broadcast(room);
  });
});

const port=process.env.PORT||10000;
server.listen(port,"0.0.0.0",()=>console.log(`BoardRoom running on ${port}`));
