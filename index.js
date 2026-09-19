const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

const PORT = process.env.PORT || 3001;

const BODY_PARTS = ["head", "body", "waist", "legs", "foot"];
const MAX_HP = 100;

const LOBBY_TTL = 60 * 1000; // a lobby must start its duel within this time
const RECONNECT_GRACE = 30 * 1000; // how long a dropped player has to come back
const ROUND_MS = 10 * 1000; // round length shown to players
const RESOLVE_GRACE = 700; // extra time for late packets before the server resolves
const BETWEEN_ROUNDS_MS = 2000; // pause so clients can show the round result

const NO_MOVE = { attack: null, defense: null };

/*
  One room per lobby / match. The SERVER owns the round clock.

  room = {
    id, createdAt, started,
    players: [{ token, pid, socketId, username, ready, offline, forfeitTimer }],
    hp:    { p1, p2 }          keyed by pid
    moves: { p1, p2 }          keyed by pid, latest pick wins, cleared every round
    phase: "lobby" | "round" | "between"
    round: number
  }

  - `pid` ("p1" / "p2") is the public id sent to clients.
  - `token` is a secret the client keeps in sessionStorage. It is never
    broadcast; it is how a player is recognised after a reconnect (socket ids
    change on every connection).
*/
const rooms = {};

/* ---------- helpers ---------- */

const generateRoomId = () => {
  let id;
  do {
    id = Math.random().toString(36).substring(2, 8).toUpperCase();
  } while (rooms[id]);
  return id;
};

const publicPlayers = (room) =>
  room.players.map((p) => ({ pid: p.pid, username: p.username, ready: p.ready }));

const lobbyState = (room) => ({
  roomId: room.id,
  players: publicPlayers(room),
  expiresIn: Math.max(0, room.expiresAt - Date.now()),
});

const openLobbies = () =>
  Object.values(rooms)
    .filter((r) => !r.started && r.players.length < 2)
    .map((r) => ({
      roomId: r.id,
      createdAt: r.createdAt,
      players: publicPlayers(r),
    }));

const findRoomByToken = (token) =>
  Object.values(rooms).find((r) => r.players.some((p) => p.token === token));

const cleanZone = (zone) => (BODY_PARTS.includes(zone) ? zone : null);

const closeRoom = (room) => {
  clearTimeout(room.expireTimer);
  clearTimeout(room.roundTimer);
  clearTimeout(room.betweenTimer);
  room.players.forEach((p) => clearTimeout(p.forfeitTimer));
  io.in(room.id).socketsLeave(room.id);
  delete rooms[room.id];
};

/* An unstarted lobby expires; joining gives the room a fresh window. */
const armExpiry = (room) => {
  clearTimeout(room.expireTimer);
  room.expiresAt = Date.now() + LOBBY_TTL;
  room.expireTimer = setTimeout(() => {
    if (rooms[room.id] === room && !room.started) {
      io.to(room.id).emit("lobbyExpired");
      closeRoom(room);
    }
  }, LOBBY_TTL);
};

function resolve(a, b) {
  if (!a.attack) return { damage: 0, type: "miss", attack: a.attack };

  if (a.attack === b.defense)
    return { damage: 5, type: "block", attack: a.attack };

  if (Math.random() < 0.2)
    return { damage: 20, type: "crit", attack: a.attack };

  return { damage: 10, type: "hit", attack: a.attack };
}

/* ---------- round clock ---------- */

function startRound(room, { keepMoves = false } = {}) {
  clearTimeout(room.roundTimer);
  clearTimeout(room.betweenTimer);

  if (!keepMoves) {
    room.round += 1;
    room.moves = {};
  }

  room.phase = "round";
  room.roundEndsAt = Date.now() + ROUND_MS;
  room.roundTimer = setTimeout(
    () => finishRound(room),
    ROUND_MS + RESOLVE_GRACE
  );

  io.to(room.id).emit("roundStart", { round: room.round, endsIn: ROUND_MS });
}

function finishRound(room) {
  clearTimeout(room.roundTimer);
  if (rooms[room.id] !== room || room.phase !== "round") return;

  // A player who picked nothing simply misses and has no defense.
  const m1 = room.moves.p1 || NO_MOVE;
  const m2 = room.moves.p2 || NO_MOVE;

  const r1 = resolve(m1, m2);
  const r2 = resolve(m2, m1);

  room.hp.p1 = Math.max(room.hp.p1 - r2.damage, 0);
  room.hp.p2 = Math.max(room.hp.p2 - r1.damage, 0);
  room.moves = {};
  room.phase = "between";

  io.to(room.id).emit("roundResult", {
    round: room.round,
    hp: room.hp,
    results: { p1: r1, p2: r2 },
  });

  // Match over: free the room so it can't be "reconnected" to later.
  if (room.hp.p1 <= 0 || room.hp.p2 <= 0) return closeRoom(room);

  room.betweenTimer = setTimeout(() => startRound(room), BETWEEN_ROUNDS_MS);
}

/* Pause the clock while a player is offline, resume when everyone is back. */
function suspendRoom(room) {
  clearTimeout(room.roundTimer);
  clearTimeout(room.betweenTimer);
}

function resumeRoom(room) {
  if (room.phase === "round") {
    startRound(room, { keepMoves: true }); // same round, fresh full timer
  } else if (room.phase === "between") {
    room.betweenTimer = setTimeout(() => startRound(room), BETWEEN_ROUNDS_MS);
  }
}

const isSuspended = (room) => room.players.some((p) => p.offline);

/* ---------- HTTP ---------- */

app.get("/", (_, res) => {
  res.send("Duel Server Running");
});

app.get("/lobbies", (_, res) => {
  res.json(openLobbies());
});

/* ---------- sockets ---------- */

io.on("connection", (socket) => {
  console.log("Connected:", socket.id);

  const fail = (message) => socket.emit("lobbyError", { message });

  socket.on("createLobby", ({ username, token } = {}) => {
    if (!token || !username) return fail("Missing player info");
    if (findRoomByToken(token)) return fail("You are already in a match");

    const room = {
      id: generateRoomId(),
      createdAt: Date.now(),
      started: false,
      phase: "lobby",
      round: 0,
      players: [
        { token, pid: "p1", socketId: socket.id, username, ready: false, offline: false },
      ],
      hp: {},
      moves: {},
    };
    rooms[room.id] = room;
    armExpiry(room);

    socket.join(room.id);
    socket.emit("matchCreated", { roomId: room.id, pid: "p1" });
    io.to(room.id).emit("lobby", lobbyState(room));
  });

  socket.on("getLobbies", () => {
    socket.emit("lobbiesList", openLobbies());
  });

  socket.on("joinMatch", ({ roomId, username, token } = {}) => {
    if (!token || !username) return fail("Missing player info");

    const room = rooms[String(roomId || "").trim().toUpperCase()];

    if (!room || room.started) return fail("Lobby not found");
    if (room.players.length >= 2) return fail("Lobby is full");
    if (room.players.some((p) => p.token === token))
      return fail("You are already in this lobby");
    if (findRoomByToken(token)) return fail("You are already in a match");

    const pid = room.players.some((p) => p.pid === "p1") ? "p2" : "p1";

    room.players.push({
      token, pid, socketId: socket.id, username, ready: false, offline: false,
    });
    room.players.forEach((p) => (p.ready = false));
    armExpiry(room);

    socket.join(room.id);
    socket.emit("matchJoined", { roomId: room.id, pid });
    io.to(room.id).emit("lobby", lobbyState(room));
  });

  socket.on("ready", (roomId) => {
    const room = rooms[roomId];
    if (!room || room.started) return;

    const player = room.players.find((p) => p.socketId === socket.id);
    if (!player) return;

    player.ready = true;
    io.to(room.id).emit("lobby", lobbyState(room));

    if (room.players.length === 2 && room.players.every((p) => p.ready)) {
      console.log("DUEL STARTING:", room.id);
      clearTimeout(room.expireTimer);
      room.started = true;
      room.hp = { p1: MAX_HP, p2: MAX_HP };
      room.round = 0;
      io.to(room.id).emit("duelStart", { hp: room.hp });
      startRound(room);
    }
  });

  // Players may change their pick freely until the round ends; the latest one counts.
  socket.on("move", ({ roomId, round, attack, defense } = {}) => {
    const room = rooms[roomId];
    if (!room || room.phase !== "round" || round !== room.round) return;

    const player = room.players.find((p) => p.socketId === socket.id);
    if (!player) return;

    room.moves[player.pid] = {
      attack: cleanZone(attack),
      defense: cleanZone(defense),
    };
  });

  socket.on("reconnectPlayer", ({ token } = {}) => {
    const room = token && findRoomByToken(token);
    // Nothing to restore (match finished or forfeited while the client was away).
    if (!room) return socket.emit("noActiveMatch");

    const player = room.players.find((p) => p.token === token);
    player.socketId = socket.id;
    socket.join(room.id);

    if (player.offline) {
      player.offline = false;
      clearTimeout(player.forfeitTimer);
      io.to(room.id).emit("playerReconnected");
      if (room.started && !isSuspended(room)) resumeRoom(room);
    }

    socket.emit("reconnected", {
      roomId: room.id,
      pid: player.pid,
      started: room.started,
      players: publicPlayers(room),
      expiresIn: room.started ? null : lobbyState(room).expiresIn,
      hp: room.hp,
      phase: room.phase,
      round: room.round,
      suspended: isSuspended(room),
      endsIn:
        room.phase === "round" && !isSuspended(room)
          ? Math.max(0, room.roundEndsAt - Date.now())
          : null,
      myMove: room.moves[player.pid] || null,
    });
  });

  socket.on("disconnect", () => {
    console.log("DISCONNECTED:", socket.id);

    for (const room of Object.values(rooms)) {
      const player = room.players.find((p) => p.socketId === socket.id);
      if (!player) continue;

      // Still in the lobby: just leave it.
      if (!room.started) {
        room.players = room.players.filter((p) => p !== player);
        room.players.forEach((p) => (p.ready = false));

        if (room.players.length === 0) closeRoom(room);
        else io.to(room.id).emit("lobby", lobbyState(room));

        break;
      }

      // Mid-match: pause the clock and give them a window to come back.
      player.offline = true;
      suspendRoom(room);
      io.to(room.id).emit("playerDisconnected");

      player.forfeitTimer = setTimeout(() => {
        if (rooms[room.id] === room && player.offline) {
          io.to(room.id).emit("opponentForfeit");
          closeRoom(room);
        }
      }, RECONNECT_GRACE);

      break;
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on", PORT);
});
