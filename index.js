const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();

const ALLOWED_ORIGIN = process.env.CLIENT_ORIGIN || "http://localhost:5173";
app.use(cors({ origin: ALLOWED_ORIGIN }));

const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: ALLOWED_ORIGIN, methods: ["GET", "POST"] },
});

const PORT = process.env.PORT || 3001;

const BODY_PARTS = ["head", "body", "waist", "legs", "foot"];
const EMOTES = ["👋", "🔥", "😎", "😂", "👏", "💀", "GG", "🤝"];
const MAX_HP = 100;
const MAX_WAGER = 1000; // coins each
const WAGER_FEE_RATE = 0.1; // share of the pot kept as the developer fee
const EXP_LOSER = 50; // PvP: the loser (and a draw) still earns this much
const EXP_WINNER_BASE = 100; // PvP winner: dealt * 1.1 + HP left * 1.2 + this

const LOBBY_TTL = 5 * 60 * 1000; // a lobby must start its duel within this time
const RECONNECT_GRACE = 30 * 1000; // how long a dropped player has to come back
const ROUND_MS = 10 * 1000; // round length shown to players
const RESOLVE_GRACE = 700; // extra time for late packets before the server resolves
const BETWEEN_ROUNDS_MS = 2000; // pause so clients can show the round result
const EMOTE_COOLDOWN = 1000;
const OUTCOME_TTL = 24 * 60 * 60 * 1000;

// Rate-limiting: max actions per token within a window
const RATE_LIMIT_WINDOW = 10 * 1000; // 10 seconds
const RATE_LIMIT_MAX = 5;            // max lobby creates/joins per window
const rateLimits = new Map();        // token -> { count, windowStart }

const NO_MOVE = { attack: null, defense: null };

/*
  One room per lobby / match. The SERVER owns the round clock.

  room = {
    id, createdAt, started, wager,
    players: [{ token, pid, socketId, username, ready, offline, forfeitTimer }],
    hp:     { p1, p2 }          keyed by pid
    stats:  { p1, p2 }          per player: rounds, dealt, taken, crits, blocked
    moves:  { p1, p2 }          keyed by pid, latest pick wins, cleared every round
    locked: { p1, p2 }          a locked-in player can't change their pick;
                                when both lock in, the round ends early
    phase: "lobby" | "round" | "between"
    round: number
  }

  - `pid` ("p1" / "p2") is the public id sent to clients.
  - `token` is a secret the client keeps in sessionStorage. It is never
    broadcast; it is how a player is recognised after a reconnect (socket ids
    change on every connection).

  Wagers: both players stake the same amount when the duel starts. The server
  decides the winner and works out each player's payout and EXP (the winner
  gets the pot minus a 10% fee; EXP follows the design doc). Balances live in
  the players' own save data, so the clients apply the stake and the payout
  (see useOnlineDuel on the client).

  outcomes: token -> last match result, kept until the client acknowledges it,
  so a player who was offline when the match ended still learns the result
  (and can't dodge a loss by closing the tab).
*/
const rooms = {};
const outcomes = new Map();

/* ---------- rate limiter ---------- */

function checkRateLimit(token) {
  const now = Date.now();
  const entry = rateLimits.get(token);
  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW) {
    rateLimits.set(token, { count: 1, windowStart: now });
    return true;
  }
  if (entry.count >= RATE_LIMIT_MAX) return false;
  entry.count++;
  return true;
}

// Clean up stale rate-limit entries every minute.
setInterval(() => {
  const cutoff = Date.now() - RATE_LIMIT_WINDOW;
  for (const [token, entry] of rateLimits) {
    if (entry.windowStart < cutoff) rateLimits.delete(token);
  }
}, 60 * 1000).unref();

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
  wager: room.wager,
  ttl: LOBBY_TTL,
  expiresIn: Math.max(0, room.expiresAt - Date.now()),
});

const openLobbies = () =>
  Object.values(rooms)
    .filter((r) => !r.started && r.players.length < 2)
    .map((r) => ({
      roomId: r.id,
      createdAt: r.createdAt,
      wager: r.wager,
      host: r.players[0]?.username ?? "",
      players: publicPlayers(r),
    }));

const findRoomByToken = (token) =>
  Object.values(rooms).find((r) => r.players.some((p) => p.token === token));

const findPlayer = (room, socket) => room.players.find((p) => p.socketId === socket.id);

const cleanZone = (zone) => (BODY_PARTS.includes(zone) ? zone : null);

const cleanWager = (value) => {
  const n = Number(value ?? 0);
  return Number.isInteger(n) && n >= 0 && n <= MAX_WAGER ? n : null;
};

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

/* ---------- rewards ---------- */

const emptyStats = () => ({ rounds: 0, dealt: 0, taken: 0, crits: 0, blocked: 0 });

// mine / theirs: the resolved attack results of this round for one player
const addRoundStats = (stats, mine, theirs) => ({
  rounds: stats.rounds + 1,
  dealt: stats.dealt + mine.damage,
  taken: stats.taken + theirs.damage,
  crits: stats.crits + (mine.type === "crit" ? 1 : 0),
  blocked: stats.blocked + (theirs.type === "block" ? 1 : 0),
});

const feeFor = (wager) => Math.round(wager * 2 * WAGER_FEE_RATE);

// Coins paid back after both stakes were taken: the winner gets the pot minus
// the fee, a draw or a cancelled match returns the stake, the loser gets nothing.
const payoutFor = (result, wager) => {
  if (result === "win") return wager * 2 - feeFor(wager);
  if (result === "tie" || result === "void") return wager;
  return 0;
};

// EXP: winner = dealt * 1.1 + HP left * 1.2 + 100, loser = 50. A win because the
// opponent left counts for half, and quitting earns nothing.
function expFor(result, reason, stats, hpLeft) {
  if (result === "void") return { exp: 0 };
  if (result === "tie") return { exp: EXP_LOSER };
  if (result === "lose") return { exp: reason === "ko" ? EXP_LOSER : 0 };

  const damage = Math.round(stats.dealt * 1.1);
  const health = Math.round(hpLeft * 1.2);
  const full = damage + health + EXP_WINNER_BASE;
  const half = reason !== "ko";
  return {
    exp: half ? Math.round(full / 2) : full,
    detail: { damage, health, base: EXP_WINNER_BASE, half },
  };
}

/* ---------- match end ---------- */

// winner: "p1" | "p2" | null (draw). `voided` cancels the match: nobody wins,
// everybody gets their stake back.
function endMatch(room, { winner = null, reason, voided = false }) {
  for (const p of room.players) {
    const result = voided ? "void" : winner === null ? "tie" : winner === p.pid ? "win" : "lose";
    const foe = room.players.find((q) => q !== p);
    const reward = expFor(result, reason, room.stats[p.pid], room.hp[p.pid] ?? 0);

    const payload = {
      roomId: room.id,
      result,
      reason, // "ko" | "forfeit" | "timeout" | "stake"
      wager: room.wager,
      fee: result === "win" ? feeFor(room.wager) : 0,
      payout: payoutFor(result, room.wager),
      expGain: reward.exp,
      expDetail: reward.detail ?? null,
      stats: room.stats[p.pid],
      opponent: foe?.username ?? "",
      hp: room.hp,
    };
    outcomes.set(p.token, { ...payload, at: Date.now() });
    if (!p.offline) io.to(p.socketId).emit("matchOver", payload);
  }
  closeRoom(room);
}

/* ---------- round clock ---------- */

function startRound(room, { keepMoves = false } = {}) {
  clearTimeout(room.roundTimer);
  clearTimeout(room.betweenTimer);

  if (!keepMoves) {
    room.round += 1;
    room.moves = {};
    room.locked = {};
    room.roundEndsAt = Date.now() + ROUND_MS;
  }
  // keepMoves: we are resuming after a reconnect — roundEndsAt was already set
  // when the round started, so we keep it and only recalculate how much is left.

  room.phase = "round";
  const remaining = Math.max(1000, room.roundEndsAt - Date.now());
  room.roundTimer = setTimeout(() => finishRound(room), remaining + RESOLVE_GRACE);

  io.to(room.id).emit("roundStart", { round: room.round, endsIn: remaining });
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
  room.stats.p1 = addRoundStats(room.stats.p1, r1, r2);
  room.stats.p2 = addRoundStats(room.stats.p2, r2, r1);
  room.moves = {};
  room.locked = {};
  room.phase = "between";

  io.to(room.id).emit("roundResult", {
    round: room.round,
    hp: room.hp,
    results: { p1: r1, p2: r2 },
  });

  const dead1 = room.hp.p1 <= 0;
  const dead2 = room.hp.p2 <= 0;

  if (dead1 || dead2) {
    const winner = dead1 && dead2 ? null : dead2 ? "p1" : "p2";
    return endMatch(room, { winner, reason: "ko" });
  }

  room.betweenTimer = setTimeout(() => startRound(room), BETWEEN_ROUNDS_MS);
}

/* Pause the clock while a player is offline, resume when everyone is back. */
function suspendRoom(room) {
  clearTimeout(room.roundTimer);
  clearTimeout(room.betweenTimer);
  // Remember when we paused so resumeRoom can restore the correct remaining time.
  if (room.phase === "round") room.roundPausedAt = Date.now();
}

function resumeRoom(room) {
  if (room.phase === "round") {
    // Restore the time that was left when we paused rather than giving a full new timer.
    if (room.roundPausedAt && room.roundEndsAt) {
      const elapsed = room.roundPausedAt - (room.roundEndsAt - ROUND_MS);
      room.roundEndsAt = Date.now() + Math.max(1000, ROUND_MS - elapsed);
    }
    room.roundPausedAt = null;
    startRound(room, { keepMoves: true });
  } else if (room.phase === "between") {
    room.betweenTimer = setTimeout(() => startRound(room), BETWEEN_ROUNDS_MS);
  }
}

const isSuspended = (room) => room.players.some((p) => p.offline);

/* Remove a player from a room that hasn't started yet. */
function leaveLobbyRoom(room, player) {
  room.players = room.players.filter((p) => p !== player);
  room.players.forEach((p) => (p.ready = false));

  if (room.players.length === 0) closeRoom(room);
  else io.to(room.id).emit("lobby", lobbyState(room));
}

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

  socket.on("createLobby", ({ username, token, wager } = {}) => {
    if (!token || !username) return fail("Missing player info");

    if (!checkRateLimit(token)) return fail("Too many requests — slow down");

    const cleanedWager = cleanWager(wager);
    if (cleanedWager === null) return fail(`Wager must be 0-${MAX_WAGER} coins`);

    if (findRoomByToken(token)) return fail("You are already in a match");

    const room = {
      id: generateRoomId(),
      createdAt: Date.now(),
      started: false,
      wager: cleanedWager,
      phase: "lobby",
      round: 0,
      players: [
        { token, pid: "p1", socketId: socket.id, username, ready: false, offline: false },
      ],
      hp: {},
      stats: { p1: emptyStats(), p2: emptyStats() },
      moves: {},
      locked: {},
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

    if (!checkRateLimit(token)) return fail("Too many requests — slow down");

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

  socket.on("leaveLobby", (roomId) => {
    const room = rooms[roomId];
    if (!room || room.started) return;

    const player = findPlayer(room, socket);
    if (!player) return;

    socket.leave(room.id);
    leaveLobbyRoom(room, player);
    socket.emit("leftLobby");
  });

  socket.on("ready", (roomId) => {
    const room = rooms[roomId];
    if (!room || room.started) return;

    const player = findPlayer(room, socket);
    if (!player) return;

    player.ready = true;
    io.to(room.id).emit("lobby", lobbyState(room));

    if (room.players.length === 2 && room.players.every((p) => p.ready)) {
      console.log("DUEL STARTING:", room.id);
      clearTimeout(room.expireTimer);
      room.started = true;
      room.hp = { p1: MAX_HP, p2: MAX_HP };
      room.stats = { p1: emptyStats(), p2: emptyStats() };
      room.round = 0;
      io.to(room.id).emit("duelStart", {
        hp: room.hp,
        wager: room.wager,
        players: publicPlayers(room),
      });
      startRound(room);
    }
  });

  socket.on("unready", (roomId) => {
    const room = rooms[roomId];
    if (!room || room.started) return;

    const player = findPlayer(room, socket);
    if (!player) return;

    player.ready = false;
    io.to(room.id).emit("lobby", lobbyState(room));
  });

  // Quick reactions, from a fixed list only.
  socket.on("emote", ({ roomId, emote } = {}) => {
    const room = rooms[roomId];
    if (!room || !EMOTES.includes(emote)) return;

    const player = findPlayer(room, socket);
    if (!player) return;

    const now = Date.now();
    if (now - (player.lastEmoteAt || 0) < EMOTE_COOLDOWN) return;
    player.lastEmoteAt = now;

    io.to(room.id).emit("emote", { pid: player.pid, emote });
  });

  // Players may change their pick freely until they lock in or the round ends.
  socket.on("move", ({ roomId, round, attack, defense } = {}) => {
    const room = rooms[roomId];
    if (!room || room.phase !== "round" || round !== room.round) return;

    const player = findPlayer(room, socket);
    if (!player || room.locked[player.pid]) return;

    room.moves[player.pid] = {
      attack: cleanZone(attack),
      defense: cleanZone(defense),
    };
  });

  // Locking in ends the round early once both players have done it.
  socket.on("lock", ({ roomId, round } = {}) => {
    const room = rooms[roomId];
    if (!room || room.phase !== "round" || round !== room.round) return;

    const player = findPlayer(room, socket);
    if (!player || room.locked[player.pid]) return;

    room.locked[player.pid] = true;
    io.to(room.id).emit("locked", { pid: player.pid });

    if (room.locked.p1 && room.locked.p2 && !isSuspended(room)) finishRound(room);
  });

  // A player could not cover the wager when the duel started: cancel it, refund everyone.
  // Only valid before the first round has been resolved (round === 0 means no round has
  // finished yet). After that, the match is live and can't be voided this way.
  socket.on("stakeFailed", (roomId) => {
    const room = rooms[roomId];
    if (!room || !room.started || !findPlayer(room, socket)) return;
    if (room.round > 0) return; // too late — at least one round already resolved
    endMatch(room, { reason: "stake", voided: true });
  });

  socket.on("forfeit", (roomId) => {
    const room = rooms[roomId];
    if (!room) return;

    const player = findPlayer(room, socket);
    if (!player) return;

    if (!room.started) {
      socket.leave(room.id);
      leaveLobbyRoom(room, player);
      socket.emit("leftLobby");
      return;
    }

    endMatch(room, { winner: player.pid === "p1" ? "p2" : "p1", reason: "forfeit" });
  });

  // The client has processed its result: stop holding it for replay.
  socket.on("ackMatchOver", ({ token, roomId } = {}) => {
    if (token && outcomes.get(token)?.roomId === roomId) outcomes.delete(token);
  });

  socket.on("reconnectPlayer", ({ token } = {}) => {
    const room = token && findRoomByToken(token);

    if (!room) {
      // The match may have ended while this player was away.
      const outcome = token && outcomes.get(token);
      if (outcome) {
        const { at, ...payload } = outcome;
        return socket.emit("matchOver", payload);
      }
      return socket.emit("noActiveMatch");
    }

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
      wager: room.wager,
      ttl: LOBBY_TTL,
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
      locked: { p1: Boolean(room.locked.p1), p2: Boolean(room.locked.p2) },
    });
  });

  socket.on("disconnect", () => {
    console.log("DISCONNECTED:", socket.id);

    for (const room of Object.values(rooms)) {
      const player = findPlayer(room, socket);
      if (!player) continue;

      // Still in the lobby: just leave it.
      if (!room.started) {
        leaveLobbyRoom(room, player);
        break;
      }

      // Mid-match: pause the clock and give them a window to come back.
      player.offline = true;
      suspendRoom(room);
      io.to(room.id).emit("playerDisconnected");

      player.forfeitTimer = setTimeout(() => {
        if (rooms[room.id] === room && player.offline) {
          const winner = player.pid === "p1" ? "p2" : "p1";
          endMatch(room, { winner, reason: "timeout" });
        }
      }, RECONNECT_GRACE);

      break;
    }
  });
});

// Forget old unacknowledged results.
setInterval(() => {
  const cutoff = Date.now() - OUTCOME_TTL;
  for (const [token, outcome] of outcomes) {
    if (outcome.at < cutoff) outcomes.delete(token);
  }
}, 10 * 60 * 1000).unref();

server.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on", PORT);
});
