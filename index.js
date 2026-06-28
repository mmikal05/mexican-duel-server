const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {origin: "*",
    methods: ["GET", "POST"],
  },
});

const PORT = process.env.PORT || 3001;

const openLobbies = {};
const rooms = {};

app.get("/", (_, res) => {
  res.send("Duel Server Running");
});

app.get("/lobbies", (_, res) => {
  res.json(
    Object.values(openLobbies)
  );
});

function resolve(a, b) {
  if (!a.attack)
    return { damage: 0, type: "miss", attack: a.attack };

  if (a.attack === b.defense)
    return { damage: 5, type: "block", attack: a.attack };

  if (Math.random() < 0.2)
    return { damage: 20, type: "crit", attack: a.attack };

  return { damage: 10, type: "hit", attack: a.attack };
}

io.on("connection", (socket) => {
  console.log("Connected:", socket.id);

  socket.on("createLobby",({ username }) => {
    console.log("LOBBY CREATED BY:", username);
      
    const roomId = Math.random().toString(36).substring(2, 8).toUpperCase();

    openLobbies[roomId] = {
      roomId,
      hostId: socket.id,
      createdAt: Date.now(),
      players: [
        { id: socket.id, username, ready: false },
      ],
    };

    socket.join(roomId);

    socket.emit("matchCreated", { roomId });

    io.to(roomId).emit("lobby", {
      roomId,
      players: openLobbies[roomId].players
    });

    setTimeout(() => {
      if (openLobbies[roomId]) {
        io.to(roomId).emit("lobbyExpired");
        delete openLobbies[roomId];
      }
    }, 60000);
  });

  socket.on(
    "getLobbies",
    () => {
      socket.emit(
        "lobbiesList",
        Object.values(
          openLobbies
        )
      );
    }
  );

  socket.on("joinMatch",({ roomId, username }) => {
    console.log("JOIN LOBBY:", roomId);

    const lobby = openLobbies[roomId];

    if (!lobby) {
      console.log("LOBBY NOT FOUND");
      return;
    }

    if ( lobby.players.length >= 2 ) {
      console.log("LOBBY FULL");
      return;
    }

    lobby.players.push({
      id: socket.id,
      username,
      ready: false,
    });

    lobby.players.forEach(p => p.ready = false);

    rooms[roomId] = {
      id: roomId,
      players: lobby.players,
      moves: {},
      hp: { 
        [lobby.players[0].id]: 100,
        [socket.id]: 100,
      },
    };

    socket.join(roomId);

    io.to(roomId).emit("lobby", {
      roomId,
      players: lobby.players,
    });

    delete openLobbies[roomId];
  });

  socket.on("ready", (roomId) => {
    const room = rooms[roomId];
    if (!room) return;

    const player = room.players.find(p => p.id === socket.id);
    if (!player) return;

    player.ready = true;

    io.to(roomId).emit("lobby", {
      roomId,
      players: room.players,
    });

    if (room.players.every(p => p.ready)) {
      console.log("DUEL STARTING:", roomId);
      io.to(roomId).emit("duelStart");
    }
  });

  socket.on("move", ({roomId, attack, defense }) => {
    const room = rooms[roomId];
    if (!room) return;

    room.moves[socket.id] = { attack, defense };

    if (Object.keys(room.moves).length === 2) {
      const [p1, p2] = room.players.map((p) => p.id);
      
      const r1 = resolve(room.moves[p1], room.moves[p2]);
      const r2 = resolve(room.moves[p2], room.moves[p1]);

      room.hp[p1] = Math.max(room.hp[p1] - r2.damage, 0);
      room.hp[p2] = Math.max(room.hp[p2] - r1.damage, 0);

      io.to(roomId).emit("roundResult", {
        hp: room.hp,
        results: {
          [p1]: r1,
          [p2]: r2,
        },
      });

      room.moves = {};
    }
  });

  socket.on("reconnectPlayer", ({ playerId }) => {
    for (const roomId in rooms) {
      const room = rooms[roomId];

      const player = room.players.find(p => p.id === playerId);
      if (!player) continue;

      const oldId = player.id;

      player.id = socket.id;
      room.hp[socket.id] = room.hp[oldId];
      delete room.hp[oldId];

      if (room.moves[oldId]) {room.moves[socket.id] = room.moves[oldId];
        delete room.moves[oldId];
      }

      socket.join(roomId);

      room.disconnected = null;

      socket.emit("reconnected", { roomId, room });

      break;
    }
  });

  socket.on("disconnect", () => {
    console.log("DISCONNECTED:", socket.id);

    for (const roomId in openLobbies) {
      const lobby = openLobbies[roomId];
      if (lobby.hostId === socket.id) {
        delete openLobbies[roomId];
      }
    }

    for (const roomId in rooms) {
      const room = rooms[roomId];

      const player = room.players.find(p => p.id === socket.id);
      if (!player) continue;

      room.disconnected = player.id;

      io.to(roomId).emit("playerDisconnected");

      setTimeout(() => {
        if (room.disconnected === player.id) {
          io.to(roomId).emit("opponentForfeit");
          delete rooms[roomId];
        }
      }, 30000);

      break;
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on", PORT);
});