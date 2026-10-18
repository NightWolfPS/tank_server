'use strict';

const { WebSocketServer, WebSocket } = require('ws');

const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || 8080);
const SPAWN_POINTS = 6; // Keep in sync with the six spawn_point Game Objects in level_test.collection.
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS || SPAWN_POINTS);
const MAX_MESSAGE_BYTES = 16 * 1024;
const MIN_STATE_INTERVAL_MS = 20;

const wss = new WebSocketServer({
    host: HOST,
    port: PORT,
    maxPayload: MAX_MESSAGE_BYTES,
});

const players = new Map();
let nextPlayerNumber = 1;

function send(ws, message) {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify(message));
}

function broadcast(message, exceptSocket = null) {
    const payload = JSON.stringify(message);
    for (const client of wss.clients) {
        if (client === exceptSocket || client.readyState !== WebSocket.OPEN) continue;
        client.send(payload);
    }
}

function finiteNumber(value, min = -100000, max = 100000) {
    return typeof value === 'number' && Number.isFinite(value)
        && value >= min && value <= max;
}

function getFreeSpawnIndex() {
    const used = new Set(Array.from(players.values(), (player) => player.spawn_index));
    for (let index = 0; index < SPAWN_POINTS; index++) {
        if (!used.has(index)) return index;
    }
    return -1;
}

function publicPlayer(player) {
    return {
        id: player.id,
        spawn_index: player.spawn_index,
        x: player.x,
        y: player.y,
        body_angle: player.body_angle,
        turret_angle: player.turret_angle,
        state_seq: player.state_seq,
        kills: player.kills,
        deaths: player.deaths,
    };
}

function getScoreboard() {
    return Array.from(players.values())
        .map((player) => ({
            id: player.id,
            kills: player.kills,
            deaths: player.deaths,
        }))
        // Primary sort: most kills first. Tie-breakers: fewer deaths,
        // then player number to keep the order deterministic.
        .sort((a, b) => {
            if (b.kills !== a.kills) return b.kills - a.kills;
            if (a.deaths !== b.deaths) return a.deaths - b.deaths;
            const aNumber = Number(a.id.replace('player_', '')) || 0;
            const bNumber = Number(b.id.replace('player_', '')) || 0;
            return aNumber - bNumber;
        });
}

function broadcastScoreboard() {
    broadcast({
        type: 'scoreboard',
        players: getScoreboard(),
        server_time: Date.now(),
    });
}

function removePlayer(ws) {
    const id = ws.playerId;
    if (!id || !players.has(id)) return;

    players.delete(id);
    ws.playerId = null;
    console.log(`[LEFT] ${id}; players online: ${players.size}`);
    broadcast({ type: 'player_left', player_id: id });
    broadcastScoreboard();
}

wss.on('connection', (ws, request) => {
    if (players.size >= MAX_PLAYERS) {
        send(ws, { type: 'error', message: 'Server is full' });
        ws.close(1008, 'Server is full');
        return;
    }

    const spawnIndex = getFreeSpawnIndex();
    if (spawnIndex < 0) {
        send(ws, { type: 'error', message: 'No free spawn point' });
        ws.close(1008, 'No free spawn point');
        return;
    }

    const existingPlayers = Array.from(players.values(), publicPlayer);
    const id = `player_${nextPlayerNumber++}`;

    const player = {
        id,
        spawn_index: spawnIndex,
        x: null,
        y: null,
        body_angle: 0,
        turret_angle: 0,
        state_seq: 0,
        lastStateAt: 0,
        kills: 0,
        deaths: 0,
        deathReported: false,
    };

    ws.playerId = id;
    ws.isAlive = true;
    players.set(id, player);

    console.log(`[JOIN] ${id} from ${request.socket.remoteAddress}; spawn ${spawnIndex}; players online: ${players.size}`);

    send(ws, {
        type: 'welcome',
        player_id: id,
        spawn_index: spawnIndex,
        players: existingPlayers,
    });

    broadcast({ type: 'player_joined', player: publicPlayer(player) }, ws);
    broadcastScoreboard();

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (raw) => {
        const text = raw.toString('utf8');
        let message;

        try {
            message = JSON.parse(text);
        } catch (error) {
            send(ws, { type: 'error', message: 'Invalid JSON' });
            return;
        }

        if (!message || typeof message.type !== 'string') return;
        const current = players.get(ws.playerId);
        if (!current) return;

        switch (message.type) {
            case 'state': {
                const now = Date.now();
                if (now - current.lastStateAt < MIN_STATE_INTERVAL_MS) return;
                if (!finiteNumber(message.x) || !finiteNumber(message.y)) return;
                if (!finiteNumber(message.body_angle, -1000000, 1000000)) return;
                if (!finiteNumber(message.turret_angle, -1000000, 1000000)) return;

                const wasInactive = current.x === null || current.y === null;
                current.x = message.x;
                current.y = message.y;
                if (wasInactive) current.deathReported = false;
                current.body_angle = message.body_angle;
                current.turret_angle = message.turret_angle;
                current.state_seq++;
                current.lastStateAt = now;

                broadcast({
                    type: 'state',
                    player_id: current.id,
                    x: current.x,
                    y: current.y,
                    body_angle: current.body_angle,
                    turret_angle: current.turret_angle,
                    state_seq: current.state_seq,
                    server_time: now,
                }, ws);
                break;
            }

            case 'shot': {
                if (current.x === null) return;
                if (!finiteNumber(message.x) || !finiteNumber(message.y)
                    || !finiteNumber(message.angle, -1000000, 1000000)) return;

                const z = finiteNumber(message.z, -10000, 10000) ? message.z : 0.5;
                broadcast({
                    type: 'shot',
                    player_id: current.id,
                    x: message.x,
                    y: message.y,
                    z,
                    angle: message.angle,
                    shot_id: typeof message.shot_id === 'string' ? message.shot_id.slice(0, 64) : '',
                }, ws);
                break;
            }

            case 'projectile_hit': {
                if (current.x === null) return;
                if (typeof message.target_player_id !== 'string'
                    || message.target_player_id.length > 64
                    || !finiteNumber(message.damage, 0, 10000)
                    || !finiteNumber(message.x) || !finiteNumber(message.y)) return;

                const target = players.get(message.target_player_id);
                if (!target || target.id === current.id || target.x === null || target.deathReported) return;

                // Prototype only: the server validates connected/active participants
                // but does not independently simulate or verify the hit geometry.
                broadcast({
                    type: 'projectile_hit',
                    player_id: current.id,
                    target_player_id: target.id,
                    damage: message.damage,
                    x: message.x,
                    y: message.y,
                });
                console.log(`[HIT REPORT] ${current.id} -> ${target.id}: ${message.damage}`);
                break;
            }

            case 'player_inactive': {
                current.x = null;
                current.y = null;
                current.body_angle = 0;
                current.turret_angle = 0;
                broadcast({ type: 'player_inactive', player_id: current.id }, ws);
                console.log(`[INACTIVE] ${current.id}`);
                break;
            }

            case 'player_killed': {
                // A client may report its own death once per life. The victim ID
                // always comes from the authenticated WebSocket connection.
                if (current.deathReported) return;
                current.deathReported = true;

                let killerId = typeof message.killer_id === 'string'
                    && message.killer_id.length <= 64
                    ? message.killer_id
                    : '';

                // Only accept another currently connected participant as the killer.
                if (!killerId || killerId === current.id || !players.has(killerId)) {
                    killerId = '';
                }

                // The server owns the K/D values. Only the connected victim's
                // own socket can report its death, and deathReported prevents
                // duplicate reports for one life from incrementing stats twice.
                current.deaths += 1;

                let creditedKillerId = null;
                if (killerId) {
                    const killer = players.get(killerId);
                    // Only credit a currently active, living participant.
                    if (killer && killer.x !== null && !killer.deathReported) {
                        killer.kills += 1;
                        creditedKillerId = killer.id;
                    }
                }

                broadcast({
                    type: 'kill_feed',
                    killer_id: creditedKillerId,
                    victim_id: current.id,
                    server_time: Date.now(),
                });

                if (creditedKillerId) {
                    console.log(`[KILL] ${creditedKillerId} -> ${current.id}; K/D updated`);
                } else {
                    console.log(`[DEATH] ${current.id} (no credited player killer)`);
                }

                broadcastScoreboard();
                break;
            }

            case 'client_ready': {
                send(ws, { type: 'ready', player_id: current.id });
                send(ws, {
                    type: 'scoreboard',
                    players: getScoreboard(),
                    server_time: Date.now(),
                });
                break;
            }

            case 'request_scoreboard': {
                send(ws, {
                    type: 'scoreboard',
                    players: getScoreboard(),
                    server_time: Date.now(),
                });
                break;
            }

            default:
                // Ignore unknown message types for forward compatibility.
                break;
        }
    });

    ws.on('close', () => removePlayer(ws));
    ws.on('error', (error) => {
        console.error(`[WS ERROR] ${ws.playerId || 'unassigned'}: ${error.message}`);
    });
});

const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
        if (ws.isAlive === false) {
            ws.terminate();
            continue;
        }
        ws.isAlive = false;
        ws.ping();
    }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));
wss.on('listening', () => {
    console.log('-----------------------------------------');
    console.log(`Tank multiplayer test server listening on ws://${HOST}:${PORT}`);
    console.log(`Max players: ${MAX_PLAYERS}; spawn markers: ${SPAWN_POINTS}`);
    console.log('Prototype relay: clients report movement and hits; hit geometry is not server-verified.');
    console.log('Press Ctrl+C to stop.');
    console.log('-----------------------------------------');
});

process.on('SIGINT', () => {
    console.log('\nShutting down server...');
    clearInterval(heartbeat);
    for (const ws of wss.clients) ws.close(1001, 'Server shutting down');
    wss.close(() => process.exit(0));
});
