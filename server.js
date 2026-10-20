'use strict';

const { WebSocketServer, WebSocket } = require('ws');

const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || 8080);

const SPAWN_POINTS = 6;
const parsedMaxPlayers = Number.parseInt(
    process.env.MAX_PLAYERS || String(SPAWN_POINTS),
    10
);
const MAX_PLAYERS = Math.max(
    1,
    Math.min(SPAWN_POINTS, parsedMaxPlayers || SPAWN_POINTS)
);

const MAX_MESSAGE_BYTES = 16 * 1024;

// Network limits
const MIN_STATE_INTERVAL_MS = 50;
const MIN_SHOT_INTERVAL_MS = 250;
const MIN_HIT_INTERVAL_MS = 20;
const MIN_DEATH_INTERVAL_MS = 500;
const HEARTBEAT_INTERVAL_MS = 30000;

// Temporary prototype limits. Tune to actual game units and weapon settings.
const MAX_COORDINATE = 100000;
const MAX_ANGLE = 1000000;
const MAX_REPORTED_DAMAGE = 100;
const MAX_SPEED_UNITS_PER_SECOND = 1200;
const MOVEMENT_ALLOWANCE = 100;

// Must be compatible with the existing client's respawn delay.
const RESPAWN_COOLDOWN_MS = 1200;

const wss = new WebSocketServer({
    host: HOST,
    port: PORT,
    maxPayload: MAX_MESSAGE_BYTES,
});

const players = new Map();
let nextPlayerNumber = 1;
let shuttingDown = false;

function send(ws, message) {
    if (ws.readyState !== WebSocket.OPEN) return;

    try {
        ws.send(JSON.stringify(message));
    } catch (error) {
        console.error(
            `[SEND ERROR] ${ws.playerId || 'unassigned'}: ${error.message}`
        );
    }
}

function broadcast(message, exceptSocket = null) {
    const payload = JSON.stringify(message);

    for (const client of wss.clients) {
        if (
            client === exceptSocket ||
            client.readyState !== WebSocket.OPEN
        ) {
            continue;
        }

        try {
            client.send(payload);
        } catch (error) {
            console.error(
                `[BROADCAST ERROR] ${client.playerId || 'unassigned'}: ${error.message}`
            );
        }
    }
}

function finiteNumber(value, min = -MAX_COORDINATE, max = MAX_COORDINATE) {
    return typeof value === 'number'
        && Number.isFinite(value)
        && value >= min
        && value <= max;
}

function validAngle(value) {
    return finiteNumber(value, -MAX_ANGLE, MAX_ANGLE);
}

function getFreeSpawnIndex() {
    const used = new Set(
        Array.from(players.values(), player => player.spawn_index)
    );

    for (let index = 0; index < SPAWN_POINTS; index++) {
        if (!used.has(index)) return index;
    }

    return -1;
}

function isActive(player) {
    return Boolean(player)
        && player.lifeState === 'alive'
        && player.x !== null
        && player.y !== null;
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
        .map(player => ({
            id: player.id,
            kills: player.kills,
            deaths: player.deaths,
        }))
        .sort((a, b) => {
            if (b.kills !== a.kills) {
                return b.kills - a.kills;
            }

            if (a.deaths !== b.deaths) {
                return a.deaths - b.deaths;
            }

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

    broadcast({
        type: 'player_left',
        player_id: id,
    });

    broadcastScoreboard();
}

function rateLimitPassed(player, key, intervalMs, now) {
    const previous = player.rateLimits[key] || 0;

    if (now - previous < intervalMs) return false;

    player.rateLimits[key] = now;
    return true;
}

function parseMessage(raw, ws) {
    let message;

    try {
        message = JSON.parse(raw.toString('utf8'));
    } catch {
        send(ws, {
            type: 'error',
            message: 'Invalid JSON',
        });

        return null;
    }

    if (
        !message ||
        typeof message !== 'object' ||
        Array.isArray(message) ||
        typeof message.type !== 'string'
    ) {
        send(ws, {
            type: 'error',
            message: 'Invalid message structure',
        });

        return null;
    }

    return message;
}

function setInactive(player, reason = 'inactive') {
    if (!player || player.lifeState === 'inactive') return false;

    player.lifeState = 'inactive';
    player.x = null;
    player.y = null;
    player.body_angle = 0;
    player.turret_angle = 0;
    player.lastInactiveAt = Date.now();

    broadcast({
        type: 'player_inactive',
        player_id: player.id,
    });

    console.log(`[INACTIVE] ${player.id} (${reason})`);
    broadcastScoreboard();

    return true;
}

wss.on('connection', (ws, request) => {
    if (shuttingDown) {
        ws.close(1001, 'Server shutting down');
        return;
    }

    if (players.size >= MAX_PLAYERS) {
        send(ws, {
            type: 'error',
            message: 'Server is full',
        });

        ws.close(1008, 'Server is full');
        return;
    }

    const spawnIndex = getFreeSpawnIndex();

    if (spawnIndex < 0) {
        send(ws, {
            type: 'error',
            message: 'No free spawn point',
        });

        ws.close(1008, 'No free spawn point');
        return;
    }

    const existingPlayers = Array.from(
        players.values(),
        publicPlayer
    );

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
        lastInactiveAt: 0,

        kills: 0,
        deaths: 0,

        // alive | dead | inactive
        lifeState: 'alive',

        rateLimits: Object.create(null),
    };

    ws.playerId = id;
    ws.isAlive = true;

    players.set(id, player);

    console.log(
        `[JOIN] ${id} from ${request.socket.remoteAddress}; ` +
        `spawn ${spawnIndex}; players online: ${players.size}`
    );

    send(ws, {
        type: 'welcome',
        player_id: id,
        spawn_index: spawnIndex,
        players: existingPlayers,
    });

    broadcast({
        type: 'player_joined',
        player: publicPlayer(player),
    }, ws);

    broadcastScoreboard();

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', raw => {
        const message = parseMessage(raw, ws);

        if (!message) return;

        const current = players.get(ws.playerId);

        if (!current) return;

        const now = Date.now();

        switch (message.type) {
            // -----------------------------------------------
            // Player movement and rotation
            // -----------------------------------------------
            case 'state': {
                if (!rateLimitPassed(
                    current,
                    'state',
                    MIN_STATE_INTERVAL_MS,
                    now
                )) {
                    return;
                }

                if (
                    !finiteNumber(message.x) ||
                    !finiteNumber(message.y) ||
                    !validAngle(message.body_angle) ||
                    !validAngle(message.turret_angle)
                ) {
                    return;
                }

                // Dead players cannot become alive merely by sending state.
                // The current client protocol has no explicit respawn message,
                // so inactive players may resume after the cooldown below.
                if (current.lifeState === 'dead') return;

                if (current.lifeState === 'inactive') {
                    const inactiveFor = now - current.lastInactiveAt;

                    if (inactiveFor < RESPAWN_COOLDOWN_MS) return;

                    // Compatibility bridge for the existing tank_spawner.
                    // A future protocol should replace this with an explicit,
                    // server-authorized respawn event.
                    current.lifeState = 'alive';
                    console.log(`[RESPAWN PROVISIONAL] ${current.id}`);
                }

                if (
                    current.x !== null &&
                    current.y !== null &&
                    current.lastStateAt > 0
                ) {
                    const elapsedSeconds = Math.max(
                        0,
                        (now - current.lastStateAt) / 1000
                    );

                    const dx = message.x - current.x;
                    const dy = message.y - current.y;
                    const distance = Math.hypot(dx, dy);

                    const allowedDistance =
                        MAX_SPEED_UNITS_PER_SECOND * elapsedSeconds
                        + MOVEMENT_ALLOWANCE;

                    if (distance > allowedDistance) {
                        send(ws, {
                            type: 'state_rejected',
                            reason: 'movement_limit',
                        });

                        return;
                    }
                }

                current.x = message.x;
                current.y = message.y;
                current.body_angle = message.body_angle;
                current.turret_angle = message.turret_angle;
                current.state_seq += 1;
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

            // -----------------------------------------------
            // Shot visual event
            // -----------------------------------------------
            case 'shot': {
                if (!isActive(current)) return;

                if (!rateLimitPassed(
                    current,
                    'shot',
                    MIN_SHOT_INTERVAL_MS,
                    now
                )) {
                    return;
                }

                if (
                    !finiteNumber(message.x) ||
                    !finiteNumber(message.y) ||
                    !validAngle(message.angle)
                ) {
                    return;
                }

                const z = finiteNumber(message.z, -10000, 10000)
                    ? message.z
                    : 0.5;

                const shotId = typeof message.shot_id === 'string'
                    ? message.shot_id.slice(0, 64)
                    : '';

                broadcast({
                    type: 'shot',
                    player_id: current.id,
                    x: message.x,
                    y: message.y,
                    z,
                    angle: message.angle,
                    shot_id: shotId,
                    server_time: now,
                }, ws);

                break;
            }

            // -----------------------------------------------
            // Client-reported hit
            // -----------------------------------------------
            case 'projectile_hit': {
                if (!isActive(current)) return;

                if (!rateLimitPassed(
                    current,
                    'hit',
                    MIN_HIT_INTERVAL_MS,
                    now
                )) {
                    return;
                }

                if (
                    typeof message.target_player_id !== 'string' ||
                    message.target_player_id.length > 64 ||
                    !finiteNumber(message.damage, 0, MAX_REPORTED_DAMAGE) ||
                    !finiteNumber(message.x) ||
                    !finiteNumber(message.y)
                ) {
                    return;
                }

                const target = players.get(message.target_player_id);

                if (
                    !isActive(target) ||
                    target.id === current.id
                ) {
                    return;
                }

                // IMPORTANT:
                // The server still cannot verify hit geometry. This event
                // is only validated and relayed, not authoritatively simulated.
                broadcast({
                    type: 'projectile_hit',
                    player_id: current.id,
                    target_player_id: target.id,
                    damage: message.damage,
                    x: message.x,
                    y: message.y,
                    server_time: now,
                });

                console.log(
                    `[HIT REPORT] ${current.id} -> ${target.id}: ${message.damage}`
                );

                break;
            }

            // -----------------------------------------------
            // Player became inactive / tank removed
            // -----------------------------------------------
            case 'player_inactive': {
                if (!rateLimitPassed(
                    current,
                    'inactive',
                    MIN_DEATH_INTERVAL_MS,
                    now
                )) {
                    return;
                }

                setInactive(current, 'client request');
                break;
            }

            // -----------------------------------------------
            // Player death
            // -----------------------------------------------
            case 'player_killed': {
                if (current.lifeState !== 'alive') return;

                if (!rateLimitPassed(
                    current,
                    'death',
                    MIN_DEATH_INTERVAL_MS,
                    now
                )) {
                    return;
                }

                current.lifeState = 'dead';
                current.x = null;
                current.y = null;
                current.body_angle = 0;
                current.turret_angle = 0;

                let killerId =
                    typeof message.killer_id === 'string' &&
                    message.killer_id.length <= 64
                        ? message.killer_id
                        : '';

                if (killerId === current.id) {
                    killerId = '';
                }

                const killer = killerId
                    ? players.get(killerId)
                    : null;

                const creditedKillerId = isActive(killer)
                    ? killer.id
                    : null;

                current.deaths += 1;

                if (creditedKillerId) {
                    killer.kills += 1;
                }

                // Keep the existing client's protocol compatible.
                broadcast({
                    type: 'player_inactive',
                    player_id: current.id,
                });

                broadcast({
                    type: 'kill_feed',
                    killer_id: creditedKillerId,
                    victim_id: current.id,
                    server_time: now,
                });

                if (creditedKillerId) {
                    console.log(
                        `[KILL] ${creditedKillerId} -> ${current.id}; K/D updated`
                    );
                } else {
                    console.log(
                        `[DEATH] ${current.id} (no credited player killer)`
                    );
                }

                broadcastScoreboard();

                break;
            }

            // -----------------------------------------------
            // Client handshake
            // -----------------------------------------------
            case 'client_ready': {
                send(ws, {
                    type: 'ready',
                    player_id: current.id,
                });

                send(ws, {
                    type: 'scoreboard',
                    players: getScoreboard(),
                    server_time: now,
                });

                break;
            }

            // -----------------------------------------------
            // Scoreboard request
            // -----------------------------------------------
            case 'request_scoreboard': {
                send(ws, {
                    type: 'scoreboard',
                    players: getScoreboard(),
                    server_time: now,
                });

                break;
            }

            default:
                // Unknown messages are ignored.
                break;
        }
    });

    ws.on('close', () => removePlayer(ws));

    ws.on('error', error => {
        console.error(
            `[WS ERROR] ${ws.playerId || 'unassigned'}: ${error.message}`
        );
    });
});

// -----------------------------------------------
// WebSocket heartbeat
// -----------------------------------------------
const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
        if (ws.isAlive === false) {
            ws.terminate();
            continue;
        }

        ws.isAlive = false;

        try {
            ws.ping();
        } catch {
            ws.terminate();
        }
    }
}, HEARTBEAT_INTERVAL_MS);

// -----------------------------------------------
// Graceful shutdown
// -----------------------------------------------
function shutdown(signal) {
    if (shuttingDown) return;

    shuttingDown = true;

    console.log(`\nShutting down server (${signal})...`);

    clearInterval(heartbeat);

    for (const ws of wss.clients) {
        try {
            ws.close(1001, 'Server shutting down');
        } catch {
            ws.terminate();
        }
    }

    const forceExit = setTimeout(() => {
        for (const ws of wss.clients) {
            ws.terminate();
        }

        process.exit(0);
    }, 5000);

    forceExit.unref();

    wss.close(() => {
        clearTimeout(forceExit);
        process.exit(0);
    });
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

wss.on('close', () => clearInterval(heartbeat));

wss.on('listening', () => {
    console.log('-----------------------------------------');
    console.log(`Tank multiplayer test server listening on ws://${HOST}:${PORT}`);
    console.log(`Max players: ${MAX_PLAYERS}; spawn markers: ${SPAWN_POINTS}`);
    console.log(
        'Prototype relay: clients report movement and hits; hit geometry is not server-verified.'
    );
    console.log('Press Ctrl+C to stop.');
    console.log('-----------------------------------------');
});

wss.on('error', error => {
    console.error(`[SERVER ERROR] ${error.message}`);
});