'use strict';

const WebSocket = require('ws');

const HOST = '0.0.0.0';
const PORT = Number(process.env.PORT) || 8080;

const MAX_PLAYERS = Math.max(
    1,
    Math.min(6, Number(process.env.MAX_PLAYERS) || 6)
);

const MAX_MESSAGE_BYTES = 16 * 1024;
const STATE_INTERVAL_MS = 50;
const SHOT_INTERVAL_MS = 250;
const HIT_INTERVAL_MS = 20;
const SHOT_TTL_MS = 10000;
const MAX_SHOTS_PER_PLAYER = 128;
const RESPAWN_DELAY_MS = 500;
const MAX_DAMAGE = 100;
const MAX_COORDINATE = 100000;
const MAX_ANGLE = 1000000;

const wss = new WebSocket.Server({
    host: HOST,
    port: PORT,
    maxPayload: MAX_MESSAGE_BYTES
});

const players = new Map();
const shots = new Map();
const shotKeys = new Map();

let nextPlayerId = 1;
let nextShotId = 1;

const spawnPoints = [
    { x: -400, y: -400 },
    { x: 0, y: -400 },
    { x: 400, y: -400 },
    { x: -400, y: 400 },
    { x: 0, y: 400 },
    { x: 400, y: 400 }
];

function log(...args) {
    console.log('[SERVER]', ...args);
}

function isNumber(value) {
    return typeof value === 'number' &&
        Number.isFinite(value);
}

function validCoordinate(value) {
    return isNumber(value) &&
        Math.abs(value) <= MAX_COORDINATE;
}

function validAngle(value) {
    return isNumber(value) &&
        Math.abs(value) <= MAX_ANGLE;
}

function isOpen(ws) {
    return ws && ws.readyState === WebSocket.OPEN;
}

function send(ws, payload) {
    if (!isOpen(ws)) return false;

    try {
        ws.send(JSON.stringify(payload));
        return true;
    } catch (error) {
        log('Send error:', error.message);
        return false;
    }
}

function broadcast(payload, exceptWs = null) {
    const encoded = JSON.stringify(payload);

    for (const client of wss.clients) {
        if (client !== exceptWs && isOpen(client)) {
            try {
                client.send(encoded);
            } catch (error) {
                log('Broadcast error:', error.message);
            }
        }
    }
}

function getPlayer(ws) {
    if (!ws.playerId) return null;
    return players.get(ws.playerId) || null;
}

function publicPlayer(player) {
    return {
        id: player.id,
        x: player.x,
        y: player.y,
        body_angle: player.body_angle,
        turret_angle: player.turret_angle,
        state_seq: player.state_seq,
        spawn_index: player.spawn_index,
        life_state: player.lifeState,
        hp: player.hp,
        kills: player.kills,
        deaths: player.deaths
    };
}

function sendError(ws, message) {
    send(ws, {
        type: 'error',
        message
    });
}

function isAlive(player) {
    return player &&
        player.lifeState === 'alive';
}

function hasPosition(player) {
    return player &&
        isNumber(player.x) &&
        isNumber(player.y);
}

function resetPlayerPosition(player) {
    player.x = null;
    player.y = null;
    player.body_angle = 0;
    player.turret_angle = 0;
    player.state_seq = 0;
}

function broadcastScoreboard() {
    const list = [...players.values()]
        .sort((a, b) =>
            b.score - a.score ||
            b.kills - a.kills ||
            a.deaths - b.deaths
        )
        .map(player => ({
            id: player.id,
            score: player.score,
            damage_dealt: player.damageDealt,
            ping: player.ping,
            kills: player.kills,
            deaths: player.deaths,
            life_state: player.lifeState
        }));

    broadcast({
        type: 'scoreboard',
        players: list
    });
}

function sendWelcome(ws, player) {
    send(ws, {
        type: 'welcome',
        player_id: player.id,
        spawn_index: player.spawn_index,
        players: [...players.values()]
            .filter(other => other.id !== player.id)
            .map(publicPlayer)
    });
}

function countPlayerShots(playerId) {
    let count = 0;

    for (const shot of shots.values()) {
        if (shot.ownerId === playerId) count++;
    }

    return count;
}

function pruneShots() {
    const now = Date.now();

    for (const [shotId, shot] of shots) {
        if (now - shot.createdAt > SHOT_TTL_MS) {
            shots.delete(shotId);
            shotKeys.delete(shot.key);
        }
    }
}

function setInactive(player, reason = 'inactive') {
    if (!player) return;

    if (player.lifeState === 'inactive') return;

    player.lifeState = 'inactive';
    player.inactiveAt = Date.now();
    resetPlayerPosition(player);

    broadcast({
        type: 'player_inactive',
        player_id: player.id,
        reason
    });

    broadcastScoreboard();
}

function registerShot(ws, message) {
    const player = getPlayer(ws);

    if (!player) return;

    // A dead or inactive player cannot create a new shot.
    if (!isAlive(player) || !hasPosition(player)) {
        send(ws, {
            type: 'shot_rejected',
            client_shot_id: message.shot_id || '',
            reason: 'player_not_alive'
        });
        return;
    }

    const now = Date.now();

    if (now - player.lastShotAt < SHOT_INTERVAL_MS) {
        send(ws, {
            type: 'shot_rejected',
            client_shot_id: message.shot_id || '',
            reason: 'rate_limited'
        });
        return;
    }

    const clientShotId = message.shot_id;

    if (typeof clientShotId !== 'string' ||
        clientShotId.length < 1 ||
        clientShotId.length > 128) {
        send(ws, {
            type: 'shot_rejected',
            client_shot_id: '',
            reason: 'invalid_shot_id'
        });
        return;
    }

    if (!validCoordinate(message.x) ||
        !validCoordinate(message.y) ||
        !validCoordinate(message.z) ||
        !validAngle(message.angle)) {
        send(ws, {
            type: 'shot_rejected',
            client_shot_id: clientShotId,
            reason: 'invalid_coordinates'
        });
        return;
    }

    const key = `${player.id}:${clientShotId}`;

    if (shotKeys.has(key)) {
        send(ws, {
            type: 'shot_rejected',
            client_shot_id: clientShotId,
            reason: 'duplicate_shot_id'
        });
        return;
    }

    pruneShots();

    if (countPlayerShots(player.id) >= MAX_SHOTS_PER_PLAYER) {
        send(ws, {
            type: 'shot_rejected',
            client_shot_id: clientShotId,
            reason: 'too_many_active_shots'
        });
        return;
    }

    player.lastShotAt = now;

    const shotId = `shot_${nextShotId++}`;

    const shot = {
        id: shotId,
        key,
        clientShotId,
        ownerId: player.id,
        x: message.x,
        y: message.y,
        z: message.z,
        angle: message.angle,
        createdAt: now,
        hitTargets: new Set()
    };

    shots.set(shotId, shot);
    shotKeys.set(key, shotId);

    // Notify the shooter that the server accepted this exact shot.
    send(ws, {
        type: 'shot_registered',
        client_shot_id: clientShotId,
        shot_id: shotId
    });

    // Remote clients only need this for visual replication.
    broadcast({
        type: 'shot',
        player_id: player.id,
        shot_id: shotId,
        x: shot.x,
        y: shot.y,
        z: shot.z,
        angle: shot.angle
    }, ws);
}

function registerHit(ws, message) {
    const shooter = getPlayer(ws);

    if (!shooter) return;

    // Intentionally do not require the shooter to be alive here.
    // A projectile fired before death may still hit its target.

    const clientShotId = message.shot_id;

    if (typeof clientShotId !== 'string' ||
        clientShotId.length < 1 ||
        clientShotId.length > 128) {
        send(ws, {
            type: 'hit_rejected',
            reason: 'invalid_shot_id'
        });
        return;
    }

    const key = `${shooter.id}:${clientShotId}`;
    const serverShotId = shotKeys.get(key);
    const shot = serverShotId ? shots.get(serverShotId) : null;

    if (!shot || shot.ownerId !== shooter.id) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'unknown_shot'
        });
        return;
    }

    if (Date.now() - shot.createdAt > SHOT_TTL_MS) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'shot_expired'
        });
        return;
    }

    const targetId = message.target_player_id;
    const target = typeof targetId === 'string'
        ? players.get(targetId)
        : null;

    if (!target || target.id === shooter.id || !isAlive(target)) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'invalid_target'
        });
        return;
    }

    if (shot.hitTargets.has(target.id)) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'duplicate_target_hit'
        });
        return;
    }

    const now = Date.now();

    if (now - shooter.lastHitAt < HIT_INTERVAL_MS) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'rate_limited'
        });
        return;
    }

    if (!isNumber(message.damage) ||
        message.damage <= 0 ||
        message.damage > MAX_DAMAGE ||
        !validCoordinate(message.x) ||
        !validCoordinate(message.y)) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'invalid_hit_data'
        });
        return;
    }

    shooter.lastHitAt = now;
    shot.hitTargets.add(target.id);

    const requestedDamage = Math.floor(message.damage);
    const actualDamage = Math.min(requestedDamage, target.hp);

    if (actualDamage <= 0) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'target_has_no_hp'
        });
        return;
    }

    target.hp = Math.max(0, target.hp - actualDamage);

    // Match statistics are authoritative on the server.
    // +10 for every accepted hit; damage records only HP actually removed.
    shooter.score += 10;
    shooter.damageDealt += actualDamage;

    const hitEvent = {
        type: 'projectile_hit',
        player_id: shooter.id,
        target_player_id: target.id,
        shot_id: shot.id,
        damage: actualDamage,
        hp: target.hp,
        x: message.x,
        y: message.y
    };

    // The victim's client applies this damage to its local tank.
    broadcast(hitEvent);

    if (target.hp <= 0 && target.lifeState === 'alive') {
        target.lifeState = 'dead';
        target.deathAt = now;
        target.deaths++;

        // Credit the owner of the registered projectile even if that
        // owner is already dead or inactive.
        shooter.kills++;
        shooter.score += 100;

        broadcast({
            type: 'kill_feed',
            killer_id: shooter.id,
            victim_id: target.id,
            shot_id: shot.id
        });

        broadcast({
            type: 'player_killed',
            killer_id: shooter.id,
            victim_id: target.id,
            shot_id: shot.id
        });

        log(`${shooter.id} killed ${target.id} with ${shot.id}`);
    }

    // Refresh points and damage immediately after every accepted hit.
    broadcastScoreboard();
}

function handleState(ws, message) {
    const player = getPlayer(ws);

    if (!player) return;

    // State updates cannot revive dead/inactive players.
    if (!isAlive(player)) {
        send(ws, {
            type: 'state_rejected',
            reason: 'player_not_alive'
        });
        return;
    }

    if (!validCoordinate(message.x) ||
        !validCoordinate(message.y) ||
        !validAngle(message.body_angle) ||
        !validAngle(message.turret_angle)) {
        send(ws, {
            type: 'state_rejected',
            reason: 'invalid_state'
        });
        return;
    }

    const now = Date.now();

    if (now - player.lastStateAt < STATE_INTERVAL_MS - 5) {
        return;
    }

    player.lastStateAt = now;
    player.x = message.x;
    player.y = message.y;
    player.body_angle = message.body_angle;
    player.turret_angle = message.turret_angle;
    player.state_seq++;

    broadcast({
        type: 'state',
        player_id: player.id,
        x: player.x,
        y: player.y,
        body_angle: player.body_angle,
        turret_angle: player.turret_angle,
        state_seq: player.state_seq
    }, ws);
}

function requestRespawn(ws) {
    const player = getPlayer(ws);

    if (!player) return;

    if (player.lifeState === 'alive') {
        send(ws, {
            type: 'respawn_rejected',
            reason: 'already_alive'
        });
        return;
    }

    const now = Date.now();
    const transitionTime = player.deathAt || player.inactiveAt || now;

    if (now - transitionTime < RESPAWN_DELAY_MS) {
        send(ws, {
            type: 'respawn_rejected',
            reason: 'cooldown'
        });
        return;
    }

    player.lifeState = 'alive';
    player.hp = 100;
    player.lastShotAt = 0;
    player.lastHitAt = 0;
    player.lastStateAt = 0;
    player.deathAt = null;
    player.inactiveAt = null;
    resetPlayerPosition(player);

    send(ws, {
        type: 'respawn_accepted',
        player_id: player.id,
        spawn_index: player.spawn_index
    });

    broadcastScoreboard();
    log(`${player.id} respawn accepted`);
}

function handleMessage(ws, message) {
    if (!message || typeof message !== 'object' ||
        typeof message.type !== 'string') {
        sendError(ws, 'Malformed message');
        return;
    }

    switch (message.type) {
        case 'pong': {
            const player = getPlayer(ws);
            if (!player || !Number.isSafeInteger(message.ping_id)) return;

            const sentAt = player.pingPending.get(message.ping_id);
            if (sentAt === undefined) {
                // Ignore unknown, duplicated, or already superseded responses.
                return;
            }

            player.pingPending.delete(message.ping_id);
            player.ping = Math.max(0, Date.now() - sentAt);
            broadcastScoreboard();
            break;
        }

        case 'client_ready': {
            const player = getPlayer(ws);
            if (!player) return;

            player.ready = true;
            send(ws, { type: 'ready' });
            broadcastScoreboard();
            break;
        }

        case 'state':
            handleState(ws, message);
            break;

        case 'shot':
            registerShot(ws, message);
            break;

        case 'projectile_hit':
            registerHit(ws, message);
            break;

        case 'player_inactive': {
            const player = getPlayer(ws);
            if (!player) return;

            // The client may send this after its death animation / cleanup.
            // Do not count a second death here.
            setInactive(player, 'tank_destroyed');
            break;
        }

        case 'player_killed':
            // Kill credit and HP are decided by registered projectile hits.
            // Do not trust client-supplied killer_id.
            log('Ignored client player_killed report from', ws.playerId);
            break;

        case 'player_respawn':
            requestRespawn(ws);
            break;

        case 'request_scoreboard':
            send(ws, {
                type: 'scoreboard',
                players: [...players.values()].map(player => ({
                    id: player.id,
                    score: player.score,
                    damage_dealt: player.damageDealt,
                    ping: player.ping,
                    kills: player.kills,
                    deaths: player.deaths,
                    life_state: player.lifeState
                }))
            });
            break;

        default:
            sendError(ws, `Unknown message type: ${message.type}`);
            break;
    }
}

function removePlayer(ws) {
    const player = getPlayer(ws);
    if (!player) return;

    players.delete(player.id);

    // Remove shots owned by a disconnected player. A disconnected client
    // can no longer report collisions, so those projectiles cannot be
    // validated by this protocol.
    for (const [shotId, shot] of shots) {
        if (shot.ownerId === player.id) {
            shots.delete(shotId);
            shotKeys.delete(shot.key);
        }
    }

    broadcast({
        type: 'player_left',
        player_id: player.id
    });

    broadcastScoreboard();
    log('Player disconnected:', player.id);
}

wss.on('connection', (ws) => {
    if (players.size >= MAX_PLAYERS) {
        send(ws, {
            type: 'error',
            message: 'Server is full'
        });
        ws.close(1008, 'Server full');
        return;
    }

    const playerNumber = nextPlayerId++;
    const id = `P${playerNumber}`;
    const spawnIndex = (playerNumber - 1) % spawnPoints.length;

    const player = {
        id,
        ws,
        x: null,
        y: null,
        body_angle: 0,
        turret_angle: 0,
        state_seq: 0,
        spawn_index: spawnIndex,
        lifeState: 'alive',
        hp: 100,
        score: 0,
        damageDealt: 0,
        ping: 0,
        pingPending: new Map(),
        nextPingId: 1,
        kills: 0,
        deaths: 0,
        ready: false,
        lastStateAt: 0,
        lastShotAt: 0,
        lastHitAt: 0,
        deathAt: null,
        inactiveAt: null
    };

    ws.playerId = id;
    players.set(id, player);

    sendWelcome(ws, player);

    broadcast({
        type: 'player_joined',
        player: publicPlayer(player)
    }, ws);

    log('Player connected:', id);

    ws.on('message', (raw, isBinary) => {
        if (isBinary) {
            sendError(ws, 'Binary messages are not supported');
            return;
        }

        let message;

        try {
            message = JSON.parse(raw.toString());
        } catch {
            sendError(ws, 'Invalid JSON');
            return;
        }

        try {
            handleMessage(ws, message);
        } catch (error) {
            log('Message handler error:', error.stack || error.message);
            sendError(ws, 'Internal server error');
        }
    });

    ws.on('close', () => removePlayer(ws));

    ws.on('error', (error) => {
        log(`Socket error for ${id}:`, error.message);
    });
});

const cleanupTimer = setInterval(pruneShots, 1000);

let nextPingId = 1;

const pingTimer = setInterval(() => {
    for (const player of players.values()) {
        if (!isOpen(player.ws)) continue;

        const pingId = nextPingId++;
        const sentAt = Date.now();

        // Keep outstanding requests correlated by ID. This matters when a
        // response is delayed and newer pings have already been sent.
        player.pingPending.set(pingId, sentAt);

        // Bound memory if a client stays connected but never responds.
        while (player.pingPending.size > 120) {
            const oldestPingId = player.pingPending.keys().next().value;
            player.pingPending.delete(oldestPingId);
        }

        const sent = send(player.ws, {
            type: 'ping',
            ping_id: pingId
        });

        if (!sent) {
            player.pingPending.delete(pingId);
        }
    }
}, 1000);

wss.on('listening', () => {
    log(`Listening on ${HOST}:${PORT}`);
    log(`Max players: ${MAX_PLAYERS}`);
});

function shutdown() {
    log('Shutting down...');
    clearInterval(cleanupTimer);
    clearInterval(pingTimer);

    for (const ws of wss.clients) {
        try {
            ws.close(1001, 'Server shutting down');
        } catch (_) {}
    }

    wss.close(() => process.exit(0));

    setTimeout(() => process.exit(0), 2000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);