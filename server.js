
'use strict';

const WebSocket = require('ws');
const http = require('http');

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
const RESPAWN_DELAY_MS = 1500;
const MAX_DAMAGE = 100;
const CHAT_MAX_LENGTH = 120;
const CHAT_INTERVAL_MS = 500;

// Central PvP combat tuning.
const BALANCE = {
    BASE_DAMAGE: 30,
    MIN_DAMAGE: 1,
    PROJECTILE_LIFETIME_SEC: 1.5,

    PENETRATION_FULL_DAMAGE_ANGLE_DEG: 5,
    PENETRATION_RICOCHET_ANGLE_DEG: 80,
    PENETRATION_MAX_CHANCE: 0.98,

    // Defold tank.go collision box half extents.
    TANK_HITBOX_HALF_WIDTH: 36.5,
    TANK_HITBOX_HALF_HEIGHT: 40.0,

    CRITICAL_CHANCE: 0.15,
    CRITICAL_MAX_BONUS_MULTIPLIER: 1.5,
    CRITICAL_MAX_ANGLE_DEG: 15,

    SLOW_CHANCE_POINT_BLANK: 0.12,
    SLOW_DURATION_SEC: 7,

    STUN_CHANCE_POINT_BLANK: 0.10,
    STUN_DURATION_SEC: 5,

    GUN_DAMAGE_CHANCE: 0.05,
    GUN_DAMAGE_RELOADS: 3,
    GUN_DAMAGE_RELOAD_PENALTY: 0.50,

    BURN_MAX_CHANCE: 0.15,
    BURN_MIN_CHANCE: 0.01,
    BURN_DURATION_SEC: 3,
    BURN_TICK_INTERVAL_SEC: 0.75,
    BURN_TICK_DAMAGE_MIN: 5,
    BURN_TICK_DAMAGE_MAX: 7
};

const MAX_COORDINATE = 100000;
const MAX_ANGLE = 1000000;

const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Tank server is running');
});

const wss = new WebSocket.Server({
    server: httpServer,
    maxPayload: MAX_MESSAGE_BYTES
});

httpServer.listen(PORT, HOST, () => {
    console.log(`[SERVER] Listening on ${HOST}:${PORT}`);
});

// All connected WebSocket clients, including players in the menu.
const players = new Map();

// Registered server-approved projectiles.
const shots = new Map();
const shotKeys = new Map();

let nextPlayerId = 1;
let nextShotId = 1;
let nextPingId = 1;

const spawnPoints = [
    { x: -400, y: -400 },
    { x: 0, y: -400 },
    { x: 400, y: -400 },
    { x: -400, y: 400 },
    { x: 0, y: 400 },
    { x: 400, y: 400 }
];

// -----------------------------------------------------------------------------
// General helpers
// -----------------------------------------------------------------------------

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

function sendError(ws, message) {
    send(ws, {
        type: 'error',
        message
    });
}

function getPlayer(ws) {
    if (!ws || !ws.playerId) return null;
    return players.get(ws.playerId) || null;
}

function isMatchPlayer(player) {
    return !!player && player.inBattle === true;
}

function isAlive(player) {
    return !!player && player.lifeState === 'alive';
}

function hasPosition(player) {
    return !!player &&
        isNumber(player.x) &&
        isNumber(player.y);
}

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function randomChance(chance) {
    return Math.random() < clamp(chance, 0, 1);
}

function resetPlayerPosition(player) {
    player.x = null;
    player.y = null;
    player.body_angle = 0;
    player.turret_angle = 0;
    player.state_seq = 0;
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
        in_battle: player.inBattle,
        hp: player.hp,
        score: player.score,
        damage_dealt: player.damageDealt,
        ping: player.ping,
        kills: player.kills,
        deaths: player.deaths
    };
}

// Broadcast only to players currently participating in the match.
// Clients in the main menu continue receiving their individual ping messages,
// but never receive match events or occupy match slots.
function broadcastToMatch(payload, exceptPlayerId = null) {
    const encoded = JSON.stringify(payload);

    for (const player of players.values()) {
        if (!player.inBattle) continue;
        if (player.id === exceptPlayerId) continue;
        if (!isOpen(player.ws)) continue;

        try {
            player.ws.send(encoded);
        } catch (error) {
            log('Match broadcast error:', error.message);
        }
    }
}

// -----------------------------------------------------------------------------
// Match chat
// -----------------------------------------------------------------------------

function handleChatMessage(ws, message) {
    const player = getPlayer(ws);

    // Only current match participants can use the chat.
    if (!isMatchPlayer(player)) {
        send(ws, {
            type: 'chat_rejected',
            reason: 'not_in_battle'
        });
        return;
    }

    if (typeof message.text !== 'string') {
        send(ws, {
            type: 'chat_rejected',
            reason: 'invalid_message'
        });
        return;
    }

    // Remove control characters and trim whitespace.
    const text = message.text
        .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
        .trim();

    if (!text) {
        send(ws, {
            type: 'chat_rejected',
            reason: 'empty_message'
        });
        return;
    }

    // Count Unicode code points instead of UTF-16 code units.
    const characters = [...text];

    if (characters.length > CHAT_MAX_LENGTH) {
        send(ws, {
            type: 'chat_rejected',
            reason: 'message_too_long'
        });
        return;
    }

    const now = Date.now();

    if (now - player.lastChatAt < CHAT_INTERVAL_MS) {
        send(ws, {
            type: 'chat_rejected',
            reason: 'rate_limited'
        });
        return;
    }

    player.lastChatAt = now;

    // The sender ID is taken from the server's player record.
    // broadcastToMatch includes the sender and excludes menu clients.
    broadcastToMatch({
        type: 'chat_message',
        player_id: player.id,
        text,
        timestamp: now
    });

    log(`CHAT ${player.id}: ${text}`);
}

function getMatchPlayers(exceptPlayerId = null) {
    return [...players.values()]
        .filter(player =>
            player.inBattle &&
            player.id !== exceptPlayerId
        );
}

function countMatchPlayers() {
    let count = 0;

    for (const player of players.values()) {
        if (player.inBattle) count++;
    }

    return count;
}

function getAvailableSpawnIndex() {
    const occupied = new Set();

    for (const player of players.values()) {
        if (player.inBattle &&
            Number.isInteger(player.spawn_index)) {
            occupied.add(player.spawn_index);
        }
    }

    for (let i = 0; i < spawnPoints.length; i++) {
        if (!occupied.has(i)) return i;
    }

    return -1;
}

function broadcastScoreboard() {
    const list = getMatchPlayers()
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

    broadcastToMatch({
        type: 'scoreboard',
        players: list
    });
}

// A newly connected client is not yet a match participant.
// Its spawn index remains null until enter_battle is accepted.
function sendWelcome(ws, player) {
    send(ws, {
        type: 'welcome',
        player_id: player.id,
        spawn_index: null,
        in_battle: false,
        players: []
    });
}

// -----------------------------------------------------------------------------
// Match lifecycle
// -----------------------------------------------------------------------------

function removeShotsOwnedBy(playerId) {
    for (const [shotId, shot] of shots) {
        if (shot.ownerId === playerId) {
            shots.delete(shotId);
            shotKeys.delete(shot.key);
        }
    }
}

function clearBurnsOwnedBy(playerId) {
    for (const target of players.values()) {
        if (target.burn && target.burn.ownerId === playerId) {
            target.burn = null;

            if (target.inBattle) {
                send(target.ws, {
                    type: 'status_effect_expired',
                    target_player_id: target.id,
                    effect: 'burn'
                });
            }
        }
    }
}

function enterBattle(ws) {
    const player = getPlayer(ws);

    if (!player) return;

    // Idempotent: a duplicate enter request must not allocate another slot.
    if (player.inBattle) {
        send(ws, {
            type: 'enter_battle_accepted',
            player_id: player.id,
            spawn_index: player.spawn_index,
            players: getMatchPlayers(player.id).map(publicPlayer)
        });
        return;
    }

    if (countMatchPlayers() >= MAX_PLAYERS) {
        send(ws, {
            type: 'enter_battle_rejected',
            reason: 'match_full'
        });
        return;
    }

    const spawnIndex = getAvailableSpawnIndex();

    if (spawnIndex < 0) {
        send(ws, {
            type: 'enter_battle_rejected',
            reason: 'no_spawn_available'
        });
        return;
    }

    player.inBattle = true;
    player.spawn_index = spawnIndex;

    player.lifeState = 'alive';
    player.hp = 100;
    player.burn = null;

    player.lastStateAt = 0;
    player.lastShotAt = 0;
    player.lastHitAt = 0;
    player.deathAt = null;
    player.inactiveAt = null;

    resetPlayerPosition(player);

    // Send existing match participants to the joining client first.
    send(ws, {
        type: 'enter_battle_accepted',
        player_id: player.id,
        spawn_index: player.spawn_index,
        players: getMatchPlayers(player.id).map(publicPlayer)
    });

    // Announce the joining participant to the other match clients.
    broadcastToMatch({
        type: 'player_joined',
        player: publicPlayer(player)
    }, player.id);

    broadcastScoreboard();

    log(
        'Player entered battle:',
        player.id,
        `spawn=${player.spawn_index}`,
        `players=${countMatchPlayers()}/${MAX_PLAYERS}`
    );
}

function leaveBattle(ws, reason = 'left_battle') {
    const player = getPlayer(ws);

    if (!player) return;

    if (!player.inBattle) {
        send(ws, {
            type: 'leave_battle_accepted'
        });
        return;
    }

    const playerId = player.id;

    // Mark the player as outside the match before broadcasting.
    // This ensures the leaving client will not receive its own match events.
     player.inBattle = false;
    player.lifeState = 'inactive';
    player.inactiveAt = null;
    player.deathAt = null;
    player.burn = null;

    // Reset match statistics when the player leaves the battle.
    player.score = 0;
    player.damageDealt = 0;
    player.kills = 0;
    player.deaths = 0;

    resetPlayerPosition(player);

    // A client that leaves cannot report future collisions for its shots.
    removeShotsOwnedBy(playerId);

    // Prevent ongoing burns from crediting a player who left the match.
    clearBurnsOwnedBy(playerId);

    broadcastToMatch({
        type: 'player_left',
        player_id: playerId,
        reason
    });

    send(ws, {
        type: 'leave_battle_accepted'
    });

    broadcastScoreboard();

    log(
        'Player left battle:',
        playerId,
        `players=${countMatchPlayers()}/${MAX_PLAYERS}`
    );
}

// -----------------------------------------------------------------------------
// Projectile registration and cleanup
// -----------------------------------------------------------------------------

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

function registerShot(ws, message) {
    const player = getPlayer(ws);

    if (!isMatchPlayer(player)) {
        send(ws, {
            type: 'shot_rejected',
            client_shot_id: message.shot_id || '',
            reason: 'not_in_battle'
        });
        return;
    }

    // A dead or inactive player cannot create a new projectile.
    // Previously registered projectiles can still score a later hit.
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

    send(ws, {
        type: 'shot_registered',
        client_shot_id: clientShotId,
        shot_id: shotId
    });

    // Only other match participants need visual projectile replication.
    broadcastToMatch({
        type: 'shot',
        player_id: player.id,
        shot_id: shotId,
        x: shot.x,
        y: shot.y,
        z: shot.z,
        angle: shot.angle
    }, player.id);
}

// -----------------------------------------------------------------------------
// Server-authoritative penetration, damage and status effects
// -----------------------------------------------------------------------------

// Returns the impact angle relative to the normal of the nearest hull side.
// 0 degrees = perpendicular hit; 90 degrees = fully grazing.
//
// Hit coordinates are transformed into the target hull's local space so
// front, rear, left and right sides are evaluated independently.
function getImpactAngleDeg(shot, target, hitX, hitY) {
    if (
        !isNumber(target.x) ||
        !isNumber(target.y) ||
        !isNumber(hitX) ||
        !isNumber(hitY)
    ) {
        const difference = Math.atan2(
            Math.sin(shot.angle - (target.body_angle || 0)),
            Math.cos(shot.angle - (target.body_angle || 0))
        );

        const angle = Math.abs(difference) * 180 / Math.PI;

        return clamp(
            Math.abs(180 - angle),
            0,
            90
        );
    }

    const dx = hitX - target.x;
    const dy = hitY - target.y;
    const bodyAngle = target.body_angle || 0;

    // Transform impact coordinates into tank-local space.
    const cos = Math.cos(bodyAngle);
    const sin = Math.sin(bodyAngle);

    const localX = dx * cos + dy * sin;
    const localY = -dx * sin + dy * cos;

    const distanceToXSide = Math.abs(
        BALANCE.TANK_HITBOX_HALF_WIDTH - Math.abs(localX)
    );

    const distanceToYSide = Math.abs(
        BALANCE.TANK_HITBOX_HALF_HEIGHT - Math.abs(localY)
    );

    let sideNormalAngle;

    if (distanceToXSide < distanceToYSide) {
        // Left/right side: normal points along local X.
        sideNormalAngle = bodyAngle +
            (localX >= 0 ? 0 : Math.PI);
    } else {
        // Front/rear side: normal points along local Y.
        sideNormalAngle = bodyAngle +
            (localY >= 0 ? Math.PI / 2 : -Math.PI / 2);
    }

    // Shot angle represents the actual travel direction,
    // measured from world +X in radians.
    const incomingAngle = sideNormalAngle + Math.PI;

    const difference = Math.atan2(
        Math.sin(shot.angle - incomingAngle),
        Math.cos(shot.angle - incomingAngle)
    );

    return clamp(
        Math.abs(difference) * 180 / Math.PI,
        0,
        90
    );
}

function getPenetrationChance(angleDeg) {
    if (angleDeg >= BALANCE.PENETRATION_RICOCHET_ANGLE_DEG) {
        return 0;
    }

    if (angleDeg <= BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG) {
        return BALANCE.PENETRATION_MAX_CHANCE;
    }

    const progress =
        (angleDeg - BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG) /
        (BALANCE.PENETRATION_RICOCHET_ANGLE_DEG -
            BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG);

    return BALANCE.PENETRATION_MAX_CHANCE * (1 - progress);
}

function getAngleDamageMultiplier(angleDeg) {
    if (angleDeg >= BALANCE.PENETRATION_RICOCHET_ANGLE_DEG) {
        return 0;
    }

    if (angleDeg <= BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG) {
        return 1;
    }

    const progress =
        (angleDeg - BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG) /
        (BALANCE.PENETRATION_RICOCHET_ANGLE_DEG -
            BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG);

    return 1 - progress;
}

function calculateBaseDamage(shot, now) {
    const elapsed = Math.max(0, (now - shot.createdAt) / 1000);

    const progress = clamp(
        elapsed / BALANCE.PROJECTILE_LIFETIME_SEC,
        0,
        1
    );

    return Math.max(
        0,
        Math.round(
            BALANCE.BASE_DAMAGE +
            (BALANCE.MIN_DAMAGE - BALANCE.BASE_DAMAGE) * progress
        )
    );
}

function broadcastKill(killerId, target, shotId, now) {
    if (!target ||
        !target.inBattle ||
        target.lifeState !== 'alive') {
        return false;
    }

    target.lifeState = 'dead';
    target.deathAt = now;
    target.inactiveAt = null;
    target.deaths++;
    target.burn = null;

    const possibleKiller = typeof killerId === 'string'
        ? players.get(killerId)
        : null;

    // The killer may already be dead: a projectile registered before death
    // is still allowed to earn the frag. The killer must still be in the match.
    const killer = possibleKiller &&
        possibleKiller.inBattle &&
        possibleKiller.id !== target.id
        ? possibleKiller
        : null;

    if (killer) {
        killer.kills++;
        killer.score += 10;
    }

    broadcastToMatch({
        type: 'kill_feed',
        killer_id: killer ? killer.id : 'WORLD',
        victim_id: target.id,
        shot_id: shotId || ''
    });

    broadcastToMatch({
        type: 'player_killed',
        killer_id: killer ? killer.id : 'WORLD',
        victim_id: target.id,
        shot_id: shotId || ''
    });

    broadcastScoreboard();

    log(
        `${killer ? killer.id : 'WORLD'} killed ${target.id}` +
        `${shotId ? ` with ${shotId}` : ' with burn'}`
    );

    return true;
}

function applyBurnTick(target, now) {
    const burn = target.burn;

    if (!burn ||
        !target.inBattle ||
        target.lifeState !== 'alive') {
        target.burn = null;
        return;
    }

    if (now >= burn.endsAt) {
        target.burn = null;

        send(target.ws, {
            type: 'status_effect_expired',
            target_player_id: target.id,
            effect: 'burn'
        });

        return;
    }

    if (now < burn.nextTickAt) return;

    burn.nextTickAt =
        now + BALANCE.BURN_TICK_INTERVAL_SEC * 1000;

    const damage =
        BALANCE.BURN_TICK_DAMAGE_MIN +
        Math.floor(
            Math.random() *
            (BALANCE.BURN_TICK_DAMAGE_MAX -
                BALANCE.BURN_TICK_DAMAGE_MIN + 1)
        );

    const actualDamage = Math.min(damage, target.hp);

    target.hp = Math.max(0, target.hp - actualDamage);

    const burnOwner = players.get(burn.ownerId);

    if (burnOwner &&
        burnOwner.inBattle &&
        burnOwner.id !== target.id) {
        burnOwner.damageDealt += actualDamage;
    }

    // Burn ticks do not use projectile penetration variables.
    // This also avoids the previous ReferenceError in applyBurnTick().
    broadcastToMatch({
        type: 'status_damage',
        target_player_id: target.id,
        killer_id: burn.ownerId || '',
        damage: actualDamage,
        hp: target.hp,
        effect: 'burn'
    });

    if (target.hp <= 0 && target.lifeState === 'alive') {
        broadcastKill(burn.ownerId, target, burn.shotId, now);
        target.burn = null;
    } else {
        broadcastScoreboard();
    }
}

// -----------------------------------------------------------------------------
// Hit validation and damage application
// -----------------------------------------------------------------------------

function registerHit(ws, message) {
    const shooter = getPlayer(ws);

    if (!isMatchPlayer(shooter)) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: message.shot_id || '',
            reason: 'not_in_battle'
        });
        return;
    }

    // Do not require the shooter to be alive.
    // A previously registered projectile may hit after its owner dies.

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

    const now = Date.now();
    const shotAge = now - shot.createdAt;

    if (shotAge > SHOT_TTL_MS) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'shot_expired'
        });
        return;
    }

    if (shotAge >
        BALANCE.PROJECTILE_LIFETIME_SEC * 1000 + 250) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'projectile_lifetime_exceeded'
        });
        return;
    }

    const targetId = message.target_player_id;

    const target = typeof targetId === 'string'
        ? players.get(targetId)
        : null;

    if (!target ||
        !target.inBattle ||
        target.id === shooter.id ||
        !isAlive(target)) {
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

    if (now - shooter.lastHitAt < HIT_INTERVAL_MS) {
        send(ws, {
            type: 'hit_rejected',
            client_shot_id: clientShotId,
            reason: 'rate_limited'
        });
        return;
    }

    if (!validCoordinate(message.x) ||
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

    // Never trust client-supplied damage or effect flags.
    let damage = calculateBaseDamage(shot, now);

    const impactAngleDeg = getImpactAngleDeg(
        shot,
        target,
        message.x,
        message.y
    );

    const penetrationChance =
        getPenetrationChance(impactAngleDeg);

    const angleDamageMultiplier =
        getAngleDamageMultiplier(impactAngleDeg);

    const ricochet =
        impactAngleDeg >= BALANCE.PENETRATION_RICOCHET_ANGLE_DEG ||
        !randomChance(penetrationChance);

    const flightProgress = clamp(
        ((now - shot.createdAt) / 1000) /
            BALANCE.PROJECTILE_LIFETIME_SEC,
        0,
        1
    );

    const distanceFactor = 1 - flightProgress;

    const effects = {
        critical: false,
        slow_duration: 0,
        stun_duration: 0,
        gun_damage_reloads: 0,
        burn_duration: 0
    };

    if (!ricochet &&
        impactAngleDeg <= BALANCE.CRITICAL_MAX_ANGLE_DEG &&
        randomChance(BALANCE.CRITICAL_CHANCE)) {
        damage = Math.round(
            damage * (
                1 + BALANCE.CRITICAL_MAX_BONUS_MULTIPLIER *
                    distanceFactor
            )
        );

        effects.critical = true;
    }

    if (!ricochet &&
        randomChance(
            BALANCE.SLOW_CHANCE_POINT_BLANK * distanceFactor
        )) {
        effects.slow_duration = BALANCE.SLOW_DURATION_SEC;
    }

    if (!ricochet &&
        randomChance(
            BALANCE.STUN_CHANCE_POINT_BLANK * distanceFactor
        )) {
        effects.stun_duration = BALANCE.STUN_DURATION_SEC;
    }

    if (!ricochet &&
        randomChance(BALANCE.GUN_DAMAGE_CHANCE)) {
        effects.gun_damage_reloads =
            BALANCE.GUN_DAMAGE_RELOADS;
    }

    const angle = clamp(impactAngleDeg, 0, 90);

    const burnChance =
        BALANCE.BURN_MIN_CHANCE +
        (BALANCE.BURN_MAX_CHANCE - BALANCE.BURN_MIN_CHANCE) *
            Math.sin(angle * Math.PI / 90);

    if (!ricochet && randomChance(burnChance)) {
        effects.burn_duration = BALANCE.BURN_DURATION_SEC;

        target.burn = {
            ownerId: shooter.id,
            shotId: shot.id,
            endsAt: now + BALANCE.BURN_DURATION_SEC * 1000,
            nextTickAt:
                now + BALANCE.BURN_TICK_INTERVAL_SEC * 1000
        };
    }

    log(
    `HIT DEBUG: ${shooter.id} -> ${target.id}`,
    `shot=${shot.id}`,
    `base=${calculateBaseDamage(shot, now)}`,
    `angle=${impactAngleDeg.toFixed(1)}deg`,
    `penetration=${(penetrationChance * 100).toFixed(1)}%`,
    `multiplier=${angleDamageMultiplier.toFixed(2)}`,
    `ricochet=${ricochet}`,
    `shotAngle=${shot.angle.toFixed(3)}`,
    `bodyAngle=${(target.body_angle || 0).toFixed(3)}`
);

    // Perpendicular hits keep full damage.
    // At 80 degrees and beyond, the shot ricochets for 0 damage.
    damage = ricochet
        ? 0
        : Math.floor(damage * angleDamageMultiplier);

    damage = clamp(damage, 0, MAX_DAMAGE);

    const actualDamage = Math.min(damage, target.hp);

    target.hp = Math.max(0, target.hp - actualDamage);

    // Server-authoritative statistics.
    shooter.score += 1;
    shooter.damageDealt += actualDamage;

    broadcastToMatch({
        type: 'projectile_hit',
        player_id: shooter.id,
        target_player_id: target.id,
        shot_id: shot.id,
        damage: actualDamage,
        penetration_chance: Math.round(penetrationChance * 100),
        ricochet,
        angle_damage_multiplier:
            Math.round(angleDamageMultiplier * 100),
        hp: target.hp,
        x: message.x,
        y: message.y,
        effects,
        impact_angle_deg: Math.round(impactAngleDeg)
    });

    if (target.hp <= 0 && target.lifeState === 'alive') {
        broadcastKill(shooter.id, target, shot.id, now);
    } else {
        broadcastScoreboard();
    }
}

// -----------------------------------------------------------------------------
// Position synchronization
// -----------------------------------------------------------------------------

function handleState(ws, message) {
    const player = getPlayer(ws);

    if (!isMatchPlayer(player)) {
        send(ws, {
            type: 'state_rejected',
            reason: 'not_in_battle'
        });
        return;
    }

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

    broadcastToMatch({
        type: 'state',
        player_id: player.id,
        x: player.x,
        y: player.y,
        body_angle: player.body_angle,
        turret_angle: player.turret_angle,
        state_seq: player.state_seq
    }, player.id);
}

// -----------------------------------------------------------------------------
// Death, inactivity and respawn
// -----------------------------------------------------------------------------

function setInactive(player, reason = 'inactive') {
    if (!player || !player.inBattle) return;

    if (player.lifeState === 'inactive') return;

    player.lifeState = 'inactive';
    player.inactiveAt = Date.now();

    resetPlayerPosition(player);

    broadcastToMatch({
        type: 'player_inactive',
        player_id: player.id,
        reason
    }, player.id);

    broadcastScoreboard();
}

function requestRespawn(ws) {
    const player = getPlayer(ws);

    if (!isMatchPlayer(player)) {
        send(ws, {
            type: 'respawn_rejected',
            reason: 'not_in_battle'
        });
        return;
    }

    if (player.lifeState === 'alive') {
        send(ws, {
            type: 'respawn_rejected',
            reason: 'already_alive'
        });
        return;
    }

    const now = Date.now();
    const transitionTime =
        player.deathAt || player.inactiveAt || now;

    if (now - transitionTime < RESPAWN_DELAY_MS) {
        send(ws, {
            type: 'respawn_rejected',
            reason: 'cooldown'
        });
        return;
    }

    player.lifeState = 'alive';
    player.hp = 100;
    player.burn = null;

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

// -----------------------------------------------------------------------------
// Message routing
// -----------------------------------------------------------------------------

function handleMessage(ws, message) {
    if (!message ||
        typeof message !== 'object' ||
        typeof message.type !== 'string') {
        sendError(ws, 'Malformed message');
        return;
    }

    switch (message.type) {

        case 'server_status': {
            // Статус сервера для выбора из главного меню.
            const playersInBattle = countMatchPlayers();

            send(ws, {
                type: 'server_status',
                connected_users: players.size,
                players_in_battle: playersInBattle,
                max_players: MAX_PLAYERS,
                available_slots: Math.max(
                    0,
                    MAX_PLAYERS - playersInBattle
                )
            });
            break;
        }

        case 'chat_send':
            handleChatMessage(ws, message);
        break;

        case 'pong': {
            const player = getPlayer(ws);

            if (!player ||
                !Number.isSafeInteger(message.ping_id)) {
                return;
            }

            const sentAt =
                player.pingPending.get(message.ping_id);

            if (sentAt === undefined) return;

            player.pingPending.delete(message.ping_id);

const now = Date.now();

// Не позволяем старому ответу перезаписать более свежий результат.
if (sentAt < player.lastPingSampleAt) {
    break;
}

player.lastPingSampleAt = sentAt;
player.ping = Math.max(0, now - sentAt);

if (player.inBattle) {
    broadcastScoreboard();
}

            break;
        }

        case 'enter_battle':
            enterBattle(ws);
            break;

        case 'leave_battle':
            leaveBattle(ws, 'left_battle');
            break;

        case 'client_ready': {
            // Compatibility with older clients.
            // client_ready must NOT enroll a client into a match.
            const player = getPlayer(ws);

            if (!player) return;

            send(ws, {
                type: 'ready',
                in_battle: player.inBattle
            });

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

            if (!player || !player.inBattle) return;

            // This notification does not add a second death.
            setInactive(player, 'tank_destroyed');
            break;
        }

        case 'player_killed':
            // Kill credit and HP are decided by registered projectile hits.
            // Do not trust client-supplied killer_id.
            log(
                'Ignored client player_killed report from',
                ws.playerId
            );
            break;

        case 'player_respawn':
            requestRespawn(ws);
            break;

        case 'request_scoreboard': {
            const player = getPlayer(ws);

            // A client in the menu must not receive match statistics.
            if (!isMatchPlayer(player)) {
                send(ws, {
                    type: 'scoreboard',
                    players: []
                });
                return;
            }

            send(ws, {
                type: 'scoreboard',
                players: getMatchPlayers()
                    .sort((a, b) =>
                        b.score - a.score ||
                        b.kills - a.kills ||
                        a.deaths - b.deaths
                    )
                    .map(matchPlayer => ({
                        id: matchPlayer.id,
                        score: matchPlayer.score,
                        damage_dealt: matchPlayer.damageDealt,
                        ping: matchPlayer.ping,
                        kills: matchPlayer.kills,
                        deaths: matchPlayer.deaths,
                        life_state: matchPlayer.lifeState
                    }))
            });

            break;
        }

        default:
            sendError(ws, `Unknown message type: ${message.type}`);
            break;
    }
}

// -----------------------------------------------------------------------------
// Disconnect handling
// -----------------------------------------------------------------------------

function removePlayer(ws) {
    const player = getPlayer(ws);

    if (!player) return;

    const playerId = player.id;
    const wasInBattle = player.inBattle;

    // Remove from the match before broadcasting to remaining participants.
    player.inBattle = false;

    players.delete(playerId);

    removeShotsOwnedBy(playerId);
    clearBurnsOwnedBy(playerId);

    if (wasInBattle) {
        broadcastToMatch({
            type: 'player_left',
            player_id: playerId,
            reason: 'disconnected'
        });

        broadcastScoreboard();
    }

    log(
        'Player disconnected:',
        playerId,
        wasInBattle ? '(was in battle)' : '(was in menu)'
    );
}

// -----------------------------------------------------------------------------
// WebSocket connection
// -----------------------------------------------------------------------------

wss.on('connection', (ws) => {
    // The connection limit is intentionally separate from MAX_PLAYERS.
    // Menu clients can stay connected without occupying a match slot.
    const playerNumber = nextPlayerId++;
    const id = `P${playerNumber}`;

    const player = {
        id,
        ws,

        // Match participation is explicit.
        inBattle: false,

        x: null,
        y: null,
        body_angle: 0,
        turret_angle: 0,
        state_seq: 0,

        spawn_index: null,

        lifeState: 'inactive',
        hp: 100,
        burn: null,

        score: 0,
        damageDealt: 0,
        ping: 0,
        lastPingSampleAt: 0,
        pingPending: new Map(),

        kills: 0,
        deaths: 0,

        lastStateAt: 0,
        lastShotAt: 0,
        lastHitAt: 0,
        lastChatAt: 0,

        deathAt: null,
        inactiveAt: null
    };

    ws.playerId = id;
    players.set(id, player);

    // Connecting does not broadcast player_joined and does not alter
    // the match scoreboard. The client is still in the menu at this point.
    sendWelcome(ws, player);

    log('WebSocket connected:', id, '(not yet in battle)');

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
            log(
                'Message handler error:',
                error.stack || error.message
            );

            sendError(ws, 'Internal server error');
        }
    });

    ws.on('close', () => removePlayer(ws));

    ws.on('error', (error) => {
        log(`Socket error for ${id}:`, error.message);
    });
});

// -----------------------------------------------------------------------------
// Periodic cleanup and ping
// -----------------------------------------------------------------------------

const cleanupTimer = setInterval(() => {
    pruneShots();

    const now = Date.now();

    for (const player of players.values()) {
        if (player.inBattle) {
            applyBurnTick(player, now);
        }
    }
}, 100);

const pingTimer = setInterval(() => {
    for (const player of players.values()) {
        if (!isOpen(player.ws)) continue;

        const pingId = nextPingId++;

        player.pingPending.set(pingId, Date.now());

        while (player.pingPending.size > 120) {
            const oldest =
                player.pingPending.keys().next().value;

            player.pingPending.delete(oldest);
        }

        if (!send(player.ws, {
            type: 'ping',
            ping_id: pingId
        })) {
            player.pingPending.delete(pingId);
        }
    }
}, 1000);

// -----------------------------------------------------------------------------
// Startup and graceful shutdown
// -----------------------------------------------------------------------------

wss.on('listening', () => {
    log(`Listening on ${HOST}:${PORT}`);
    log(`Max match players: ${MAX_PLAYERS}`);
    log(`Spawn points: ${spawnPoints.length}`);
});

function shutdown() {
    log('Shutting down...');

    clearInterval(cleanupTimer);
    clearInterval(pingTimer);

    for (const ws of wss.clients) {
        try {
            ws.close(1001, 'Server shutting down');
        } catch (_) {
            // Ignore shutdown errors.
        }
    }

    wss.close(() => process.exit(0));

    setTimeout(() => process.exit(0), 2000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
