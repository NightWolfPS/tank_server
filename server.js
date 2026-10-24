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

// Central PvP combat tuning.
const BALANCE = {
    BASE_DAMAGE: 30,
    MIN_DAMAGE: 1,
    PROJECTILE_LIFETIME_SEC: 1.5,
    // Angle is measured away from a perpendicular hit on the nearest
    // side of the target's rectangular hull hitbox.
    PENETRATION_FULL_DAMAGE_ANGLE_DEG: 5,
    PENETRATION_RICOCHET_ANGLE_DEG: 80,
    PENETRATION_MAX_CHANCE: 0.98,
    // Defold tank.go collision box half extents (36.5 x 40 units).
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
        score: player.score,
        damage_dealt: player.damageDealt,
        ping: player.ping,
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

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function randomChance(chance) {
    return Math.random() < clamp(chance, 0, 1);
}

// Returns an angle relative to the normal of the nearest hitbox side:
// 0 degrees = perpendicular hit; 90 degrees = fully grazing.
// Hit coordinates are transformed into the target hull's local space so
// front, rear, left and right sides are evaluated independently.
function getImpactAngleDeg(shot, target, hitX, hitY) {
    if (!isNumber(target.x) || !isNumber(target.y) ||
        !isNumber(hitX) || !isNumber(hitY)) {
        // Defensive fallback for incomplete state/hit data.
        const raw = Math.abs(shot.angle - target.body_angle);
        const wrapped = Math.abs(Math.atan2(Math.sin(raw), Math.cos(raw)));
        return clamp(Math.abs(180 - wrapped * 180 / Math.PI), 0, 90);
    }

    const dx = hitX - target.x;
    const dy = hitY - target.y;
    const bodyAngle = target.body_angle || 0;
    const cos = Math.cos(bodyAngle);
    const sin = Math.sin(bodyAngle);

    // Inverse 2D rotation into the target's local hull coordinates.
    const localX = dx * cos + dy * sin;
    const localY = -dx * sin + dy * cos;

    const distanceToXSide = Math.abs(BALANCE.TANK_HITBOX_HALF_WIDTH - Math.abs(localX));
    const distanceToYSide = Math.abs(BALANCE.TANK_HITBOX_HALF_HEIGHT - Math.abs(localY));

    let sideNormalAngle;
    if (distanceToXSide < distanceToYSide) {
        // Local +X/-X sides; angle convention matches the turret's shot angle.
        sideNormalAngle = bodyAngle + (localX >= 0 ? -Math.PI / 2 : Math.PI / 2);
    } else {
        // Local +Y/-Y (front/rear) sides.
        sideNormalAngle = bodyAngle + (localY >= 0 ? 0 : Math.PI);
    }

    // A projectile travelling perpendicular into the selected side points
    // opposite that side's outward normal.
    const incomingAngle = sideNormalAngle + Math.PI;
    const difference = Math.atan2(
        Math.sin(shot.angle - incomingAngle),
        Math.cos(shot.angle - incomingAngle)
    );

    return clamp(Math.abs(difference) * 180 / Math.PI, 0, 90);
}

function getPenetrationChance(angleDeg) {
    if (angleDeg >= BALANCE.PENETRATION_RICOCHET_ANGLE_DEG) return 0;
    if (angleDeg <= BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG) {
        return BALANCE.PENETRATION_MAX_CHANCE;
    }

    const progress = (angleDeg - BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG) /
        (BALANCE.PENETRATION_RICOCHET_ANGLE_DEG - BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG);
    return BALANCE.PENETRATION_MAX_CHANCE * (1 - progress);
}

function getAngleDamageMultiplier(angleDeg) {
    if (angleDeg >= BALANCE.PENETRATION_RICOCHET_ANGLE_DEG) return 0;
    if (angleDeg <= BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG) return 1;

    const progress = (angleDeg - BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG) /
        (BALANCE.PENETRATION_RICOCHET_ANGLE_DEG - BALANCE.PENETRATION_FULL_DAMAGE_ANGLE_DEG);
    return 1 - progress;
}

function calculateBaseDamage(shot, now) {
    const elapsed = Math.max(0, (now - shot.createdAt) / 1000);
    const progress = clamp(elapsed / BALANCE.PROJECTILE_LIFETIME_SEC, 0, 1);
    return Math.max(0, Math.round(
        BALANCE.BASE_DAMAGE +
        (BALANCE.MIN_DAMAGE - BALANCE.BASE_DAMAGE) * progress
    ));
}

function broadcastKill(killerId, target, shotId, now) {
    if (!target || target.lifeState !== 'alive') return false;
    target.lifeState = 'dead';
    target.deathAt = now;
    target.deaths++;

    const killer = typeof killerId === 'string' ? players.get(killerId) : null;
    if (killer && killer.id !== target.id) {
        killer.kills++;
        killer.score += 100;
    }

    broadcast({
        type: 'kill_feed',
        killer_id: killer ? killer.id : 'WORLD',
        victim_id: target.id,
        shot_id: shotId || ''
    });
    broadcast({
        type: 'player_killed',
        killer_id: killer ? killer.id : 'WORLD',
        victim_id: target.id,
        shot_id: shotId || ''
    });
    broadcastScoreboard();
    log(`${killer ? killer.id : 'WORLD'} killed ${target.id}${shotId ? ` with ${shotId}` : ' with burn'}`);
    return true;
}

function applyBurnTick(target, now) {
    const burn = target.burn;
    if (!burn || target.lifeState !== 'alive') {
        target.burn = null;
        return;
    }
    if (now >= burn.endsAt) {
        target.burn = null;
        broadcast({ type: 'status_effect_expired', target_player_id: target.id, effect: 'burn' });
        return;
    }
    if (now < burn.nextTickAt) return;

    burn.nextTickAt = now + BALANCE.BURN_TICK_INTERVAL_SEC * 1000;
    const damage = BALANCE.BURN_TICK_DAMAGE_MIN + Math.floor(
        Math.random() * (BALANCE.BURN_TICK_DAMAGE_MAX - BALANCE.BURN_TICK_DAMAGE_MIN + 1)
    );
    const actualDamage = Math.min(damage, target.hp);
    target.hp = Math.max(0, target.hp - actualDamage);
    const burnOwner = players.get(burn.ownerId);
    if (burnOwner) burnOwner.damageDealt += actualDamage;

    broadcast({
        type: 'status_damage',
        target_player_id: target.id,
        killer_id: burn.ownerId || '',
        damage: actualDamage,
        penetration_chance: Math.round(penetrationChance * 100),
        ricochet,
        angle_damage_multiplier: Math.round(angleDamageMultiplier * 100),
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

    if (Date.now() - shot.createdAt > BALANCE.PROJECTILE_LIFETIME_SEC * 1000 + 250) {
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

    if (!validCoordinate(message.x) || !validCoordinate(message.y)) {
        send(ws, { type: 'hit_rejected', client_shot_id: clientShotId, reason: 'invalid_hit_data' });
        return;
    }

    shooter.lastHitAt = now;
    shot.hitTargets.add(target.id);

    // Never trust client-supplied damage or effect flags.
    let damage = calculateBaseDamage(shot, now);
    const impactAngleDeg = getImpactAngleDeg(shot, target, message.x, message.y);
    const penetrationChance = getPenetrationChance(impactAngleDeg);
    const angleDamageMultiplier = getAngleDamageMultiplier(impactAngleDeg);
    const ricochet = impactAngleDeg >= BALANCE.PENETRATION_RICOCHET_ANGLE_DEG ||
        !randomChance(penetrationChance);
    const flightProgress = clamp(
        ((now - shot.createdAt) / 1000) / BALANCE.PROJECTILE_LIFETIME_SEC, 0, 1
    );
    const distanceFactor = 1 - flightProgress;
    const effects = {
        critical: false,
        slow_duration: 0,
        stun_duration: 0,
        gun_damage_reloads: 0,
        burn_duration: 0
    };

    if (!ricochet && impactAngleDeg <= BALANCE.CRITICAL_MAX_ANGLE_DEG &&
        randomChance(BALANCE.CRITICAL_CHANCE)) {
        damage = Math.round(damage * (
            1 + BALANCE.CRITICAL_MAX_BONUS_MULTIPLIER * distanceFactor
        ));
        effects.critical = true;
    }
    if (!ricochet && randomChance(BALANCE.SLOW_CHANCE_POINT_BLANK * distanceFactor)) {
        effects.slow_duration = BALANCE.SLOW_DURATION_SEC;
    }
    if (!ricochet && randomChance(BALANCE.STUN_CHANCE_POINT_BLANK * distanceFactor)) {
        effects.stun_duration = BALANCE.STUN_DURATION_SEC;
    }
    if (!ricochet && randomChance(BALANCE.GUN_DAMAGE_CHANCE)) {
        effects.gun_damage_reloads = BALANCE.GUN_DAMAGE_RELOADS;
    }

    const angle = clamp(impactAngleDeg, 0, 90);
    const burnChance = BALANCE.BURN_MIN_CHANCE +
        (BALANCE.BURN_MAX_CHANCE - BALANCE.BURN_MIN_CHANCE) *
        Math.sin(angle * Math.PI / 90);
    if (!ricochet && randomChance(burnChance)) {
        effects.burn_duration = BALANCE.BURN_DURATION_SEC;
        target.burn = {
            ownerId: shooter.id,
            shotId: shot.id,
            endsAt: now + BALANCE.BURN_DURATION_SEC * 1000,
            nextTickAt: now + BALANCE.BURN_TICK_INTERVAL_SEC * 1000
        };
    }

    // Angle penalty is applied to the final distance/critical-adjusted damage.
    // Perpendicular hits within +/-5 degrees keep 100% damage; at 80 degrees
    // and beyond the shot ricochets for exactly 0 damage.
    damage = ricochet ? 0 : Math.floor(damage * angleDamageMultiplier);
    damage = clamp(damage, 0, MAX_DAMAGE);
    const actualDamage = Math.min(damage, target.hp);
    target.hp = Math.max(0, target.hp - actualDamage);

    // Server-authoritative match statistics: +10 for an accepted hit,
    // and damage counts only the HP actually removed.
    shooter.score += 10;
    shooter.damageDealt += actualDamage;

    broadcast({
        type: 'projectile_hit',
        player_id: shooter.id,
        target_player_id: target.id,
        shot_id: shot.id,
        damage: actualDamage,
        penetration_chance: Math.round(penetrationChance * 100),
        ricochet,
        angle_damage_multiplier: Math.round(angleDamageMultiplier * 100),
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
            if (sentAt === undefined) return;
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
        burn: null,
        score: 0,
        damageDealt: 0,
        ping: 0,
        pingPending: new Map(),
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

const cleanupTimer = setInterval(() => {
    pruneShots();
    const now = Date.now();
    for (const player of players.values()) applyBurnTick(player, now);
}, 100);

let nextPingId = 1;
const pingTimer = setInterval(() => {
    for (const player of players.values()) {
        if (!isOpen(player.ws)) continue;
        const pingId = nextPingId++;
        player.pingPending.set(pingId, Date.now());
        while (player.pingPending.size > 120) {
            const oldest = player.pingPending.keys().next().value;
            player.pingPending.delete(oldest);
        }
        if (!send(player.ws, { type: 'ping', ping_id: pingId })) {
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