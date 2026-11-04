// Behaviour tests for the bundled lib.js (the file users paste into the Flic Hub IDE),
// run under Node against a fake MQTT broker on 127.0.0.1. No hub or Home Assistant needed.
//
//   npm test                                  # tests ./lib.js
//   FLIC2HASS_LIB=/path/to/lib.js npm test    # tests another build
//
// Node is not the hub's runtime (QuickJS), so these test protocol and Home Assistant
// behaviour, not hub timing. Every scenario runs in its own process (see scenario.js).
"use strict";
const { test, before } = require("node:test");
const assert = require("node:assert/strict");
const { execFile } = require("child_process");
const path = require("path");

const LIB = path.resolve(process.env.FLIC2HASS_LIB || path.join(__dirname, "..", "lib.js"));

function run(scenario, arg) {
    return new Promise((resolve, reject) => {
        const args = [path.join(__dirname, "scenario.js"), scenario, LIB];
        if (arg !== undefined) args.push(String(arg));
        execFile(process.execPath, args, { timeout: 30000, maxBuffer: 64 * 1024 * 1024 }, (err, stdout) => {
            const lines = String(stdout).trim().split("\n");
            try {
                const res = JSON.parse(lines[lines.length - 1]);
                if (res.error) return reject(new Error(res.error));
                resolve(res);
            } catch (e) {
                reject(new Error(`scenario ${scenario} produced no result (${err && err.message})`));
            }
        });
    });
}

const R = {};
before(async () => {
    const jobs = {
        basic: ["basic"], kinds: ["press-kinds"], battery: ["battery"], noBattery: ["battery", "missing"],
        del: ["delete"], order: ["order"], burst: ["burst", 200000], two: ["two-buttons"],
    };
    await Promise.all(Object.keys(jobs).map(async (k) => { R[k] = await run(...jobs[k]); }));
});

const topic = (res, t) => res.topics[t] || { n: 0, payloads: [], retain: [] };
const configs = (res) => Object.keys(res.topics)
    .filter((t) => t.startsWith("homeassistant/") && t.endsWith("/config"))
    .map((t) => ({ t, payload: res.topics[t].payloads[0], retain: res.topics[t].retain[0] }));
const parsed = (payload) => (payload ? JSON.parse(payload) : null);
const BUTTON_ENTITIES = ["action", "state", "battery", "batteryLastUpdate", "lifeline", "connected",
    "ready", "activeDisconnect", "event", "button_short_press", "button_long_press", "button_double_press"];

// --- MQTT client --------------------------------------------------------------------

test("CONNECT carries a 60 s keep-alive by default", () => {
    assert.equal(R.basic.connect.keepAliveSecs, 60);
});

test("no keep-alive ping flood: no PINGREQ within the first 5 s", () => {
    assert.equal(R.basic.pingreq, 0);
});

test("parser survives a burst of 200,000 small packets in one read", () => {
    assert.equal(R.burst.crash, null);
});

test("coalesced packets are handled in order, and split packets reassemble", () => {
    assert.deepEqual(R.order.received, ["m1", "m2", "m3", "m4-split"]);
});

// --- Home Assistant discovery -------------------------------------------------------

test("each button registers its entities with retained discovery configs", () => {
    const mine = configs(R.basic).filter((c) => c.t.includes("/<bdaddr>/"));
    for (const id of BUTTON_ENTITIES) {
        const c = mine.find((x) => x.t.endsWith(`/${id}/config`));
        assert.ok(c, `missing discovery config for ${id}`);
        assert.equal(c.retain, true, `${id} config not retained`);
        assert.ok(parsed(c.payload).unique_id, `${id} has no unique_id`);
    }
});

test("no sensor/binary_sensor uses entity_category \"config\" (HA refuses to add them)", () => {
    const bad = configs(R.basic).filter((c) => c.payload && /^homeassistant\/(binary_sensor|sensor)\//.test(c.t)
        && parsed(c.payload).entity_category === "config").map((c) => c.t);
    assert.deepEqual(bad, []);
});

test("availability payloads match what the module publishes (ON/OFF)", () => {
    const bad = [];
    configs(R.basic).forEach((c) => {
        const cfg = parsed(c.payload);
        (cfg && cfg.availability || []).forEach((a) => {
            if (a.payload_not_available !== "OFF") bad.push(`${c.t}: ${a.payload_not_available}`);
        });
    });
    assert.deepEqual(bad, []);
});

test("the retired \"Passive Mode\" entity is removed with an empty retained config", () => {
    const c = configs(R.basic).find((x) => x.t === "homeassistant/binary_sensor/<bdaddr>/passive/config");
    assert.ok(c, "no publish on the passive config topic");
    assert.equal(c.payload, "");
    assert.equal(c.retain, true);
    assert.equal(topic(R.basic, "flic/<bdaddr>/passive").n, 0, "still publishing passive state");
});

// --- Button presses -----------------------------------------------------------------

test("a click fires the device trigger once, not retained", () => {
    const t = topic(R.basic, "flic/<bdaddr>/button_short_press");
    assert.deepEqual(t.payloads, ["ON"]);
    assert.deepEqual(t.retain, [false]);
});

test("event entity: retained config with the three event types", () => {
    const c = configs(R.basic).find((x) => x.t === "homeassistant/event/<bdaddr>/event/config");
    assert.ok(c, "no event entity config");
    const cfg = parsed(c.payload);
    assert.equal(c.retain, true);
    assert.equal(cfg.device_class, "button");
    assert.deepEqual(cfg.event_types, ["press", "double_press", "long_press"]);
});

test("click, double click and hold publish one non-retained event each", () => {
    const t = topic(R.kinds, "flic/<bdaddr>/event");
    assert.deepEqual(t.payloads.map((p) => JSON.parse(p).event_type), ["press", "double_press", "long_press"]);
    assert.ok(t.retain.every((r) => r === false));
});

test("each button's Click Action resets to none independently", () => {
    assert.deepEqual(Object.values(R.two.lastAction), ["none", "none"]);
});

// --- Battery ------------------------------------------------------------------------

test("missing battery data is published as HA's \"None\", not null/unknown", () => {
    assert.ok(topic(R.noBattery, "flic/<bdaddr>/battery").payloads.every((p) => p === "None"));
    assert.ok(topic(R.noBattery, "flic/<bdaddr>/batteryLastUpdate").payloads.every((p) => p === "None"));
});

test("battery last-update is a timestamp that does not change between publishes", () => {
    const t = topic(R.battery, "flic/<bdaddr>/batteryLastUpdate");
    assert.ok(t.n >= 2, `expected several meta publishes, got ${t.n}`);
    assert.equal(new Set(t.payloads).size, 1, `changed every publish: ${t.payloads.join(", ")}`);
    assert.ok(!Number.isNaN(Date.parse(t.payloads[0])), `not a timestamp: ${t.payloads[0]}`);
});

// --- Button removal -----------------------------------------------------------------

test("deleting a button does not crash the module", () => {
    assert.equal(R.del.crash, null);
});

test("deleting a button clears all its discovery configs (empty, retained)", () => {
    const cleared = BUTTON_ENTITIES.filter((id) => {
        const t = Object.keys(R.del.topics).find((k) => k.includes("/<bdaddr>/") && k.endsWith(`/${id}/config`));
        if (!t) return false;
        const last = R.del.topics[t];
        return last.payloads[last.n - 1] === "" && last.retain[last.n - 1] === true;
    });
    assert.deepEqual(cleared, BUTTON_ENTITIES);
});
