// Runs ONE scenario against a built lib.js and prints a JSON result as the last line of
// stdout. Run in its own process (see lib.test.js): lib.js patches Object.prototype and
// keeps module-level listener state, so scenarios must not share a process.
//
//   node test/scenario.js <scenario> <path/to/lib.js> [arg]
"use strict";
const EventEmitter = require("events");
const path = require("path");
const { startFakeBroker, publishPacket } = require("./fakeBroker");

const [, , scenario, libPath, arg] = process.argv;
const lib = require(path.resolve(libPath));

// Silence the module's own logging so the result line is easy to find.
const log = console.log.bind(console);
console.log = () => {};
const result = { scenario, crash: null };
// lib.js reports MQTT errors by throwing "Crashed" from a timer, so the hub restarts it.
process.on("uncaughtException", (e) => { result.crash = result.crash || String(e && e.message); });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function makeButton(bdaddr, name, extra) {
    return Object.assign({
        bdaddr, name, serialNumber: "BH12-" + name, uuid: "uuid-" + name, color: "white",
        flicVersion: 2, firmwareVersion: 10, batteryStatus: 87, batteryTimestamp: Date.now() - 60000,
        connected: true, ready: true, activeDisconnect: false, key: null,
    }, extra || {});
}

function makeButtonModule(buttons) {
    const bm = new EventEmitter();
    const byAddr = {};
    buttons.forEach((b) => { byAddr[b.bdaddr] = b; });
    bm.getButtons = () => buttons.slice();
    bm.getButton = (a) => byAddr[a];
    return bm;
}

function makeIR() {
    const ir = new EventEmitter();
    ir.record = () => {};
    ir.cancelRecord = () => {};
    ir.play = (arr, cb) => cb && cb();
    return ir;
}

function summarise(broker) {
    const r = broker.record;
    // Group publishes per topic, with the button address normalised to <bdaddr>.
    const topics = {};
    r.publishes.forEach((p) => {
        const key = p.topic.replace(/[0-9a-f]{2}(_[0-9a-f]{2}){5}/, "<bdaddr>");
        const t = topics[key] = topics[key] || { n: 0, payloads: [], retain: [] };
        t.n++;
        t.payloads.push(p.payload);
        t.retain.push(p.retain);
    });
    return { connect: r.connect, pingreq: r.pingreq, subscribes: r.subscribes, topics };
}

async function main() {
    const broker = await startFakeBroker();
    const mqtt = { host: "127.0.0.1", port: broker.port, client_id: "flic2hass-test" };
    const A = makeButton("80:e4:da:00:00:0a", "A");

    if (scenario === "basic") {
        // One button; single click after 800 ms; observe for 5 s.
        const bm = makeButtonModule([A]);
        lib.start(bm, makeIR(), { mqtt });
        await sleep(800);
        bm.emit("buttonSingleOrDoubleClickOrHold", { bdaddr: A.bdaddr, isSingleClick: true });
        await sleep(4200);
    } else if (scenario === "press-kinds") {
        const bm = makeButtonModule([A]);
        lib.start(bm, makeIR(), { mqtt, flicIR: { disabled: true } });
        await sleep(400);
        for (const kind of ["isSingleClick", "isDoubleClick", "isHold"]) {
            bm.emit("buttonSingleOrDoubleClickOrHold", { bdaddr: A.bdaddr, [kind]: true });
            await sleep(700);
        }
    } else if (scenario === "battery") {
        // arg "missing": no battery data, as the SDK allows (batteryStatus: null).
        const b = arg === "missing" ? makeButton(A.bdaddr, "A", { batteryStatus: null, batteryTimestamp: undefined }) : A;
        lib.start(makeButtonModule([b]), makeIR(), { mqtt, flicIR: { disabled: true } });
        await sleep(7000); // the module publishes button meta every 3 s
    } else if (scenario === "delete") {
        const bm = makeButtonModule([A]);
        lib.start(bm, makeIR(), { mqtt, flicIR: { disabled: true } });
        await sleep(500);
        bm.emit("buttonDeleted", { bdaddr: A.bdaddr }); // the SDK passes only { bdaddr }
        await sleep(2000); // "Crashed" is thrown 1 s after a disconnect
    } else if (scenario === "order") {
        // Three PUBLISHes in one TCP write, then one PUBLISH split over two writes.
        // The IR controller logs each value it receives on .../signal.
        const got = [];
        console.log = (...a) => { const m = a.join(" ").match(/setting currentSignal\t(\S+)/); if (m) got.push(m[1]); };
        lib.start(makeButtonModule([]), makeIR(), { mqtt });
        await sleep(300);
        const T = "flic/FlicHubIR0/signal";
        broker.send(Buffer.concat([publishPacket(T, "m1"), publishPacket(T, "m2"), publishPacket(T, "m3")]));
        await sleep(200);
        const p4 = publishPacket(T, "m4-split");
        broker.send(p4.slice(0, 5));
        await sleep(100);
        broker.send(p4.slice(5));
        await sleep(300);
        result.received = got;
    } else if (scenario === "burst") {
        // arg = number of 2-byte PINGRESP packets delivered in ONE write.
        lib.start(makeButtonModule([]), makeIR(), { mqtt, flicIR: { disabled: true } });
        await sleep(300);
        const n = Number(arg);
        const bytes = Buffer.alloc(2 * n);
        for (let i = 0; i < n; i++) bytes[2 * i] = 0xd0;
        broker.send(bytes);
        await sleep(1500);
    } else if (scenario === "two-buttons") {
        // A clicked, B double-clicked 100 ms later; both Click Actions must reset to "none".
        const B = makeButton("80:e4:da:00:00:0b", "B");
        const bm = makeButtonModule([A, B]);
        lib.start(bm, makeIR(), { mqtt, flicIR: { disabled: true } });
        await sleep(300);
        bm.emit("buttonSingleOrDoubleClickOrHold", { bdaddr: A.bdaddr, isSingleClick: true });
        await sleep(100);
        bm.emit("buttonSingleOrDoubleClickOrHold", { bdaddr: B.bdaddr, isDoubleClick: true });
        await sleep(1100);
        const last = {};
        broker.record.publishes.filter((p) => p.topic.endsWith("/action")).forEach((p) => { last[p.topic] = p.payload; });
        result.lastAction = last;
    } else {
        throw new Error("unknown scenario " + scenario);
    }

    Object.assign(result, summarise(broker));
    broker.close();
    log(JSON.stringify(result));
    process.exit(0);
}

main().catch((e) => { log(JSON.stringify({ scenario, error: String(e && e.stack) })); process.exit(2); });
