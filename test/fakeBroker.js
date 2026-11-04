// A minimal MQTT 3.1.1 broker for tests. It listens on 127.0.0.1 (random port), answers
// CONNECT/PINGREQ/SUBSCRIBE, and records everything the client sends. It is not a real
// broker: it never forwards messages, and it only sends what a test tells it to send.
"use strict";
const net = require("net");

function decodeRemainingLength(buf, offset) {
    let multiplier = 1, value = 0, i = offset, byte;
    do {
        if (i >= buf.length) return null; // incomplete
        byte = buf[i++];
        value += (byte & 127) * multiplier;
        multiplier *= 128;
    } while (byte & 128);
    return { value, next: i };
}

function startFakeBroker() {
    const record = {
        connect: null,   // { protocol, level, flags, keepAliveSecs, clientId }
        pingreq: 0,
        publishes: [],   // { topic, payload, retain, qos }
        subscribes: [],  // topic strings
    };
    let socket = null;
    const server = net.createServer((s) => {
        socket = s;
        let buf = Buffer.alloc(0);
        s.on("error", () => {});
        s.on("data", (chunk) => {
            buf = Buffer.concat([buf, chunk]);
            for (;;) {
                if (buf.length < 2) return;
                const rl = decodeRemainingLength(buf, 1);
                if (!rl || buf.length < rl.next + rl.value) return;
                const type = buf[0] >> 4;
                const body = buf.slice(rl.next, rl.next + rl.value);
                if (type === 1) { // CONNECT
                    const pl = body.readUInt16BE(0);
                    const flagsAt = 2 + pl + 1;
                    const cidLen = body.readUInt16BE(flagsAt + 3);
                    record.connect = {
                        protocol: body.slice(2, 2 + pl).toString(),
                        level: body[2 + pl],
                        flags: body[flagsAt],
                        keepAliveSecs: body.readUInt16BE(flagsAt + 1),
                        clientId: body.slice(flagsAt + 5, flagsAt + 5 + cidLen).toString(),
                    };
                    s.write(Buffer.from([0x20, 2, 0, 0])); // CONNACK, accepted
                } else if (type === 3) { // PUBLISH
                    const tl = body.readUInt16BE(0);
                    const qos = (buf[0] >> 1) & 3;
                    record.publishes.push({
                        topic: body.slice(2, 2 + tl).toString(),
                        payload: body.slice(2 + tl + (qos ? 2 : 0)).toString(),
                        retain: (buf[0] & 1) === 1,
                        qos,
                    });
                } else if (type === 8) { // SUBSCRIBE
                    let i = 2;
                    while (i < body.length) {
                        const l = body.readUInt16BE(i);
                        record.subscribes.push(body.slice(i + 2, i + 2 + l).toString());
                        i += 2 + l + 1;
                    }
                    s.write(Buffer.from([0x90, 3, body[0], body[1], 0])); // SUBACK
                } else if (type === 12) { // PINGREQ
                    record.pingreq++;
                    s.write(Buffer.from([0xd0, 0])); // PINGRESP
                }
                buf = buf.slice(rl.next + rl.value);
            }
        });
    });
    return new Promise((resolve) => {
        server.listen(0, "127.0.0.1", () => resolve({
            port: server.address().port,
            record,
            /** Send raw bytes to the connected client. */
            send: (bytes) => socket && socket.write(bytes),
            close: () => { if (socket) socket.destroy(); server.close(); },
        }));
    });
}

/** Encode a QoS 0 PUBLISH packet (remaining length < 128). */
function publishPacket(topic, payload) {
    const t = Buffer.from(topic), p = Buffer.from(payload);
    const body = Buffer.concat([Buffer.from([t.length >> 8, t.length & 255]), t, p]);
    if (body.length > 127) throw new Error("test helper only encodes short packets");
    return Buffer.concat([Buffer.from([0x30, body.length]), body]);
}

module.exports = { startFakeBroker, publishPacket };
