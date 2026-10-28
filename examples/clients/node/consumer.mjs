// Consumes NOTIFICATIONS like any application. Definitions live in
// their own stream, so every message here is an occurrence or a
// regular event; the hydra headers are read for logging only.

import { connect } from "@nats-io/transport-node";
import { AckPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";
import { attributes, occurrence } from "./hydra.mjs";

const STREAM = "NOTIFICATIONS";
const CONSUMER = "node-app";

const nc = await connect({
	servers: process.env.NATS_URL ?? "nats://localhost:4222",
});

const jsm = await jetstreamManager(nc);
await jsm.consumers.add(STREAM, {
	durable_name: CONSUMER,
	ack_policy: AckPolicy.Explicit,
	filter_subject: "notification.>",
});

const consumer = await jetstream(nc).consumers.get(STREAM, CONSUMER);
const decoder = new TextDecoder();

for await (const message of await consumer.consume()) {
	handle(message);
	message.ack();
}

/**
 * Handles one message. A real consumer would make this idempotent,
 * keyed by the event id.
 *
 * @param {import("@nats-io/jetstream").JsMsg} message
 */
function handle(message) {
	const headers = message.headers
		? [...message.headers.keys()].map((name) => [
				name,
				message.headers.get(name),
			])
		: [];
	const body = decoder.decode(message.data);

	const o = occurrence(attributes(headers));
	if (o) {
		console.log(
			`occurrence ${message.subject} schedule=${o.schedule} at=${o.at.toISOString()}`,
			`revision=${o.revision} trace=${o.trace} ${body}`,
		);
	} else {
		console.log(`event      ${message.subject} ${body}`);
	}
}
