// Publishes recurrence definitions, then walks one of them through
// its lifecycle. Run hydra-scheduler on SCHEDULES first.

import { setTimeout as sleep } from "node:timers/promises";
import { connect, headers } from "@nats-io/transport-node";
import {
	jetstream,
	jetstreamManager,
	RetentionPolicy,
} from "@nats-io/jetstream";
import {
	Action,
	definitionHeaders,
	definitionSubject,
	newIds,
} from "./hydra.mjs";

const nc = await connect({
	servers: process.env.NATS_URL ?? "nats://localhost:4222",
});
await createStreams();

const js = jetstream(nc);
const encoder = new TextEncoder();

// Every 10 s, six times.
await publish(
	"notification.adherence",
	{ id: "adherence-p-001", type: "interval", rule: "PT10S", count: 6 },
	{ patient: "p-001", medication: "metformin 500mg" },
);

// Every minute, 10 s before the minute, in Santiago.
await publish(
	"notification.reminder",
	{
		id: "standup-u-42",
		type: "cron",
		rule: "* * * * *",
		offsetMs: -10_000,
		timezone: "America/Santiago",
	},
	{ user: "u-42", text: "stand-up in 10 seconds" },
);

// Once, in 30 s.
await publish(
	"notification.reminder",
	{ id: "appointment-u-7", type: "once", rule: "PT30S" },
	{ user: "u-7" },
);

// The lifecycle: pause, resume, replace, cancel.
await sleep(25_000);
await publish("notification.adherence", {
	id: "adherence-p-001",
	action: Action.Pause,
});

await sleep(20_000);
await publish("notification.adherence", {
	id: "adherence-p-001",
	action: Action.Resume,
});

await sleep(15_000);
await publish(
	"notification.adherence",
	{
		id: "adherence-p-001",
		action: Action.Replace,
		type: "interval",
		rule: "PT5S",
		count: 4,
	},
	{ patient: "p-001", medication: "metformin 850mg" },
);

await sleep(30_000);
await publish("notification.reminder", {
	id: "standup-u-42",
	action: Action.Cancel,
});

await nc.drain();

/**
 * The application owns its streams: NOTIFICATIONS for its events and
 * the occurrences (the duplicate window drops one published twice),
 * SCHEDULES for the definitions, a work queue that hydra-scheduler
 * alone consumes.
 */
async function createStreams() {
	const jsm = await jetstreamManager(nc);
	const twoMinutes = 120 * 1e9; // nanoseconds

	const streams = [
		{
			name: "NOTIFICATIONS",
			subjects: ["notification.>"],
			duplicate_window: twoMinutes,
		},
		{
			name: "SCHEDULES",
			subjects: [definitionSubject("notification.>")],
			retention: RetentionPolicy.Workqueue,
			duplicate_window: twoMinutes,
		},
	];
	for (const config of streams) {
		await jsm.streams
			.add(config)
			.catch(() => jsm.streams.update(config.name, config));
	}
}

/**
 * Publishes a definition for occurrences due on subject, and waits
 * until JetStream stored it.
 *
 * @param {string} subject
 * @param {import("./hydra.mjs").Definition} definition
 * @param {object} [body] Sent as JSON; hydra carries it untouched.
 */
async function publish(subject, definition, body) {
	const { id, traceparent } = newIds();
	const event = {
		id,
		source: "/examples/node",
		type: `com.acme.${subject}`,
		contentType: "application/json",
		traceparent,
	};

	const h = headers();
	for (const [name, value] of Object.entries(
		definitionHeaders(event, definition),
	)) {
		h.set(name, value);
	}

	const data =
		body === undefined
			? new Uint8Array()
			: encoder.encode(JSON.stringify(body));
	// The event id as msgID (Nats-Msg-Id): JetStream drops a retried publish.
	await js.publish(definitionSubject(subject), data, {
		headers: h,
		msgID: id,
	});

	console.log(
		`published ${subject} ${definition.action ?? Action.Create} ${definition.id}`,
	);
}
