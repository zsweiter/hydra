// The hydra-scheduler wire contract (docs/CONTRACT.md), with no
// dependencies.
//
// A producer publishes a definition as a CloudEvent in binary
// mode on definitionSubject(subject): every attribute is a header,
// and the body is carried untouched. hydra-scheduler publishes
// each occurrence on the subject without the prefix, where
// occurrence() reads where it came from.

/** Header prefix of the CloudEvents NATS binding (Kafka uses "ce_"). */
export const NATS_PREFIX = "ce-";

/** Prefix of the subjects definitions are published on. */
export const DEFINITION_PREFIX = "schedule.";

/**
 * The subject to publish definitions on, for occurrences due on
 * subject: "notification.billing" becomes "schedule.notification.billing".
 *
 * @param {string} subject
 */
export function definitionSubject(subject) {
	return DEFINITION_PREFIX + subject;
}

/** What a definition does to its schedule. */
export const Action = Object.freeze({
	Create: "create",
	Replace: "replace",
	Pause: "pause",
	Resume: "resume",
	Cancel: "cancel",
	Delete: "delete",
});

const STATE_CHANGES = new Set([
	Action.Pause,
	Action.Resume,
	Action.Cancel,
	Action.Delete,
]);

const TYPES = new Set(["cron", "rrule", "interval", "once"]);

/**
 * @typedef {object} Event The CloudEvent carrying a definition.
 * @property {string} id           Unique per event; also send it as Nats-Msg-Id.
 * @property {string} source
 * @property {string} type
 * @property {string} [contentType] Media type of the body, which hydra never reads.
 * @property {string} [traceparent] W3C traceparent: its trace id is recorded with the change.
 */

/**
 * @typedef {object} Definition A recurrence definition, or a change of its state.
 * @property {string} id            Your stable id of the schedule.
 * @property {string} [action]      One of Action; "create" by default.
 * @property {"cron"|"rrule"|"interval"|"once"} [type]  Required to create or replace.
 * @property {string} [rule]        Cron or RRULE expression, ISO 8601 interval ("PT30S"),
 *                                  or for once an RFC 3339 time or ISO 8601 delay.
 * @property {number} [offsetMs]    Shift of every occurrence; negative fires early.
 * @property {number} [jitterMs]    Spread over [0, jitter), fixed per schedule.
 * @property {string} [start]       First nominal occurrence, RFC 3339.
 * @property {string} [end]         Last nominal occurrence, RFC 3339.
 * @property {number} [count]       Maximum number of occurrences.
 * @property {string} [timezone]    IANA zone of the rule; UTC by default.
 * @property {"skip"|"run_once"|"catch_up"} [misfire]
 * @property {number} [priority]    Lower fires first when several are due.
 * @property {string} [target]      Subject of the occurrences, instead of the default.
 */

/**
 * The headers of a definition event.
 *
 * @param {Event} event
 * @param {Definition} definition
 * @param {string} [prefix] Header prefix of the binding.
 * @returns {Record<string, string>}
 */
export function definitionHeaders(event, definition, prefix = NATS_PREFIX) {
	validate(event, definition);

	const action = definition.action ?? Action.Create;
	const headers = {};
	const attr = (name, value) => {
		headers[prefix + name] = String(value);
	};

	attr("specversion", "1.0");
	attr("id", event.id);
	attr("source", event.source);
	attr("type", event.type);
	attr("recurrenceid", definition.id);
	if (event.contentType) headers["content-type"] = event.contentType;
	if (event.traceparent) attr("traceparent", event.traceparent);
	if (action !== Action.Create) attr("recurrenceaction", action);
	if (STATE_CHANGES.has(action)) return headers;

	attr("recurrencetype", definition.type);

	// Empty values are left out: a once without rule fires now.
	const optional = {
		recurrencerule: definition.rule,
		recurrenceoffset:
			definition.offsetMs && isoDuration(definition.offsetMs),
		recurrencejitter:
			definition.jitterMs && isoDuration(definition.jitterMs),
		recurrencestart: definition.start,
		recurrenceend: definition.end,
		recurrencecount: definition.count,
		recurrencetimezone: definition.timezone,
		recurrencemisfire: definition.misfire,
		recurrencepriority: definition.priority,
		recurrencetarget: definition.target,
	};
	for (const [name, value] of Object.entries(optional)) {
		if (value) attr(name, value);
	}
	return headers;
}

/**
 * @param {Event} event
 * @param {Definition} definition
 */
function validate(event, definition) {
	if (!definition.id?.trim() || definition.id.includes("#")) {
		throw new Error("recurrenceid is required and must not contain '#'");
	}
	if (!event.id || !event.source || !event.type) {
		throw new Error("the event's id, source and type are required");
	}

	const action = definition.action ?? Action.Create;
	if (!Object.values(Action).includes(action)) {
		throw new Error(`unknown action ${action}`);
	}
	if (!STATE_CHANGES.has(action) && !TYPES.has(definition.type)) {
		throw new Error(`unknown recurrencetype ${definition.type}`);
	}
}

/**
 * Formats milliseconds as an ISO 8601 duration: -PT5M, PT1H30M, P1DT2H, PT0.5S.
 *
 * @param {number} ms
 */
export function isoDuration(ms) {
	if (ms === 0) return "PT0S";

	let out = ms < 0 ? "-P" : "P";
	let rest = Math.abs(ms);

	const DAY = 86_400_000;
	if (rest >= DAY) {
		out += `${Math.floor(rest / DAY)}D`;
		rest %= DAY;
	}
	if (rest === 0) return out;

	out += "T";
	for (const [unit, size] of [
		["H", 3_600_000],
		["M", 60_000],
	]) {
		if (rest >= size) {
			out += `${Math.floor(rest / size)}${unit}`;
			rest %= size;
		}
	}
	if (rest > 0) out += `${rest / 1000}S`;
	return out;
}

/**
 * The CloudEvents attributes of a message's headers, by lowercase name.
 *
 * @param {Iterable<[string, string]>} headers
 * @param {string} [prefix]
 * @returns {Record<string, string>}
 */
export function attributes(headers, prefix = NATS_PREFIX) {
	const attrs = {};
	for (const [name, value] of headers) {
		const key = name.toLowerCase();
		if (key === "content-type") {
			attrs.datacontenttype = value;
		} else if (key.startsWith(prefix)) {
			attrs[key.slice(prefix.length)] = value;
		}
	}
	return attrs;
}

/**
 * @typedef {object} Occurrence Where an occurrence comes from.
 * @property {string} schedule The recurrenceid of its definition.
 * @property {Date} at         Nominal time, without offset or jitter.
 * @property {number} revision
 * @property {string} trace
 * @property {string} id       Its event id: the same if it is ever delivered twice.
 */

/**
 * Reads an occurrence's hydra attributes; null for any other event on the subject.
 *
 * @param {Record<string, string>} attrs
 * @returns {Occurrence | null}
 */
export function occurrence(attrs) {
	if (!("hydraschedule" in attrs)) return null;

	return {
		schedule: attrs.hydraschedule,
		at: new Date(attrs.hydraoccurrence),
		revision: Number(attrs.hydrarevision ?? 1),
		trace: attrs.hydratrace,
		id: attrs.id,
	};
}

let sequence = 0;

/** A unique event id and a W3C traceparent, from the clock. */
export function newIds() {
	const now = process.hrtime.bigint();
	sequence++;

	const traceId = now.toString(16).padStart(32, "0");
	const spanId = sequence.toString(16).padStart(16, "0");
	return {
		id: `evt-${now}-${sequence}`,
		traceparent: `00-${traceId}-${spanId}-01`,
	};
}
