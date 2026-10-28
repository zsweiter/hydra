import { test } from "node:test";
import assert from "node:assert/strict";
import {
	Action,
	attributes,
	definitionHeaders,
	definitionSubject,
	isoDuration,
	occurrence,
} from "./hydra.mjs";

const event = {
	id: "e1",
	source: "/s",
	type: "t",
	contentType: "application/json",
};

test("isoDuration", () => {
	assert.equal(isoDuration(-300_000), "-PT5M");
	assert.equal(isoDuration(5_400_000), "PT1H30M");
	assert.equal(isoDuration(93_600_000), "P1DT2H");
	assert.equal(isoDuration(500), "PT0.5S");
});

test("create", () => {
	const headers = definitionHeaders(event, {
		id: "x",
		type: "interval",
		rule: "PT1M",
		offsetMs: -5000,
	});
	const attrs = attributes(Object.entries(headers));

	assert.equal(attrs.recurrencetype, "interval");
	assert.equal(attrs.recurrenceoffset, "-PT5S");
	assert.equal(attrs.datacontenttype, "application/json");
	assert.equal(attrs.recurrenceaction, undefined);
	assert.equal(occurrence(attrs), null);
});

test("state change", () => {
	const headers = definitionHeaders(event, { id: "x", action: Action.Pause });

	assert.equal(headers["ce-recurrenceaction"], "pause");
	assert.equal(headers["ce-recurrencetype"], undefined);
});

test("invalid", () => {
	assert.throws(() => definitionHeaders(event, { id: "a#1", type: "once" }));
	assert.throws(() => definitionHeaders(event, { id: "a", type: "weekly" }));
	assert.throws(() =>
		definitionHeaders({ ...event, id: "" }, { id: "a", type: "once" }),
	);
});

test("occurrence", () => {
	const o = occurrence({
		hydraschedule: "x",
		hydraoccurrence: "2026-11-01T12:00:00Z",
		hydrarevision: "2",
		id: "i",
	});

	assert.equal(o.schedule, "x");
	assert.equal(o.revision, 2);
	assert.equal(o.at.toISOString(), "2026-11-01T12:00:00.000Z");
});

test("definitionSubject", () => {
	assert.equal(definitionSubject("a.b"), "schedule.a.b");
});
