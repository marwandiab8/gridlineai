const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");
const { pathToFileURL } = require("url");
const { PLACE_ACTIVITY_CHOICES, normalizePlaceActivity, placeActivity, suggestPlaceActivity } = require("./placeActivities");
const { parseShortcutEventPayload } = require("./iosShortcutsIntegration");
const { mapShortcutEventToTimeLeftLifeEvent } = require("./timeLeftLifeEventMapper");

test("a place's name suggests what you do there; anything unclear is a visit", () => {
  const cases = {
    "Layal's house": "Visit",
    "Mom's place": "Visit",
    Home: "Home",
    "6 Docksteader": "Work",
    "Matheson Headoffice": "Work",
    "Costco Guelph": "Grocery shopping",
    "Costco Gas": "Fueling up",
    "Petro-Canada": "Fueling up",
    "Canadian Tire": "Shopping",
    "Canadian Tire Gas+": "Fueling up",
    "Kal Tire": "Car service",
    "Quick Oil Change": "Car service",
    "GoodLife Newmarket": "Gym",
    "Giant tiger": "Shopping",
    "Home Depot": "Shopping",
    "Coffee shop": "Eating out",
    "Tim Hortons": "Eating out",
    "Barber shop": "Errand",
    "Dr. Smith Dental": "Appointment",
    "St. Mary's Church": "Worship",
    "Raging Bull": "Visit",
  };
  for (const [name, activity] of Object.entries(cases)) assert.equal(suggestPlaceActivity(name), activity, name);
});

test("a place's own activity wins over the suggestion, tidied", () => {
  assert.equal(placeActivity({ name: "Raging Bull", activity: "  Eating   out " }), "Eating out");
  assert.equal(placeActivity({ name: "Costco Guelph" }), "Grocery shopping", "older places without one get the suggestion");
  assert.equal(normalizePlaceActivity("x".repeat(60)).length, 40);
});

test("the Known Places page offers the same choices as the server", async () => {
  const browser = await import(pathToFileURL(path.resolve(__dirname, "../public/place-activity-choices.js")).href);
  assert.deepEqual(browser.PLACE_ACTIVITY_CHOICES, PLACE_ACTIVITY_CHOICES);
});

test("a visit's activity and a departure's stay reach Time Left as a timed activity", () => {
  const now = new Date("2026-10-12T00:00:00Z");
  const arrive = parseShortcutEventPayload({ event_type: "arrive_location", timestamp: "2026-10-11T19:00:00Z", location_label: "Layal's house", latitude: 43.5, longitude: -80.2, activity: " Visit " }, now);
  assert.equal(arrive.event.placeActivity, "Visit");
  const mappedArrive = mapShortcutEventToTimeLeftLifeEvent({ ...arrive.event, id: "a1" }).event;
  assert.equal(mappedArrive.title, "Visit at Layal's house");
  assert.equal(mappedArrive.metadata.placeActivity, "Visit");
  assert.equal(mappedArrive.startAt, undefined, "an arrival is a moment");

  const leave = parseShortcutEventPayload({ event_type: "leave_location", timestamp: "2026-10-11T23:30:00Z", location_label: "Layal's house", activity: "Visit", stay_started_at: "2026-10-11T19:00:00.000Z" }, now);
  const mappedLeave = mapShortcutEventToTimeLeftLifeEvent({ ...leave.event, id: "l1" }).event;
  assert.deepEqual([mappedLeave.title, mappedLeave.startAt, mappedLeave.endAt, mappedLeave.occurredAt], ["Visit at Layal's house", "2026-10-11T19:00:00.000Z", "2026-10-11T23:30:00.000Z", "2026-10-11T23:30:00.000Z"]);

  // A start that isn't before the departure, or more than a week before it, is left out.
  for (const bad of ["2026-10-11T23:45:00Z", "2026-10-01T10:00:00Z", "soon"]) {
    const parsed = parseShortcutEventPayload({ event_type: "leave_location", timestamp: "2026-10-11T23:30:00Z", location_label: "X", stay_started_at: bad }, now);
    assert.equal(parsed.event.stayStartedAtIso, undefined, bad);
  }
  // Without an activity a visit maps exactly as before; other events never take one.
  const plain = mapShortcutEventToTimeLeftLifeEvent({ ...parseShortcutEventPayload({ event_type: "arrive_location", timestamp: "2026-10-11T19:00:00Z", location_label: "Somewhere" }, now).event, id: "p" }).event;
  assert.equal(plain.title, "Arrived at location");
  assert.equal(parseShortcutEventPayload({ event_type: "arrive_home", activity: "Visit" }, now).event.placeActivity, undefined);
});
