const test = require("node:test");
const assert = require("node:assert/strict");
const { nameStop, addressLine, coordinatesName } = require("./placeNaming");

const RB = { latitude: 43.5471, longitude: -80.2968 };
const fetchReturning = (status, body) => async () => ({ ok: status === 200, status, json: async () => body });
const pub = { osm_type: "way", osm_id: 1, category: "amenity", type: "pub", name: "Raging Bull Sports Bar", display_name: "Raging Bull Sports Bar, 25 Macdonell Street, Guelph, Ontario, Canada", address: { amenity: "Raging Bull Sports Bar", house_number: "25", road: "Macdonell Street", city: "Guelph", state: "Ontario" } };

test("a business the map knows is named after it, with its town", async () => {
  const named = await nameStop({ ...RB, fetchImpl: fetchReturning(200, pub) });
  assert.equal(named.name, "Raging Bull Sports Bar");
  assert.equal(named.town, "Guelph");
  assert.equal(named.source, "osm_name");
  assert.equal(named.address, "25 Macdonell Street, Guelph");
});

test("a building without a name is its street address; a named road is an address too", async () => {
  const building = { ...pub, name: "", category: "building", type: "yes", address: { house_number: "25", road: "Macdonell Street", town: "Guelph" } };
  assert.deepEqual((({ name, source }) => ({ name, source }))(await nameStop({ ...RB, fetchImpl: fetchReturning(200, building) })), { name: "25 Macdonell Street, Guelph", source: "osm_address" });
  const road = { ...pub, name: "Macdonell Street", category: "highway", type: "residential", address: { road: "Macdonell Street", town: "Guelph" } };
  assert.deepEqual((({ name, source }) => ({ name, source }))(await nameStop({ ...RB, fetchImpl: fetchReturning(200, road) })), { name: "Macdonell Street, Guelph", source: "osm_address" });
});

test("when the lookup fails the stop is named after its coordinates, and nothing throws", async () => {
  assert.equal((await nameStop({ ...RB, fetchImpl: fetchReturning(503, {}) })).name, "Stop near 43.5471, -80.2968");
  assert.equal((await nameStop({ ...RB, fetchImpl: async () => { throw new Error("offline"); } })).source, "coordinates");
  assert.equal((await nameStop({ ...RB, fetchImpl: fetchReturning(200, { error: "Unable to geocode" }) })).source, "coordinates");
  assert.equal(coordinatesName(43.54709, -80.29679), "Stop near 43.5471, -80.2968");
  assert.deepEqual(addressLine({}), { street: "", town: "", line: "" });
});
