import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  parsePostgisTileUrl,
  postgisTilesUrl,
} from "../apps/geolibre-desktop/src/lib/postgis-tiles-url";

const source = {
  connectionLabel: "postgresql://gis:****@db.example.com:5432/city",
  schema: "public",
  table: "roads & paths",
  geometryColumn: "geom",
};

function expand(template: string, z: number, x: number, y: number): string {
  return template.replace("{z}", String(z)).replace("{x}", String(x)).replace("{y}", String(y));
}

describe("postgisTilesUrl", () => {
  it("round-trips a source and tile coordinates", () => {
    const url = expand(postgisTilesUrl(source), 12, 2048, 1361);
    assert.deepEqual(parsePostgisTileUrl(url), {
      ...source,
      defaultSrid: undefined,
      z: 12,
      x: 2048,
      y: 1361,
    });
  });

  it("keeps the default SRID only when set", () => {
    assert.ok(!postgisTilesUrl({ ...source, defaultSrid: " " }).includes("srid="));
    const url = expand(postgisTilesUrl({ ...source, defaultSrid: "4326" }), 0, 0, 0);
    assert.equal(parsePostgisTileUrl(url).defaultSrid, "4326");
  });

  it("never contains a password", () => {
    const template = postgisTilesUrl(source);
    assert.ok(template.includes(encodeURIComponent("****")));
    assert.ok(template.startsWith("geolibre-pgtiles://tile/{z}/{x}/{y}?"));
  });
});

describe("parsePostgisTileUrl", () => {
  it("rejects other protocols, bad coordinates and missing parameters", () => {
    const good = expand(postgisTilesUrl(source), 1, 0, 1);
    assert.throws(() => parsePostgisTileUrl(good.replace("geolibre-pgtiles:", "https:")));
    assert.throws(() => parsePostgisTileUrl(expand(postgisTilesUrl(source), 1, -1, 0)));
    assert.throws(() => parsePostgisTileUrl(expand(postgisTilesUrl(source), 1, 0.5, 0)));
    assert.throws(() => parsePostgisTileUrl(good.replace(/&geom=[^&]*/, "")));
    assert.throws(() => parsePostgisTileUrl("geolibre-pgtiles://tile/1/0?conn=a"));
  });
});
