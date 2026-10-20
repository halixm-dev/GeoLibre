/**
 * Tile URLs for the in-process PostGIS tile renderer (`postgis-tiles.ts`).
 *
 * Pure and dependency-free so it unit-tests without MapLibre or Tauri.
 *
 * The URL is persisted in `.geolibre.json` projects, so it must never carry
 * credentials: it names the connection by its password-masked label (the same
 * label editable PostGIS layers persist as `postgisConnectionLabel`) and the
 * protocol handler resolves that back to a connection string at request time.
 */

export const POSTGIS_TILES_PROTOCOL = "geolibre-pgtiles";

export interface PostgisTileSourceRef {
  /** Password-masked connection label (see `savedPostgresConnectionLabel`). */
  connectionLabel: string;
  schema: string;
  table: string;
  geometryColumn: string;
  /** SRID assumed for a geometry column registered with SRID 0. */
  defaultSrid?: string;
}

export interface PostgisTileRequest extends PostgisTileSourceRef {
  z: number;
  x: number;
  y: number;
}

/** The `{z}/{x}/{y}` tile template for one PostGIS source. */
export function postgisTilesUrl(source: PostgisTileSourceRef): string {
  const params = new URLSearchParams({
    conn: source.connectionLabel,
    schema: source.schema,
    table: source.table,
    geom: source.geometryColumn,
  });
  const srid = source.defaultSrid?.trim();
  if (srid) params.set("srid", srid);
  return `${POSTGIS_TILES_PROTOCOL}://tile/{z}/{x}/{y}?${params.toString()}`;
}

/** Parse an expanded tile URL back into its source and tile coordinates. */
export function parsePostgisTileUrl(url: string): PostgisTileRequest {
  const parsed = new URL(url);
  if (parsed.protocol !== `${POSTGIS_TILES_PROTOCOL}:`) {
    throw new Error("Invalid PostGIS tile URL.");
  }
  // `tile` is the URL's host; the path carries z/x/y.
  const parts = parsed.pathname.split("/").filter(Boolean);
  if (parsed.hostname !== "tile" || parts.length !== 3) {
    throw new Error("Invalid PostGIS tile URL.");
  }
  const [z, x, y] = parts.map(parseTileCoordinate);
  const connectionLabel = parsed.searchParams.get("conn") ?? "";
  const schema = parsed.searchParams.get("schema") ?? "";
  const table = parsed.searchParams.get("table") ?? "";
  const geometryColumn = parsed.searchParams.get("geom") ?? "";
  if (!connectionLabel || !schema || !table || !geometryColumn) {
    throw new Error("Invalid PostGIS tile URL.");
  }
  const defaultSrid = parsed.searchParams.get("srid") ?? undefined;
  return {
    connectionLabel,
    schema,
    table,
    geometryColumn,
    defaultSrid,
    z,
    x,
    y,
  };
}

function parseTileCoordinate(value: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error("Invalid PostGIS tile coordinate.");
  }
  return parsed;
}
