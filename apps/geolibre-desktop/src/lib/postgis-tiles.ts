/**
 * PostGIS vector tiles rendered in-process by the Tauri backend
 * (`src-tauri/src/postgis_tiles.rs`, `ST_AsMVT`), for the Android/iOS apps
 * that cannot run the Martin tile server (GeoLibre#2091).
 *
 * MapLibre reaches the renderer through the `geolibre-pgtiles://` custom
 * protocol; nothing listens on a port. Tile URLs carry only a password-masked
 * connection label (see `postgis-tiles-url.ts`), resolved here to the real
 * connection string from this session's connects or the saved connections.
 */

import { invoke } from "@tauri-apps/api/core";
import { addProtocol } from "maplibre-gl";
import {
  POSTGIS_TILES_PROTOCOL,
  parsePostgisTileUrl,
  type PostgisTileSourceRef,
} from "./postgis-tiles-url";
import {
  readSavedPostgresConnections,
  savedPostgresConnectionLabel,
} from "./saved-postgres-connections";

export interface PostgisTileSource {
  schema: string;
  table: string;
  geometryColumn: string;
  geometryType: string;
  srid: number;
}

export interface PostgisTileSourceInfo {
  /** Vector-tile layer name (the table name). */
  layer: string;
  /** West, south, east, north in WGS84; null for an empty table. */
  bounds: [number, number, number, number] | null;
  fields: Record<string, string>;
}

/** Connection strings used this session, by masked label. */
const sessionConnections = new Map<string, string>();

let protocolRegistered = false;

/** Remember a connection so its tile URLs resolve without a saved entry. */
export function rememberPostgisTilesConnection(connection: string): string {
  const label = savedPostgresConnectionLabel(connection);
  sessionConnections.set(label, connection);
  return label;
}

/**
 * Resolve a masked label to a connection string: this session's connects
 * first, else an unambiguous saved connection (the same rule as editable
 * PostGIS layers, `resolvePostgisConnection`).
 */
export function resolvePostgisTilesConnection(label: string): string | null {
  const session = sessionConnections.get(label);
  if (session) return session;
  const matches = readSavedPostgresConnections().filter(
    (connection) => savedPostgresConnectionLabel(connection) === label,
  );
  return matches.length === 1 ? matches[0] : null;
}

export function listPostgisTileSources(
  connection: string,
  defaultSrid?: string,
): Promise<PostgisTileSource[]> {
  return invoke<PostgisTileSource[]>("postgis_tiles_list_sources", {
    connection,
    defaultSrid: defaultSrid?.trim() || null,
  });
}

export function readPostgisTileSourceInfo(
  connection: string,
  source: Omit<PostgisTileSourceRef, "connectionLabel">,
): Promise<PostgisTileSourceInfo> {
  return invoke<PostgisTileSourceInfo>("postgis_tiles_source_info", {
    connection,
    defaultSrid: source.defaultSrid?.trim() || null,
    schema: source.schema,
    table: source.table,
    geometryColumn: source.geometryColumn,
  });
}

/** Close the backend's pool for a connection (or all pools). */
export function disconnectPostgisTiles(connection?: string): Promise<void> {
  return invoke("postgis_tiles_disconnect", { connection: connection ?? null });
}

export function registerPostgisTilesProtocol(): void {
  if (protocolRegistered) return;
  addProtocol(POSTGIS_TILES_PROTOCOL, async (request) => {
    const tile = parsePostgisTileUrl(request.url);
    const connection = resolvePostgisTilesConnection(tile.connectionLabel);
    if (!connection) {
      throw new Error(
        `No saved PostgreSQL connection matches ${tile.connectionLabel}. Reconnect it in Add Data.`,
      );
    }
    // The command returns a raw tauri::ipc::Response, which arrives as an
    // ArrayBuffer rather than a JSON number array.
    const data = await invoke<ArrayBuffer>("postgis_tiles_tile", {
      connection,
      defaultSrid: tile.defaultSrid ?? null,
      schema: tile.schema,
      table: tile.table,
      geometryColumn: tile.geometryColumn,
      z: tile.z,
      x: tile.x,
      y: tile.y,
    });
    return { data };
  });
  protocolRegistered = true;
}
