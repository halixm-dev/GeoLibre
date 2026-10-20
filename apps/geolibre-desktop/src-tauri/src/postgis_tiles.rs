//! In-process PostGIS vector tiles (`ST_AsMVT`) for the platforms that cannot
//! run the Martin tile server (Android/iOS have no Martin build and cannot spawn
//! helper processes, GeoLibre#2091).
//!
//! The webview never talks to PostgreSQL itself: a MapLibre custom protocol
//! (`geolibre-pgtiles://`, see `src/lib/postgis-tiles.ts`) forwards each tile
//! request to [`postgis_tiles_tile`], which renders it in the database with
//! `ST_AsMVT` and hands the protobuf bytes back over IPC. No local HTTP server
//! is bound, so nothing here needs a listening socket.
//!
//! Connection pools are keyed by the connection string and kept for the life
//! of the app; the frontend sends the string with every call, so a saved
//! project's layers resolve again after a restart without any server state.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use serde::Serialize;
use tokio::sync::Mutex;
use tokio_postgres::config::SslMode;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

/// Pools kept open at once. Each is small, but a phone should not accumulate
/// sockets for every connection string the user ever tried.
const MAX_POOLS: usize = 4;
/// Connections per pool. MapLibre requests several tiles in parallel, and one
/// PostgreSQL connection runs its queries one after another.
const POOL_SIZE: usize = 4;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Server-side cap on a single query (set per session unless the connection
/// string sets its own `options`), so a huge unindexed table cannot tie up a
/// pooled connection indefinitely.
const TILE_STATEMENT_TIMEOUT_MS: u32 = 30_000;
const MVT_EXTENT: u32 = 4096;
const MVT_BUFFER: u32 = 64;
/// Web Mercator world width in metres (EPSG:3857).
const WEB_MERCATOR_WORLD: f64 = 40_075_016.685_578_49;
const WEB_MERCATOR_HALF: f64 = WEB_MERCATOR_WORLD / 2.0;
const MAX_ZOOM: u32 = 30;

#[derive(Default)]
pub struct PostgisTilesState {
    pools: Mutex<HashMap<String, Arc<PoolEntry>>>,
}

struct PoolEntry {
    pool: Pool,
    /// The SRID assumed for geometry columns registered with SRID 0.
    default_srid: Option<i32>,
    /// Tile SQL per `schema.table.geometry_column`, built once per source.
    plans: Mutex<HashMap<SourceKey, Arc<TilePlan>>>,
    /// Insertion order for the LRU-ish eviction in [`pool_for`].
    created: std::time::Instant,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SourceKey {
    schema: String,
    table: String,
    geometry_column: String,
}

struct TilePlan {
    sql: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostgisTileSource {
    schema: String,
    table: String,
    geometry_column: String,
    geometry_type: String,
    srid: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostgisTileSourceInfo {
    /// The vector-tile layer name: the table name, as Martin names it.
    layer: String,
    /// West, south, east, north in WGS84, or None for an empty table.
    bounds: Option<[f64; 4]>,
    /// Attribute name -> PostgreSQL type name.
    fields: HashMap<String, String>,
}

/// Connect (or reuse the pool for) a database and list its spatial tables.
#[tauri::command]
pub async fn postgis_tiles_list_sources(
    state: tauri::State<'_, PostgisTilesState>,
    connection: String,
    default_srid: Option<String>,
) -> Result<Vec<PostgisTileSource>, String> {
    let default_srid = parse_default_srid(default_srid.as_deref())?;
    let entry = pool_for(&state, &connection, default_srid).await?;
    let client = entry.pool.get().await.map_err(pool_error)?;
    // geometry_columns covers tables, views and materialized views. Only list
    // what this role may read, so a table the user cannot open never shows up
    // as a source whose every tile then fails.
    let rows = client
        .query(
            "SELECT f_table_schema::text, f_table_name::text, f_geometry_column::text, \
                    type::text, srid \
             FROM geometry_columns \
             WHERE has_table_privilege(format('%I.%I', f_table_schema, f_table_name), 'SELECT') \
             ORDER BY f_table_schema, f_table_name, f_geometry_column",
            &[],
        )
        .await
        .map_err(|error| query_error("list spatial tables", &error))?;
    Ok(rows
        .iter()
        .map(|row| PostgisTileSource {
            schema: row.get(0),
            table: row.get(1),
            geometry_column: row.get(2),
            geometry_type: row.get(3),
            srid: row.get(4),
        })
        .collect())
}

/// Bounds and attribute fields for one source, for the layer's TileJSON-like
/// metadata (fit-to-layer, styling by attribute).
#[tauri::command]
pub async fn postgis_tiles_source_info(
    state: tauri::State<'_, PostgisTilesState>,
    connection: String,
    default_srid: Option<String>,
    schema: String,
    table: String,
    geometry_column: String,
) -> Result<PostgisTileSourceInfo, String> {
    let default_srid = parse_default_srid(default_srid.as_deref())?;
    let entry = pool_for(&state, &connection, default_srid).await?;
    let client = entry.pool.get().await.map_err(pool_error)?;
    let columns = read_columns(&client, &schema, &table, &geometry_column).await?;
    let key = SourceKey {
        schema,
        table,
        geometry_column,
    };
    let srid = resolve_srid(
        &client,
        &entry,
        &key.schema,
        &key.table,
        &key.geometry_column,
    )
    .await?;
    let bounds = mercator_extent(&client, &key, srid)
        .await?
        .map(|[west, south, east, north]| {
            let (west, south) = mercator_to_lon_lat(west, south);
            let (east, north) = mercator_to_lon_lat(east, north);
            [west, south, east, north]
        });
    let table = key.table;

    Ok(PostgisTileSourceInfo {
        layer: table,
        bounds,
        fields: columns
            .into_iter()
            .map(|column| (column.name, column.type_name))
            .collect(),
    })
}

/// Render one vector tile. Returns raw protobuf bytes (empty for an empty
/// tile) without JSON-encoding them through the IPC layer.
#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub async fn postgis_tiles_tile(
    state: tauri::State<'_, PostgisTilesState>,
    connection: String,
    default_srid: Option<String>,
    schema: String,
    table: String,
    geometry_column: String,
    z: u32,
    x: u32,
    y: u32,
) -> Result<tauri::ipc::Response, String> {
    validate_tile(z, x, y)?;
    let default_srid = parse_default_srid(default_srid.as_deref())?;
    let entry = pool_for(&state, &connection, default_srid).await?;
    let key = SourceKey {
        schema,
        table,
        geometry_column,
    };
    let plan = tile_plan(&entry, &key).await?;
    let client = entry.pool.get().await.map_err(pool_error)?;
    let margin =
        WEB_MERCATOR_WORLD / f64::from(1_u32 << z) * f64::from(MVT_BUFFER) / f64::from(MVT_EXTENT);
    let row = client
        .query_one(&plan.sql, &[&(z as i32), &(x as i32), &(y as i32), &margin])
        .await
        .map_err(|error| query_error("render the tile", &error))?;
    let bytes: Option<Vec<u8>> = row.get(0);
    Ok(tauri::ipc::Response::new(bytes.unwrap_or_default()))
}

/// Close every pool, e.g. when the user stops the connection in Add Data.
#[tauri::command]
pub async fn postgis_tiles_disconnect(
    state: tauri::State<'_, PostgisTilesState>,
    connection: Option<String>,
) -> Result<(), String> {
    let mut pools = state.pools.lock().await;
    match connection {
        Some(connection) => {
            if let Some(entry) = pools.remove(&connection) {
                entry.pool.close();
            }
        }
        None => {
            for (_, entry) in pools.drain() {
                entry.pool.close();
            }
        }
    }
    Ok(())
}

async fn pool_for(
    state: &PostgisTilesState,
    connection: &str,
    default_srid: Option<i32>,
) -> Result<Arc<PoolEntry>, String> {
    let connection = connection.trim();
    if connection.is_empty() {
        return Err("Enter a PostgreSQL connection string.".to_string());
    }
    let mut pools = state.pools.lock().await;
    if let Some(entry) = pools.get(connection) {
        if entry.default_srid == default_srid {
            return Ok(Arc::clone(entry));
        }
        // A changed default SRID changes the cached tile SQL; start fresh.
        if let Some(stale) = pools.remove(connection) {
            stale.pool.close();
        }
    }

    let entry = Arc::new(build_pool(connection, default_srid)?);
    // Fail the connect here with a clear message rather than on the first tile.
    let client = entry.pool.get().await.map_err(pool_error)?;
    client
        .query_one("SELECT postgis_lib_version()", &[])
        .await
        .map_err(|_| "This database does not have the PostGIS extension enabled.".to_string())?;
    drop(client);

    if pools.len() >= MAX_POOLS {
        if let Some(oldest) = pools
            .iter()
            .min_by_key(|(_, entry)| entry.created)
            .map(|(key, _)| key.clone())
        {
            if let Some(evicted) = pools.remove(&oldest) {
                evicted.pool.close();
            }
        }
    }
    pools.insert(connection.to_string(), Arc::clone(&entry));
    Ok(entry)
}

fn build_pool(connection: &str, default_srid: Option<i32>) -> Result<PoolEntry, String> {
    let tls = TlsRequirement::from_connection_string(connection);
    let mut config = tokio_postgres::Config::from_str(&tls.normalized_connection)
        .map_err(|error| format!("Could not parse the PostgreSQL connection string: {error}"))?;
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(CONNECT_TIMEOUT);
    }
    if config.get_options().is_none() {
        config.options(format!("-c statement_timeout={TILE_STATEMENT_TIMEOUT_MS}"));
    }
    if config.get_application_name().is_none() {
        config.application_name("GeoLibre");
    }
    let manager_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let manager = if config.get_ssl_mode() == SslMode::Disable {
        Manager::from_config(config, NoTls, manager_config)
    } else {
        Manager::from_config(config, tls.connector()?, manager_config)
    };
    let pool = Pool::builder(manager)
        .max_size(POOL_SIZE)
        .build()
        .map_err(|error| format!("Could not create the PostgreSQL connection pool: {error}"))?;
    Ok(PoolEntry {
        pool,
        default_srid,
        plans: Mutex::new(HashMap::new()),
        created: std::time::Instant::now(),
    })
}

async fn tile_plan(entry: &PoolEntry, key: &SourceKey) -> Result<Arc<TilePlan>, String> {
    if let Some(plan) = entry.plans.lock().await.get(key) {
        return Ok(Arc::clone(plan));
    }
    let client = entry.pool.get().await.map_err(pool_error)?;
    let columns = read_columns(&client, &key.schema, &key.table, &key.geometry_column).await?;
    let srid = resolve_srid(
        &client,
        entry,
        &key.schema,
        &key.table,
        &key.geometry_column,
    )
    .await?;
    let id_column = read_integer_primary_key(&client, &key.schema, &key.table).await?;
    // A failed extent (e.g. a CRS whose transform rejects the padded box)
    // only costs the clip: fall back to filtering by the raw tile envelope.
    let clip = mercator_extent(&client, key, srid).await.ok().flatten();
    let plan = Arc::new(TilePlan {
        sql: build_tile_sql(key, srid, &columns, id_column.as_deref(), clip),
    });
    entry
        .plans
        .lock()
        .await
        .insert(key.clone(), Arc::clone(&plan));
    Ok(plan)
}

/// The tile query. $1..$3 are z/x/y, $4 the buffer margin in metres.
///
/// The bbox filter transforms the (buffered) tile envelope into the column's
/// SRID rather than transforming every row, so the column's spatial index is
/// used. `ST_TileEnvelope` needs PostGIS 3.0+.
///
/// A low-zoom tile envelope spans far more of the world than a projected CRS
/// can represent (a z0 tile transformed to UTM is garbage, so the filter
/// matched nothing and the tile came back empty). With `clip` (the table's
/// padded Web Mercator extent) the envelope is first cut down to the data,
/// then densified so its transformed bounding box still covers the curved
/// edges.
fn build_tile_sql(
    key: &SourceKey,
    srid: ColumnSrid,
    columns: &[ColumnInfo],
    id_column: Option<&str>,
    clip: Option<[f64; 4]>,
) -> String {
    // Every name this query introduces is prefixed so no table column can
    // collide with it.
    let source = "__geolibre_src";
    let geometry = format!("{source}.{}", quote_ident(&key.geometry_column));
    let qualified = format!("{}.{}", quote_ident(&key.schema), quote_ident(&key.table));
    // ST_AsMVT's feature_id_name must name an integer column; leave it out
    // otherwise rather than passing NULL.
    let feature_id = match id_column {
        Some(id) if columns.iter().any(|column| column.name == id) => {
            format!(", {}", quote_literal(id))
        }
        _ => String::new(),
    };
    let tile_envelope = "ST_Expand(ST_TileEnvelope($1, $2, $3), $4)";
    // An SRID 0 column is tagged with the user's default SRID for the
    // transform, while the bbox filter compares in SRID 0 (mixing SRIDs in
    // `&&` is an error).
    let to_column_srid = |envelope: &str| match srid {
        ColumnSrid::Declared(3857) => envelope.to_string(),
        ColumnSrid::Declared(srid) => format!("ST_Transform({envelope}, {srid})"),
        ColumnSrid::Assumed(srid) => format!("ST_SetSRID(ST_Transform({envelope}, {srid}), 0)"),
    };
    let source_geometry = match srid {
        ColumnSrid::Declared(_) => format!("{geometry}::geometry"),
        ColumnSrid::Assumed(srid) => format!("ST_SetSRID({geometry}::geometry, {srid})"),
    };
    let from_where = match clip {
        Some([west, south, east, north]) if srid != ColumnSrid::Declared(3857) => {
            let densified = to_column_srid(
                "ST_Segmentize(c, GREATEST(ST_XMax(c) - ST_XMin(c), ST_YMax(c) - ST_YMin(c)) / 16 + 1)",
            );
            format!(
                "{qualified} AS {source}, \
                 (SELECT {densified} AS f FROM (SELECT ST_Intersection({tile_envelope}, \
                 ST_MakeEnvelope({west}, {south}, {east}, {north}, 3857)) AS c) AS __geolibre_clip \
                 WHERE NOT ST_IsEmpty(c)) AS __geolibre_filter \
                 WHERE {geometry} && __geolibre_filter.f"
            )
        }
        _ => format!(
            "{qualified} AS {source} WHERE {geometry} && {}",
            to_column_srid(tile_envelope)
        ),
    };
    let mvt_geometry = "__geolibre_mvt_geom";
    let mut select_list: Vec<String> = columns
        .iter()
        .map(|column| format!("{source}.{}", quote_ident(&column.name)))
        .collect();
    select_list.push(format!(
        "ST_AsMVTGeom(ST_Transform(ST_CurveToLine({source_geometry}), 3857), \
         ST_TileEnvelope($1, $2, $3), {MVT_EXTENT}, {MVT_BUFFER}, true) AS {mvt_geometry}"
    ));
    format!(
        "SELECT ST_AsMVT(tile, {layer}, {MVT_EXTENT}, '{mvt_geometry}'{feature_id}) \
         FROM (SELECT {select} FROM {from_where}) AS tile \
         WHERE {mvt_geometry} IS NOT NULL",
        layer = quote_literal(&key.table),
        select = select_list.join(", "),
    )
}

/// The table's extent in Web Mercator metres (west, south, east, north),
/// padded so features on the edge stay inside, or None for an empty table.
///
/// `ST_EstimatedExtent` reads planner statistics (instant, but NULL until the
/// table was ANALYZEd, and an error for a view), so it is tried on its own and
/// a real `ST_Extent` scan is the fallback. The box is densified before the
/// transform so a projected CRS's curved edges are covered.
async fn mercator_extent(
    client: &deadpool_postgres::Client,
    key: &SourceKey,
    srid: ColumnSrid,
) -> Result<Option<[f64; 4]>, String> {
    let estimated = client
        .query_one(
            "SELECT ST_EstimatedExtent($1, $2, $3) IS NOT NULL",
            &[&key.schema, &key.table, &key.geometry_column],
        )
        .await
        .map(|row| row.get::<_, bool>(0))
        .unwrap_or(false);
    let extent = if estimated {
        format!(
            "ST_EstimatedExtent({}, {}, {})::geometry",
            quote_literal(&key.schema),
            quote_literal(&key.table),
            quote_literal(&key.geometry_column),
        )
    } else {
        format!(
            "(SELECT ST_Extent({}::geometry) FROM {}.{})::geometry",
            quote_ident(&key.geometry_column),
            quote_ident(&key.schema),
            quote_ident(&key.table),
        )
    };
    // Geographic coordinates are clamped to the Web Mercator latitude range
    // after padding, where ST_Transform would otherwise reject them.
    let clamp_geographic = if srid.value() == 4326 {
        "ST_Intersection(e, ST_MakeEnvelope(-180, -85.06, 180, 85.06, 4326))"
    } else {
        "e"
    };
    let sql = format!(
        "SELECT ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b) FROM ( \
           SELECT ST_Transform(ST_Segmentize(g, \
             GREATEST(ST_XMax(g) - ST_XMin(g), ST_YMax(g) - ST_YMin(g)) / 32 + 1e-9), 3857) AS b \
           FROM (SELECT {clamp_geographic} AS g FROM ( \
             SELECT ST_SetSRID(ST_Expand(x, \
               GREATEST(ST_XMax(x) - ST_XMin(x), ST_YMax(x) - ST_YMin(x)) * 0.05), $1) AS e \
             FROM (SELECT {extent} AS x) AS raw WHERE x IS NOT NULL \
           ) AS padded) AS clamped WHERE NOT ST_IsEmpty(g) \
         ) AS mercator"
    );
    let row = client
        .query_opt(&sql, &[&srid.value()])
        .await
        .map_err(|error| query_error("compute the table extent", &error))?;
    Ok(row.map(|row| {
        let [west, south, east, north] = [0, 1, 2, 3].map(|index| row.get::<_, f64>(index));
        // A point table has a zero-size extent; give it a few metres so the
        // clipped tile envelope is never degenerate.
        let pad = ((east - west).max(north - south) * 0.01).max(10.0);
        [
            clamp(west - pad, -WEB_MERCATOR_HALF, WEB_MERCATOR_HALF),
            clamp(south - pad, -WEB_MERCATOR_HALF, WEB_MERCATOR_HALF),
            clamp(east + pad, -WEB_MERCATOR_HALF, WEB_MERCATOR_HALF),
            clamp(north + pad, -WEB_MERCATOR_HALF, WEB_MERCATOR_HALF),
        ]
    }))
}

/// Web Mercator metres to WGS84 longitude/latitude degrees.
fn mercator_to_lon_lat(x: f64, y: f64) -> (f64, f64) {
    const EARTH_RADIUS: f64 = 6_378_137.0;
    let lon = (x / EARTH_RADIUS).to_degrees();
    let lat = (y / EARTH_RADIUS).sinh().atan().to_degrees();
    (
        clamp(lon, -180.0, 180.0),
        clamp(lat, -85.051_129, 85.051_129),
    )
}

/// A geometry column's SRID: as registered, or the user's default SRID for a
/// column registered with SRID 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnSrid {
    Declared(i32),
    Assumed(i32),
}

impl ColumnSrid {
    fn value(self) -> i32 {
        match self {
            ColumnSrid::Declared(srid) | ColumnSrid::Assumed(srid) => srid,
        }
    }
}

struct ColumnInfo {
    name: String,
    type_name: String,
}

/// The table's attribute columns that make sense as MVT properties: every
/// column except geometry/geography/raster/binary ones (ST_AsMVT would encode
/// those as huge text blobs).
async fn read_columns(
    client: &deadpool_postgres::Client,
    schema: &str,
    table: &str,
    geometry_column: &str,
) -> Result<Vec<ColumnInfo>, String> {
    let rows = client
        .query(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod) \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE n.nspname = $1 AND c.relname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped AND a.attname <> $3 \
               AND t.typname NOT IN ('geometry', 'geography', 'raster', 'bytea', 'box2d', 'box3d') \
             ORDER BY a.attnum",
            &[&schema, &table, &geometry_column],
        )
        .await
        .map_err(|error| query_error("read the table columns", &error))?;
    if rows.is_empty() {
        // Either a table of only geometry, or it does not exist; tell them apart.
        let exists = client
            .query_opt(
                "SELECT 1 FROM geometry_columns \
                 WHERE f_table_schema = $1 AND f_table_name = $2 AND f_geometry_column = $3",
                &[&schema, &table, &geometry_column],
            )
            .await
            .map_err(|error| query_error("read the table columns", &error))?;
        if exists.is_none() {
            return Err(format!(
                "The spatial table {schema}.{table} ({geometry_column}) was not found."
            ));
        }
    }
    Ok(rows
        .iter()
        .map(|row| ColumnInfo {
            name: row.get(0),
            type_name: row.get(1),
        })
        .collect())
}

/// The column's SRID from geometry_columns, or the default SRID for SRID 0.
async fn resolve_srid(
    client: &deadpool_postgres::Client,
    entry: &PoolEntry,
    schema: &str,
    table: &str,
    geometry_column: &str,
) -> Result<ColumnSrid, String> {
    let row = client
        .query_opt(
            "SELECT srid FROM geometry_columns \
             WHERE f_table_schema = $1 AND f_table_name = $2 AND f_geometry_column = $3",
            &[&schema, &table, &geometry_column],
        )
        .await
        .map_err(|error| query_error("read the geometry SRID", &error))?
        .ok_or_else(|| {
            format!("The spatial table {schema}.{table} ({geometry_column}) was not found.")
        })?;
    let srid: i32 = row.get(0);
    if srid > 0 {
        return Ok(ColumnSrid::Declared(srid));
    }
    // A plain `geometry` column (no typmod) registers as SRID 0 even when
    // every row carries one, which is common for tables loaded by GIS tools.
    // Sample a row: if it has an SRID, the rows are tagged and need no default.
    let sql = format!(
        "SELECT ST_SRID({column}::geometry) FROM {schema_ident}.{table_ident} \
         WHERE {column} IS NOT NULL LIMIT 1",
        column = quote_ident(geometry_column),
        schema_ident = quote_ident(schema),
        table_ident = quote_ident(table),
    );
    let sampled: Option<i32> = client
        .query_opt(&sql, &[])
        .await
        .map_err(|error| query_error("read the geometry SRID", &error))?
        .map(|row| row.get(0));
    if let Some(sampled) = sampled.filter(|srid| *srid > 0) {
        return Ok(ColumnSrid::Declared(sampled));
    }
    entry.default_srid.map(ColumnSrid::Assumed).ok_or_else(|| {
        format!(
            "{schema}.{table} has no SRID. Enter a default SRID (for example 4326) and connect again."
        )
    })
}

/// A single-column integer primary key, used as the MVT feature id so the map
/// can address features (hover/selection state) across tiles.
async fn read_integer_primary_key(
    client: &deadpool_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<Option<String>, String> {
    let rows = client
        .query(
            "SELECT a.attname::text, t.typname::text \
             FROM pg_index i \
             JOIN pg_class c ON c.oid = i.indrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey) \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2",
            &[&schema, &table],
        )
        .await
        .map_err(|error| query_error("read the primary key", &error))?;
    if rows.len() != 1 {
        return Ok(None);
    }
    let type_name: String = rows[0].get(1);
    Ok(matches!(type_name.as_str(), "int2" | "int4" | "int8").then(|| rows[0].get(0)))
}

/// How the connection string's `sslmode` maps onto rustls.
///
/// tokio-postgres only parses `disable`/`prefer`/`require`, and with a rustls
/// connector it would always verify the certificate. libpq semantics differ:
/// `prefer`/`require` encrypt without verifying, and only `verify-ca`/
/// `verify-full` check the certificate. That matters in practice: hosted
/// PostgreSQL often serves a private-CA certificate with `sslmode=require`.
struct TlsRequirement {
    normalized_connection: String,
    verify: bool,
}

impl TlsRequirement {
    fn from_connection_string(connection: &str) -> Self {
        let mut normalized = connection.to_string();
        let mut verify = false;
        for mode in ["verify-full", "verify-ca"] {
            let needle = format!("sslmode={mode}");
            if normalized.contains(&needle) {
                normalized = normalized.replace(&needle, "sslmode=require");
                verify = true;
            }
        }
        Self {
            normalized_connection: normalized,
            verify,
        }
    }

    fn connector(&self) -> Result<MakeRustlsConnect, String> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .map_err(|error| format!("Could not configure TLS: {error}"))?;
        let config = if self.verify {
            // verify-ca is treated as verify-full: rustls always checks the
            // host name, and being stricter than asked fails safe.
            let roots = RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            builder.with_root_certificates(roots).with_no_client_auth()
        } else {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(EncryptOnlyVerifier { provider }))
                .with_no_client_auth()
        };
        Ok(MakeRustlsConnect::new(config))
    }
}

/// libpq `sslmode=require`: the channel is encrypted and the handshake
/// signatures are checked, but the certificate chain and host name are not.
#[derive(Debug)]
struct EncryptOnlyVerifier {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for EncryptOnlyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn parse_default_srid(value: Option<&str>) -> Result<Option<i32>, String> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some(value) => value
            .parse::<i32>()
            .ok()
            .filter(|srid| *srid > 0)
            .map(Some)
            .ok_or_else(|| "The default SRID must be a positive integer.".to_string()),
    }
}

fn validate_tile(z: u32, x: u32, y: u32) -> Result<(), String> {
    if z > MAX_ZOOM {
        return Err(format!("Tile zoom {z} is out of range."));
    }
    let tiles = 1_u64 << z;
    if u64::from(x) >= tiles || u64::from(y) >= tiles {
        return Err(format!("Tile {z}/{x}/{y} is out of range."));
    }
    Ok(())
}

/// Double-quote an SQL identifier (PostgreSQL `quote_ident` semantics).
fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Single-quote an SQL string literal.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn clamp(value: f64, min: f64, max: f64) -> f64 {
    value.max(min).min(max)
}

fn pool_error(error: deadpool_postgres::PoolError) -> String {
    match error {
        deadpool_postgres::PoolError::Backend(error) => connect_error(&error),
        deadpool_postgres::PoolError::Timeout(_) => {
            "Timed out waiting for a PostgreSQL connection.".to_string()
        }
        other => format!("Could not get a PostgreSQL connection: {other}"),
    }
}

fn connect_error(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("Could not connect to PostgreSQL: {}", db.message()),
        None => format!("Could not connect to PostgreSQL: {error}"),
    }
}

fn query_error(action: &str, error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("Could not {action}: {}", db.message()),
        None => format!("Could not {action}: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers_and_literals() {
        assert_eq!(quote_ident("roads"), "\"roads\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        assert_eq!(quote_literal("o'brien"), "'o''brien'");
    }

    #[test]
    fn maps_libpq_sslmodes() {
        let require = TlsRequirement::from_connection_string("postgres://h/db?sslmode=require");
        assert!(!require.verify);
        let full = TlsRequirement::from_connection_string("host=h sslmode=verify-full");
        assert!(full.verify);
        assert_eq!(full.normalized_connection, "host=h sslmode=require");
        let ca = TlsRequirement::from_connection_string("postgres://h/db?sslmode=verify-ca");
        assert!(ca.verify);
        assert!(tokio_postgres::Config::from_str(&ca.normalized_connection).is_ok());
    }

    fn key() -> SourceKey {
        SourceKey {
            schema: "public".into(),
            table: "roads".into(),
            geometry_column: "geom".into(),
        }
    }

    fn columns(names: &[&str]) -> Vec<ColumnInfo> {
        names
            .iter()
            .map(|name| ColumnInfo {
                name: (*name).into(),
                type_name: "text".into(),
            })
            .collect()
    }

    #[test]
    fn builds_tile_sql_for_each_srid_kind() {
        let sql = build_tile_sql(
            &key(),
            ColumnSrid::Declared(4326),
            &columns(&["id", "name"]),
            Some("id"),
            None,
        );
        assert!(sql.contains("ST_AsMVT(tile, 'roads', 4096, '__geolibre_mvt_geom', 'id')"));
        assert!(sql.contains("__geolibre_src.\"id\", __geolibre_src.\"name\", ST_AsMVTGeom("));
        assert!(sql.contains(
            "FROM \"public\".\"roads\" AS __geolibre_src WHERE __geolibre_src.\"geom\" && ST_Transform("
        ));
        assert!(!sql.contains("  "), "continuation whitespace leaked: {sql}");

        let mercator = build_tile_sql(
            &key(),
            ColumnSrid::Declared(3857),
            &[],
            None,
            Some([0.0; 4]),
        );
        assert!(mercator.contains("'__geolibre_mvt_geom')"));
        // No clip for a Web Mercator column: the envelope needs no transform.
        assert!(mercator.contains("&& ST_Expand(ST_TileEnvelope($1, $2, $3), $4)"));
        assert!(!mercator.contains("ST_MakeEnvelope"));

        let assumed = build_tile_sql(
            &key(),
            ColumnSrid::Assumed(4326),
            &[],
            Some("missing"),
            None,
        );
        assert!(assumed.contains("ST_SetSRID(__geolibre_src.\"geom\"::geometry, 4326)"));
        assert!(assumed.contains("ST_SetSRID(ST_Transform("));
        assert!(!assumed.contains("'missing'"));
    }

    #[test]
    fn clips_projected_envelopes_to_the_data() {
        let sql = build_tile_sql(
            &key(),
            ColumnSrid::Declared(32748),
            &[],
            None,
            Some([1.5, -2.0, 3.0, 4.0]),
        );
        assert!(sql.contains("ST_MakeEnvelope(1.5, -2, 3, 4, 3857)"));
        assert!(sql.contains("ST_Transform(ST_Segmentize(c, "));
        assert!(sql.contains("WHERE __geolibre_src.\"geom\" && __geolibre_filter.f"));
        assert!(!sql.contains("  "), "continuation whitespace leaked: {sql}");
    }

    #[test]
    fn converts_mercator_to_lon_lat() {
        let (lon, lat) = mercator_to_lon_lat(0.0, 0.0);
        assert!(lon.abs() < 1e-9 && lat.abs() < 1e-9);
        let (lon, lat) = mercator_to_lon_lat(WEB_MERCATOR_HALF, WEB_MERCATOR_HALF);
        assert!((lon - 180.0).abs() < 1e-6);
        assert!((lat - 85.051_129).abs() < 1e-4);
    }

    #[test]
    fn validates_tile_coordinates() {
        assert!(validate_tile(0, 0, 0).is_ok());
        assert!(validate_tile(0, 1, 0).is_err());
        assert!(validate_tile(3, 7, 7).is_ok());
        assert!(validate_tile(3, 8, 0).is_err());
        assert!(validate_tile(31, 0, 0).is_err());
    }

    #[test]
    fn parses_default_srid() {
        assert_eq!(parse_default_srid(None), Ok(None));
        assert_eq!(parse_default_srid(Some(" ")), Ok(None));
        assert_eq!(parse_default_srid(Some("4326")), Ok(Some(4326)));
        assert!(parse_default_srid(Some("0")).is_err());
        assert!(parse_default_srid(Some("abc")).is_err());
    }
}
