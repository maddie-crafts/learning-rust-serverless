use chrono::{DateTime, Utc};
use lambda_http::tracing::{error, info};
use polars::prelude::*;
use rand::Rng;
use serde::Serialize;

/// Get the current unix timestamp in milliseconds
pub fn get_unix_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[derive(Debug, Serialize, Clone)]
pub struct Earthquake {
    time: DateTime<Utc>,
    latitude: f64,
    longitude: f64,
    depth_km: f64,
    magnitude: f64,
    location: String,
}

/// Returns fake earthquakes for testing or fallback mode
async fn get_fake_earthquakes(from_ms: i64, n_results: i64) -> anyhow::Result<Vec<Earthquake>> {
    let mut rng = rand::thread_rng();

    let quakes = (0..n_results)
        .map(|i| {
            let timestamp = DateTime::<Utc>::from_timestamp_millis(from_ms + i * 60_000).unwrap();
            Earthquake {
                time: timestamp,
                latitude: rng.gen_range(-90.0..90.0),
                longitude: rng.gen_range(-180.0..180.0),
                depth_km: rng.gen_range(1.0..700.0),
                magnitude: rng.gen_range(1.0..7.5),
                location: "Somewhere".to_string(),
            }
        })
        .collect();

    Ok(quakes)
}

/// Gets real earthquake data or falls back to fake data
pub async fn get_earthquakes(from_ms: i64, n_results: i64) -> anyhow::Result<Vec<Earthquake>> {
    let file_path = download_parquet_file().await?;
    get_earthquakes_from_file(&file_path, from_ms, n_results)
}

/// Downloads and caches the USGS all-month earthquake dataset
async fn download_parquet_file() -> anyhow::Result<String> {
    let csv_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv";
    let csv_path = "/tmp/earthquakes.csv";
    let parquet_path = "/tmp/earthquakes.parquet";

    if tokio::fs::try_exists(parquet_path).await? {
        info!("Using cached parquet file: {}", parquet_path);
        return Ok(parquet_path.to_string());
    }

    info!("Downloading USGS CSV from {}", csv_url);
    let response = reqwest::get(csv_url).await?;
    let bytes = response.bytes().await?;
    tokio::fs::write(csv_path, bytes).await?;

    info!("Converting CSV to Parquet...");
    let df = CsvReader::from_path(csv_path)?
        .infer_schema(Some(100))
        .has_header(true)
        .finish()?;

    ParquetWriter::new(parquet_path.into()).finish(&df)?;

    Ok(parquet_path.to_string())
}

fn get_earthquakes_from_file(
    file_path: &str,
    from_ms: i64,
    n_results: i64,
) -> anyhow::Result<Vec<Earthquake>> {
    let df = LazyFrame::scan_parquet(file_path, Default::default())?
        .select([
            col("time"),
            col("latitude"),
            col("longitude"),
            col("depth"),
            col("mag"),
            col("place"),
        ])
        .filter(col("time").gt_eq(lit(from_ms as i64)))
        .sort("time", Default::default())
        .limit(n_results as u32)
        .collect()?;

    let time = df.column("time")?.utf8()?.into_iter();
    let lat = df.column("latitude")?.f64()?.into_iter();
    let lon = df.column("longitude")?.f64()?.into_iter();
    let depth = df.column("depth")?.f64()?.into_iter();
    let mag = df.column("mag")?.f64()?.into_iter();
    let place = df.column("place")?.utf8()?.into_iter();

    let quakes = itertools::izip!(time, lat, lon, depth, mag, place)
        .filter_map(|(t, lat, lon, d, m, p)| {
            Some(Earthquake {
                time: DateTime::parse_from_rfc3339(t?).ok()?.with_timezone(&Utc),
                latitude: lat?,
                longitude: lon?,
                depth_km: d?,
                magnitude: m?,
                location: p?.to_string(),
            })
        })
        .collect();

    Ok(quakes)
}