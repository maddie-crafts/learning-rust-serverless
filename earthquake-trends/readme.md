# Earthquake Trends API (Rust + AWS Lambda + CDK)

A serverless API built in Rust that allows users to query **recent earthquake data** using HTTP GET requests. This project uses **AWS Lambda**, **API Gateway**, and is deployed via the **AWS CDK (TypeScript)**. The Lambda is instrumented with **Datadog** for full observability.

---

## What It Does

- Queries recent earthquakes from the USGS data feed
- Filters earthquakes by timestamp (`from_ms`) and limits (`n_results`)
- Returns structured JSON containing:
  - `time` (UTC timestamp)
  - `latitude`, `longitude`
  - `depth_km`, `magnitude`
  - `location` description

---

## API Usage

### `GET /?from_ms=<timestamp>&n_results=<limit>`

| Parameter   | Required | Description                             |
|-------------|----------|-----------------------------------------|
| `from_ms`   | No       | Unix timestamp in milliseconds (default: 24h ago) |
| `n_results` | No       | Max number of results to return (default: 100) |
