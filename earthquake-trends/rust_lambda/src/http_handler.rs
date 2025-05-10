use lambda_http::tracing::info;
use lambda_http::{Body, Error, Request, RequestExt, Response};

use lambda_api::{get_earthquakes, get_unix_timestamp_ms};

/// This is the main handler for AWS Lambda.
/// It receives an HTTP event, extracts query parameters,
/// and returns earthquake data in JSON.
pub(crate) async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    // Extract query parameters from the request
    let query_params = event.query_string_parameters();

    // Default: last 24 hours
    let from_ms = query_params
        .first("from_ms")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or_else(|| get_unix_timestamp_ms() - 24 * 60 * 60 * 1000);

    let n_results = query_params
        .first("n_results")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(100);

    info!("Requesting earthquakes from {from_ms} with limit {n_results}");
    let quakes = get_earthquakes(from_ms, n_results).await?;
    let body = serde_json::to_string(&quakes)?;

    // Return the response with 200 OK and JSON content type
    let response = Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(body.into())
        .map_err(Box::new)?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lambda_http::{Request, RequestExt};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_success_response() {
        let mut query_string_parameters = HashMap::new();
        query_string_parameters.insert("from_ms".to_string(), "1719783621000".to_string());
        query_string_parameters.insert("n_results".to_string(), "5".to_string());

        let request = Request::default().with_query_string_parameters(query_string_parameters);

        let response = function_handler(request).await.unwrap();

        assert_eq!(response.status(), 200);

        let body_str = response.body().to_string();
        assert!(body_str.contains("latitude"));
    }
}
