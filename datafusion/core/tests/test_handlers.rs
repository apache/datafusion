#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;

    #[actix_rt::test]
    async fn test_post_results() {
        let pool = setup_test_db();
        let result = BenchmarkResult {
            query: "tpch_q1".to_string(),
            duration_ms: 452.3,
            datafusion_version: "23.0.0".to_string(),
        };

        let resp = post_results(web::Json(result), web::Data::new(pool)).await;
        assert_eq!(resp.status(), StatusCode::CREATED);
    }
}
