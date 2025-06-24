use actix_web::{web, HttpResponse};
use serde::{Deserialize, Serialize};
use diesel::prelude::*;

#[derive(Deserialize, Serialize)]
struct BenchmarkResult {
    query: String,
    duration_ms: f64,
    datafusion_version: String,
}

// POST /results
async fn post_results(
    result: web::Json<BenchmarkResult>,
    pool: web::Data<DbPool>,
) -> HttpResponse {
    let conn = pool.get().expect("Couldn't get DB connection");
    
    diesel::insert_into(benchmark_results::table)
        .values(&result.0)
        .execute(&conn)
        .map(|_| HttpResponse::Created().finish())
        .unwrap_or_else(|_| HttpResponse::InternalServerError().finish())
}

// GET /results?version=23.0.0
async fn get_results(
    version: web::Query<String>,
    pool: web::Data<DbPool>,
) -> HttpResponse {
    let conn = pool.get().unwrap();
    let results = benchmark_results::table
        .filter(benchmark_results::datafusion_version.eq(&version.0))
        .load::<BenchmarkResult>(&conn)
        .expect("Error loading results");

    HttpResponse::Ok().json(results)
}
