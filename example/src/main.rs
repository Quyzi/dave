use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, get, post, web};
use std::{num::NonZeroUsize, time::Duration};

use metrics::{
    Metric, counter,
    recorder::{FreshnessConfig, MetricRecorder},
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().init();

    // Initialize with default freshness config (5 min default, 30 sec scan interval)
    let mut freshness_config = FreshnessConfig::default();

    // Optionally set custom freshness for specific metrics before initialization
    freshness_config.per_metric_durations.insert(
        "http_requests".to_string().into(),
        Duration::from_secs(2 * 60),
    );

    MetricRecorder::initialize(NonZeroUsize::new(16).unwrap(), 64, freshness_config);

    // Can also set freshness at runtime after initialization
    MetricRecorder::set_metric_freshness("other_metric".to_string(), Duration::from_secs(10 * 60));

    HttpServer::new(|| {
        App::new()
            .service(hello)
            .service(echo)
            .service(render)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

#[get("/")]
async fn hello(req: HttpRequest) -> impl Responder {
    counter!(
        "http_requests",
        "method" => req.method(),
        "path" => req.path(),
        "fn" => "hello"
    )
    .increment(1.1);
    HttpResponse::Ok().body("Hello world!")
}

#[post("/echo")]
async fn echo(req: HttpRequest, req_body: String) -> impl Responder {
    counter!(
        "http_requests",
        "method" => req.method(),
        "path" => req.path(),
        "fn" => "echo",
        "echo" => req_body
    )
    .increment(1.0);
    HttpResponse::Ok().body(req_body)
}

async fn manual_hello(req: HttpRequest) -> impl Responder {
    counter!(
        "http_requests",
        "method" => req.method(),
        "path" => req.path(),
        "fn" => "manual_hello"
    )
    .increment(1.0);
    HttpResponse::Ok().body("Hey there!")
}

#[get("/render")]
async fn render(req: HttpRequest) -> impl Responder {
    counter!(
        "http_requests",
        "method" => req.method(),
        "path" => req.path(),
        "fn" => "render"
    )
    .increment(1.0);

    MetricRecorder::render_prometheus_string()
}
