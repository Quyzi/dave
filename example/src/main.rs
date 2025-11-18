use actix_web::{
    App, HttpRequest, HttpResponse, HttpServer, Responder, get, post, rt::time::sleep, web,
};
use dave::{
    Metric, MetricType, builder::DaveBuilder, counter, explain, histogram, recorder::MetricRecorder,
};
use std::time::{Duration, Instant};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().init();

    DaveBuilder::default()
        .metric_freshness("http_requests", Duration::from_secs(2 * 60))
        // Configure custom histogram buckets for request duration
        .metric_histogram_buckets(
            "http_request_duration_seconds",
            vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )
        .metric_description("http_requests", "the total number of http requests")
        .build_install();

    explain!(histogram "http_request_duration_seconds" => "http request duration in seconds");
    // Can also set freshness and histogram buckets at runtime after initialization
    MetricRecorder::set_metric_freshness("other_metric".to_string(), Duration::from_secs(10 * 60));
    MetricRecorder::set_metric_histogram_buckets(
        "some_histogram".to_string(),
        vec![1.0, 5.0, 10.0],
    );

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
    let start = Instant::now();
    sleep(Duration::from_secs(1)).await;
    counter!(
        "http_requests",
        "method" => req.method(),
        "path" => req.path(),
        "fn" => "hello"
    )
    .increment(1.1);

    let response = HttpResponse::Ok().body("Hello world!");

    // Record request latency in histogram
    let latency = start.elapsed().as_secs_f64();
    histogram!(
        "http_request_duration_seconds",
        "method" => req.method(),
        "path" => req.path(),
        "fn" => "hello"
    )
    .observe(latency);

    response
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
