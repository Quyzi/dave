use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, get, post, web};
use metrics::{Metric, builder::DaveBuilder, counter, recorder::MetricRecorder};
use std::time::Duration;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().init();

    DaveBuilder::default()
        .metric_freshness("http_requests", Duration::from_secs(2 * 60))
        .build_install();

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
