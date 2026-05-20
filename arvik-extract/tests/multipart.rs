use arvik_core::{Body, FromRequest, IntoResponse, Request};
use arvik_extract::rejection::MultipartRejection;
use arvik_extract::{
    Multipart, MultipartConfig, MultipartConstraints, MultipartError, ProgressChunk,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use http::StatusCode;

const BOUNDARY: &str = "ARVIK-BOUNDARY";

fn content_type() -> String {
    format!("multipart/form-data; boundary={BOUNDARY}")
}

fn request(body: impl Into<Body>, content_type: impl AsRef<str>) -> Request {
    let req = http::Request::builder()
        .method("POST")
        .uri("/upload")
        .header(http::header::CONTENT_TYPE, content_type.as_ref())
        .body(body.into())
        .unwrap();
    Request::new(req)
}

fn request_with_length(body: impl Into<Body>, content_type: impl AsRef<str>, len: u64) -> Request {
    let req = http::Request::builder()
        .method("POST")
        .uri("/upload")
        .header(http::header::CONTENT_TYPE, content_type.as_ref())
        .header(http::header::CONTENT_LENGTH, len.to_string())
        .body(body.into())
        .unwrap();
    Request::new(req)
}

fn stream_request(chunks: Vec<Bytes>, content_type: impl AsRef<str>) -> Request {
    let stream = stream::iter(chunks.into_iter().map(Ok::<Bytes, std::io::Error>));
    request(Body::from_stream(stream), content_type)
}

fn text_field(name: &str, value: &str) -> String {
    format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n")
}

fn file_field_prefix(name: &str, file_name: &str, content_type: &str) -> String {
    format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{file_name}\"\r\nContent-Type: {content_type}\r\n\r\n"
    )
}

fn end_boundary() -> String {
    format!("\r\n--{BOUNDARY}--\r\n")
}

fn browser_form_body() -> Vec<u8> {
    let mut body = String::new();
    body.push_str(&text_field("title", "hello from browser"));
    body.push_str(&file_field_prefix("file", "hello.txt", "text/plain"));
    body.push_str("file bytes");
    body.push_str(&end_boundary());
    body.into_bytes()
}

async fn rejection_response(rejection: MultipartRejection) -> (StatusCode, String) {
    let response = rejection.into_response();
    let status = response.status();
    let body = response.into_body().to_string().await.unwrap();
    (status, body)
}

async fn next_field(multipart: &mut Multipart) -> arvik_extract::Field {
    multipart.next_field().await.unwrap().unwrap()
}

#[tokio::test]
async fn browser_style_form_upload_parses_text_and_file_fields() {
    let req = request(browser_form_body(), content_type());
    let mut multipart = Multipart::from_request_with_constraints(
        req,
        MultipartConstraints::new()
            .max_fields(4)
            .max_field_size(1024)
            .max_total_size(4096),
    )
    .await
    .unwrap();

    let title = next_field(&mut multipart).await;
    assert_eq!(title.name(), Some("title"));
    assert_eq!(title.text().await.unwrap(), "hello from browser");

    let file = next_field(&mut multipart).await;
    assert_eq!(file.name(), Some("file"));
    assert_eq!(file.file_name(), Some("hello.txt"));
    assert_eq!(file.content_type().unwrap().as_ref(), "text/plain");
    assert_eq!(
        file.bytes().await.unwrap(),
        Bytes::from_static(b"file bytes")
    );

    assert!(multipart.next_field().await.unwrap().is_none());
}

#[tokio::test]
async fn non_multipart_content_type_returns_unsupported_media_type() {
    let req = request("plain text", "text/plain");
    let err = Multipart::from_request_with_constraints(req, MultipartConstraints::new())
        .await
        .unwrap_err();

    assert!(matches!(err, MultipartRejection::InvalidContentType));
    let (status, body) = rejection_response(err).await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert!(body.contains("multipart/form-data"));
}

#[tokio::test]
async fn missing_boundary_returns_clear_rejection() {
    let req = request(browser_form_body(), "multipart/form-data");
    let err = Multipart::from_request_with_constraints(req, MultipartConstraints::new())
        .await
        .unwrap_err();

    assert!(matches!(err, MultipartRejection::MissingBoundary));
    let (status, body) = rejection_response(err).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("Missing multipart boundary"));
}

#[tokio::test]
async fn content_length_above_total_limit_rejects_before_body_is_read() {
    let req = request_with_length(Body::empty(), content_type(), 1024);
    let err = Multipart::from_request_with_constraints(
        req,
        MultipartConstraints::new().max_total_size(16),
    )
    .await
    .unwrap_err();

    assert!(matches!(err, MultipartRejection::PayloadTooLarge));
    let (status, body) = rejection_response(err).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(body.contains("maximum allowed size"));
}

#[tokio::test]
async fn streaming_total_size_limit_fails_while_reading_field() {
    let prefix = file_field_prefix("file", "large.txt", "text/plain");
    let data = "0123456789abcdef0123456789abcdef";
    let suffix = end_boundary();
    let req = stream_request(
        vec![
            Bytes::from(prefix.clone()),
            Bytes::from(data),
            Bytes::from(suffix),
        ],
        content_type(),
    );
    let mut multipart = Multipart::from_request_with_constraints(
        req,
        MultipartConstraints::new()
            .max_field_size(1024)
            .max_total_size(prefix.len() as u64 + 8),
    )
    .await
    .unwrap();

    let err = match multipart.next_field().await {
        Ok(Some(file)) => file.bytes().await.unwrap_err(),
        Err(err) => err,
        Ok(None) => panic!("expected file field"),
    };
    assert!(err.is_size_exceeded());
    assert_eq!(err.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn per_field_size_limit_fails_while_reading_field() {
    let prefix = file_field_prefix("file", "large.txt", "text/plain");
    let req = stream_request(
        vec![
            Bytes::from(prefix),
            Bytes::from_static(b"0123456789abcdef"),
            Bytes::from(end_boundary()),
        ],
        content_type(),
    );
    let mut multipart = Multipart::from_request_with_constraints(
        req,
        MultipartConstraints::new()
            .max_field_size(4)
            .max_total_size(4096),
    )
    .await
    .unwrap();

    let file = next_field(&mut multipart).await;
    let err = file.bytes().await.unwrap_err();
    assert!(err.is_size_exceeded());
    assert!(err.is_limit_exceeded());
}

#[tokio::test]
async fn max_fields_returns_typed_too_many_fields_error() {
    let mut body = String::new();
    body.push_str(&text_field("first", "one"));
    body.push_str(&text_field("second", "two"));
    body.push_str(&format!("--{BOUNDARY}--\r\n"));

    let req = request(body, content_type());
    let mut multipart = Multipart::from_request_with_constraints(
        req,
        MultipartConstraints::new()
            .max_fields(1)
            .max_field_size(1024)
            .max_total_size(4096),
    )
    .await
    .unwrap();

    let first = next_field(&mut multipart).await;
    assert_eq!(first.text().await.unwrap(), "one");

    let err = multipart.next_field().await.unwrap_err();
    assert!(matches!(err, MultipartError::TooManyFields { limit: 1 }));
    assert!(err.is_too_many_fields());
    assert_eq!(err.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn progress_stream_reports_monotonic_uploaded_bytes() {
    let prefix = file_field_prefix("file", "progress.txt", "text/plain");
    let req = stream_request(
        vec![
            Bytes::from(prefix),
            Bytes::from_static(b"abc"),
            Bytes::from_static(b"defgh"),
            Bytes::from(end_boundary()),
        ],
        content_type(),
    );
    let mut multipart = Multipart::from_request_with_constraints(req, MultipartConstraints::new())
        .await
        .unwrap();

    let file = next_field(&mut multipart).await;
    let mut progress = file.into_progress_stream();
    let mut previous = 0;
    let mut data = Vec::new();

    while let Some(chunk) = progress.next().await {
        let chunk: ProgressChunk = chunk.unwrap();
        assert!(chunk.bytes_read() > previous);
        previous = chunk.bytes_read();
        data.extend_from_slice(chunk.bytes());
    }

    assert_eq!(previous, 8);
    assert_eq!(data, b"abcdefgh");
}

#[tokio::test]
async fn save_to_temp_writes_file_and_removes_it_on_drop() {
    let req = request(browser_form_body(), content_type());
    let mut multipart = Multipart::from_request_with_constraints(req, MultipartConstraints::new())
        .await
        .unwrap();

    let title = next_field(&mut multipart).await;
    assert_eq!(title.text().await.unwrap(), "hello from browser");
    let file = next_field(&mut multipart).await;
    let temp = file.save_to_temp().await.unwrap();
    let path = temp.path().to_path_buf();

    assert_eq!(temp.bytes_written(), 10);
    assert_eq!(temp.metadata().file_name(), Some("hello.txt"));
    assert!(path.exists());
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"file bytes");

    drop(temp);
    assert!(!path.exists());
}

#[tokio::test]
async fn save_to_temp_in_honors_custom_directory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let req = request(browser_form_body(), content_type());
    let mut multipart = Multipart::from_request_with_constraints(req, MultipartConstraints::new())
        .await
        .unwrap();

    let title = next_field(&mut multipart).await;
    assert_eq!(title.text().await.unwrap(), "hello from browser");
    let file = next_field(&mut multipart).await;
    let temp = file.save_to_temp_in(temp_dir.path()).await.unwrap();

    assert!(temp.path().starts_with(temp_dir.path()));
    assert_eq!(tokio::fs::read(temp.path()).await.unwrap(), b"file bytes");
}

#[tokio::test]
async fn from_request_uses_config_extension_for_temp_directory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut req = request(browser_form_body(), content_type());
    req.extensions_mut().insert(
        MultipartConfig::new()
            .max_field_size(1024)
            .max_total_size(4096)
            .with_temp_dir(temp_dir.path().to_path_buf()),
    );

    let mut multipart = <Multipart as FromRequest<()>>::from_request(req, &())
        .await
        .unwrap();
    let title = next_field(&mut multipart).await;
    assert_eq!(title.text().await.unwrap(), "hello from browser");
    let file = next_field(&mut multipart).await;
    let temp = file.save_to_temp().await.unwrap();

    assert!(temp.path().starts_with(temp_dir.path()));
}
