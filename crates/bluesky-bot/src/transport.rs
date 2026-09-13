//! XRPC transport abstraction and default Reqwest implementation.

use std::collections::BTreeMap;

use async_trait::async_trait;
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::Value;
use thiserror::Error;
use url::Url;

/// XRPC HTTP method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Method {
    /// Idempotent query.
    Get,
    /// Mutating procedure.
    Post,
}

/// Transport-neutral XRPC request.
#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub(crate) method: Method,
    pub(crate) url: Url,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Option<RequestBody>,
}

/// Body supported by an XRPC request.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestBody {
    /// JSON input.
    Json(Value),
    /// Binary upload.
    Bytes {
        /// Media MIME type.
        content_type: String,
        /// Raw media bytes.
        data: Vec<u8>,
    },
}

impl Request {
    /// Request method.
    #[must_use]
    pub const fn method(&self) -> Method {
        self.method
    }
    /// Fully encoded request URL.
    #[must_use]
    pub const fn url(&self) -> &Url {
        &self.url
    }
    /// Request headers.
    #[must_use]
    pub const fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }
    /// JSON request body.
    #[must_use]
    pub const fn body(&self) -> Option<&RequestBody> {
        self.body.as_ref()
    }
}

/// Transport-neutral XRPC response.
#[derive(Clone, Debug, PartialEq)]
pub struct Response {
    /// HTTP status code.
    pub status: u16,
    /// Decoded response body, or `null` for an empty response.
    pub body: Value,
}

/// Failure before an XRPC response could be obtained.
#[derive(Debug, Error)]
#[error("transport request failed: {0}")]
pub struct TransportError(pub String);

/// Pluggable request executor used by [`crate::Bot`].
#[async_trait]
pub trait Transport: Send + Sync {
    /// Executes one request.
    async fn execute(&self, request: Request) -> Result<Response, TransportError>;
}

/// Rustls-backed Reqwest transport.
#[derive(Clone, Debug, Default)]
pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    /// Creates a transport with a caller-configured client.
    #[must_use]
    pub const fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn execute(&self, request: Request) -> Result<Response, TransportError> {
        let mut builder = match request.method {
            Method::Get => self.client.get(request.url),
            Method::Post => self.client.post(request.url),
        };
        for (name, value) in request.headers {
            let name =
                HeaderName::try_from(name).map_err(|error| TransportError(error.to_string()))?;
            let value =
                HeaderValue::try_from(value).map_err(|error| TransportError(error.to_string()))?;
            builder = builder.header(name, value);
        }
        if let Some(body) = request.body {
            builder = match body {
                RequestBody::Json(value) => builder.json(&value),
                RequestBody::Bytes { content_type, data } => {
                    builder.header("content-type", content_type).body(data)
                }
            };
        }
        let response = builder
            .send()
            .await
            .map_err(|error| TransportError(error.to_string()))?;
        let status = response.status().as_u16();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| TransportError(error.to_string()))?;
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).map_err(|error| TransportError(error.to_string()))?
        };
        Ok(Response { status, body })
    }
}
