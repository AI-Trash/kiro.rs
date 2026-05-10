//! Prompt-cache simulator used by the Anthropic-compatible messages endpoints.
//!
//! This mirrors the standalone `kiro-rs-cache-simulator` behavior, but runs
//! in-process so clients do not need a second proxy hop.

use axum::http::{HeaderMap, header};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

use super::stream::SseEvent;

const DEFAULT_TTL_SECS: u64 = 5 * 60;
const EXTENDED_TTL_SECS: u64 = 60 * 60;

#[derive(Clone, Default)]
pub struct PromptCacheSimulator {
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    tokens: i32,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct CacheBreakpoint {
    hash: String,
    tokens: i32,
    ttl_secs: u64,
}

#[derive(Debug, Clone)]
pub struct CachePlan {
    api_key: String,
    breakpoints: Vec<CacheBreakpoint>,
    total_input_tokens: i32,
}

#[derive(Debug, Clone, Default)]
pub struct CacheResult {
    pub(crate) cache_read_input_tokens: i32,
    pub(crate) cache_creation_input_tokens: i32,
    pub(crate) uncached_input_tokens: i32,
}

impl PromptCacheSimulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn plan_from_request(&self, headers: &HeaderMap, request: &Value) -> Option<CachePlan> {
        if !is_json_request(headers) || is_web_search_request(request) {
            return None;
        }

        let breakpoints = compute_cache_breakpoints(request);
        let total_input_tokens = count_all_tokens(request).max(1);
        Some(CachePlan {
            api_key: extract_api_key(headers),
            breakpoints,
            total_input_tokens,
        })
    }

    pub async fn lookup_or_create(&self, plan: Option<CachePlan>) -> Option<CacheResult> {
        let plan = plan?;
        let result = lookup_or_create(
            &self.cache,
            &plan.api_key,
            &plan.breakpoints,
            plan.total_input_tokens,
        )
        .await;

        tracing::info!(
            breakpoints = plan.breakpoints.len(),
            cache_read_input_tokens = result.cache_read_input_tokens,
            cache_creation_input_tokens = result.cache_creation_input_tokens,
            uncached_input_tokens = result.uncached_input_tokens,
            "prompt-cache simulator applied"
        );

        Some(result)
    }
}

fn is_json_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains("application/json"))
        .unwrap_or(true)
}

fn is_web_search_request(request: &Value) -> bool {
    let Some(tools) = request.get("tools").and_then(Value::as_array) else {
        return false;
    };
    if tools.len() != 1 {
        return false;
    }
    let tool = &tools[0];
    let name = tool.get("name").and_then(Value::as_str).unwrap_or_default();
    let tool_type = tool.get("type").and_then(Value::as_str).unwrap_or_default();
    name == "web_search" || tool_type.starts_with("web_search")
}

async fn lookup_or_create(
    cache: &Arc<Mutex<HashMap<String, CacheEntry>>>,
    api_key: &str,
    breakpoints: &[CacheBreakpoint],
    total_input_tokens: i32,
) -> CacheResult {
    if breakpoints.is_empty() {
        return CacheResult {
            uncached_input_tokens: total_input_tokens,
            ..CacheResult::default()
        };
    }

    let mut cache = cache.lock().await;
    let now = Instant::now();
    cache.retain(|_, entry| entry.expires_at > now);

    let mut result = CacheResult::default();
    let mut hit_index = None;

    for (index, breakpoint) in breakpoints.iter().enumerate().rev() {
        let key = cache_key(api_key, &breakpoint.hash);
        if let Some(entry) = cache.get_mut(&key) {
            entry.expires_at = now + Duration::from_secs(breakpoint.ttl_secs);
            result.cache_read_input_tokens = entry.tokens;
            hit_index = Some(index);
            break;
        }
    }

    if let Some(index) = hit_index {
        let mut previous_tokens = result.cache_read_input_tokens;
        for breakpoint in breakpoints.iter().skip(index + 1) {
            let additional_tokens = breakpoint.tokens - previous_tokens;
            cache.insert(
                cache_key(api_key, &breakpoint.hash),
                CacheEntry {
                    tokens: breakpoint.tokens,
                    expires_at: now + Duration::from_secs(breakpoint.ttl_secs),
                },
            );
            result.cache_creation_input_tokens += additional_tokens.max(0);
            previous_tokens = breakpoint.tokens;
        }
    } else {
        let mut previous_tokens = 0;
        for breakpoint in breakpoints {
            let additional_tokens = breakpoint.tokens - previous_tokens;
            cache.insert(
                cache_key(api_key, &breakpoint.hash),
                CacheEntry {
                    tokens: breakpoint.tokens,
                    expires_at: now + Duration::from_secs(breakpoint.ttl_secs),
                },
            );
            result.cache_creation_input_tokens += additional_tokens.max(0);
            previous_tokens = breakpoint.tokens;
        }
    }

    let cached_tokens = result.cache_read_input_tokens + result.cache_creation_input_tokens;
    result.uncached_input_tokens = (total_input_tokens - cached_tokens).max(0);
    result
}

fn cache_key(api_key: &str, hash: &str) -> String {
    format!("cache:{}:{}", api_key, hash)
}

fn extract_api_key(headers: &HeaderMap) -> String {
    if let Some(api_key) = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
    {
        return api_key.to_string();
    }

    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        let mut parts = value.split_whitespace();
        if let (Some(scheme), Some(token), None) = (parts.next(), parts.next(), parts.next()) {
            if scheme.eq_ignore_ascii_case("Bearer") {
                return token.to_string();
            }
        }
    }

    "anonymous".to_string()
}

fn compute_cache_breakpoints(request: &Value) -> Vec<CacheBreakpoint> {
    let mut hasher = Sha256::new();
    let mut breakpoints = Vec::new();
    let mut cumulative_tokens = 0;

    if let Some(tools) = request.get("tools").and_then(Value::as_array) {
        let mut sorted_tools: Vec<&Value> = tools.iter().collect();
        sorted_tools.sort_by(|left, right| tool_name(left).cmp(tool_name(right)));

        for tool in sorted_tools {
            let normalized = normalize_tool(tool);
            hasher.update(normalized.as_bytes());
            cumulative_tokens += count_tokens(&normalized);

            if let Some(cache_control) = tool.get("cache_control") {
                breakpoints.push(CacheBreakpoint {
                    hash: finalize_hash(&hasher),
                    tokens: cumulative_tokens,
                    ttl_secs: parse_ttl(cache_control),
                });
            }
        }
    }

    match request.get("system") {
        Some(Value::String(text)) => {
            hasher.update(text.as_bytes());
            cumulative_tokens += count_tokens(text);
        }
        Some(Value::Array(system_messages)) => {
            for message in system_messages {
                let text = message
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                hasher.update(text.as_bytes());
                cumulative_tokens += count_tokens(text);

                if let Some(cache_control) = message.get("cache_control") {
                    breakpoints.push(CacheBreakpoint {
                        hash: finalize_hash(&hasher),
                        tokens: cumulative_tokens,
                        ttl_secs: parse_ttl(cache_control),
                    });
                }
            }
        }
        _ => {}
    }

    if let Some(messages) = request.get("messages").and_then(Value::as_array) {
        for message in messages {
            match message.get("content") {
                Some(Value::Array(blocks)) => {
                    for block in blocks {
                        let block_json = serde_json::to_string(block).unwrap_or_default();
                        hasher.update(block_json.as_bytes());

                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            cumulative_tokens += count_tokens(text);
                        }

                        if let Some(cache_control) = block.get("cache_control") {
                            breakpoints.push(CacheBreakpoint {
                                hash: finalize_hash(&hasher),
                                tokens: cumulative_tokens,
                                ttl_secs: parse_ttl(cache_control),
                            });
                        }
                    }
                }
                Some(Value::String(text)) => {
                    hasher.update(text.as_bytes());
                    cumulative_tokens += count_tokens(text);
                }
                _ => {}
            }
        }
    }

    breakpoints
}

fn finalize_hash(hasher: &Sha256) -> String {
    format!("{:x}", hasher.clone().finalize())
}

fn parse_ttl(cache_control: &Value) -> u64 {
    match cache_control.get("ttl").and_then(Value::as_str) {
        Some("1h") => EXTENDED_TTL_SECS,
        _ => DEFAULT_TTL_SECS,
    }
}

fn normalize_tool(tool: &Value) -> String {
    let mut parts = Vec::new();
    parts.push(format!("name:{}", tool_name(tool)));

    if let Some(description) = tool.get("description").and_then(Value::as_str) {
        if !description.is_empty() {
            parts.push(format!("desc:{}", description));
        }
    }

    if let Some(input_schema) = tool.get("input_schema") {
        if !input_schema.as_object().map(Map::is_empty).unwrap_or(true) {
            let sorted = sort_json_value(input_schema);
            if let Ok(serialized) = serde_json::to_string(&sorted) {
                parts.push(format!("schema:{}", serialized));
            }
        }
    }

    parts.join("|")
}

fn tool_name(tool: &Value) -> &str {
    tool.get("name").and_then(Value::as_str).unwrap_or_default()
}

fn sort_json_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let sorted = keys
                .into_iter()
                .filter_map(|key| {
                    map.get(key)
                        .map(|value| (key.clone(), sort_json_value(value)))
                })
                .collect();
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.iter().map(sort_json_value).collect()),
        _ => value.clone(),
    }
}

fn count_all_tokens(request: &Value) -> i32 {
    let mut total = 0;

    match request.get("system") {
        Some(Value::String(text)) => total += count_tokens(text),
        Some(Value::Array(messages)) => {
            for message in messages {
                if let Some(text) = message.get("text").and_then(Value::as_str) {
                    total += count_tokens(text);
                }
            }
        }
        _ => {}
    }

    if let Some(messages) = request.get("messages").and_then(Value::as_array) {
        for message in messages {
            match message.get("content") {
                Some(Value::String(text)) => total += count_tokens(text),
                Some(Value::Array(blocks)) => {
                    for block in blocks {
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            total += count_tokens(text);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    if let Some(tools) = request.get("tools").and_then(Value::as_array) {
        for tool in tools {
            total += count_tokens(tool_name(tool));
            total += count_tokens(
                tool.get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
            if let Some(input_schema) = tool.get("input_schema") {
                total += count_tokens(&serde_json::to_string(input_schema).unwrap_or_default());
            }
        }
    }

    total.max(1)
}

fn count_tokens(text: &str) -> i32 {
    let char_units: f64 = text
        .chars()
        .map(|ch| if is_non_western_char(ch) { 4.0 } else { 1.0 })
        .sum();
    let tokens = char_units / 4.0;
    let adjusted = if tokens < 100.0 {
        tokens * 1.5
    } else if tokens < 200.0 {
        tokens * 1.3
    } else if tokens < 300.0 {
        tokens * 1.25
    } else if tokens < 800.0 {
        tokens * 1.2
    } else {
        tokens
    };
    adjusted as i32
}

fn is_non_western_char(ch: char) -> bool {
    !matches!(ch,
        '\u{0000}'..='\u{007F}' |
        '\u{0080}'..='\u{00FF}' |
        '\u{0100}'..='\u{024F}' |
        '\u{1E00}'..='\u{1EFF}' |
        '\u{2C60}'..='\u{2C7F}' |
        '\u{A720}'..='\u{A7FF}' |
        '\u{AB30}'..='\u{AB6F}'
    )
}

pub fn patch_json_response_usage(value: &mut Value, cache: &CacheResult) {
    if let Some(usage) = value.get_mut("usage").and_then(Value::as_object_mut) {
        patch_usage(usage, cache);
    }
}

pub fn patch_sse_events_usage(events: &mut [SseEvent], cache: &CacheResult) {
    for event in events {
        patch_sse_event(&mut event.data, cache);
    }
}

fn patch_sse_event(event: &mut Value, cache: &CacheResult) {
    match event.get("type").and_then(Value::as_str) {
        Some("message_start") => {
            if let Some(usage) = event
                .get_mut("message")
                .and_then(|message| message.get_mut("usage"))
                .and_then(Value::as_object_mut)
            {
                patch_usage(usage, cache);
            }
        }
        Some("message_delta") => {
            if let Some(usage) = event.get_mut("usage").and_then(Value::as_object_mut) {
                patch_usage(usage, cache);
            }
        }
        _ => {}
    }
}

fn patch_usage(usage: &mut Map<String, Value>, cache: &CacheResult) {
    usage.insert(
        "cache_creation_input_tokens".to_string(),
        Value::from(cache.cache_creation_input_tokens),
    );
    usage.insert(
        "cache_read_input_tokens".to_string(),
        Value::from(cache.cache_read_input_tokens),
    );

    let input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    if input_tokens == 0 {
        usage.insert(
            "input_tokens".to_string(),
            Value::from(cache.uncached_input_tokens),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn memory_cache_records_then_hits_breakpoint() {
        let request = json!({
            "system": [{"text": "You are helpful", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]
        });
        let simulator = PromptCacheSimulator::new();
        let plan = simulator
            .plan_from_request(&HeaderMap::new(), &request)
            .expect("cache plan should be prepared");

        let first = simulator
            .lookup_or_create(Some(plan.clone()))
            .await
            .expect("cache result");
        assert!(first.cache_creation_input_tokens > 0);
        assert_eq!(first.cache_read_input_tokens, 0);

        let second = simulator
            .lookup_or_create(Some(plan))
            .await
            .expect("cache result");
        assert_eq!(second.cache_creation_input_tokens, 0);
        assert!(second.cache_read_input_tokens > 0);
    }

    #[test]
    fn patch_json_usage_cache_fields() {
        let mut value = json!({"usage":{"input_tokens":0,"output_tokens":2}});
        let cache = CacheResult {
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 4,
            uncached_input_tokens: 5,
        };

        patch_json_response_usage(&mut value, &cache);

        assert_eq!(value["usage"]["cache_read_input_tokens"], 3);
        assert_eq!(value["usage"]["cache_creation_input_tokens"], 4);
        assert_eq!(value["usage"]["input_tokens"], 5);
    }

    #[test]
    fn patch_sse_message_start_usage_cache_fields() {
        let mut events = vec![SseEvent::new(
            "message_start",
            json!({
                "type": "message_start",
                "message": {"usage": {"input_tokens": 1}}
            }),
        )];
        let cache = CacheResult {
            cache_read_input_tokens: 7,
            cache_creation_input_tokens: 0,
            uncached_input_tokens: 1,
        };

        patch_sse_events_usage(&mut events, &cache);

        assert_eq!(
            events[0].data["message"]["usage"]["cache_read_input_tokens"],
            7
        );
        assert_eq!(
            events[0].data["message"]["usage"]["cache_creation_input_tokens"],
            0
        );
    }

    #[test]
    fn bearer_api_key_extraction_is_case_and_whitespace_tolerant() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_static("bearer    sk-lowercase"),
        );

        assert_eq!(extract_api_key(&headers), "sk-lowercase");
    }
}
