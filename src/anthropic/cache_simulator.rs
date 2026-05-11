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
const CACHE_LOOKBACK_BLOCKS: usize = 20;

#[derive(Clone)]
pub struct PromptCacheSimulator {
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    simulate_cache: bool,
    strip_cch: bool,
}

impl Default for PromptCacheSimulator {
    fn default() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            simulate_cache: true,
            strip_cch: true,
        }
    }
}

#[derive(Debug, Clone)]
struct CacheEntry {
    tokens: i32,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct CachePrefix {
    hash: String,
    tokens: i32,
}

#[derive(Debug, Clone)]
struct CacheBreakpoint {
    prefix_index: usize,
    ttl_secs: u64,
}

#[derive(Debug, Clone)]
pub struct CachePlan {
    api_key: String,
    prefixes: Vec<CachePrefix>,
    breakpoints: Vec<CacheBreakpoint>,
    total_input_tokens: i32,
}

#[derive(Debug, Clone, Default)]
struct CachePath {
    prefixes: Vec<CachePrefix>,
    breakpoints: Vec<CacheBreakpoint>,
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

    pub fn with_options(simulate_cache: bool, strip_cch: bool) -> Self {
        Self {
            simulate_cache,
            strip_cch,
            ..Self::default()
        }
    }

    pub fn strip_cch_from_request(&self, request: &mut Value) -> bool {
        self.strip_cch && strip_cch_from_request(request)
    }

    pub fn plan_from_request(&self, headers: &HeaderMap, request: &Value) -> Option<CachePlan> {
        if !self.simulate_cache || !is_json_request(headers) || is_web_search_request(request) {
            return None;
        }

        let mut cache_path = compute_cache_path(request);
        let minimum_cache_tokens = minimum_cache_tokens(request);
        cache_path.breakpoints.retain(|breakpoint| {
            cache_path.prefixes[breakpoint.prefix_index].tokens >= minimum_cache_tokens
        });
        cache_path.breakpoints.truncate(4);
        let total_input_tokens = count_all_tokens(request).max(1);
        Some(CachePlan {
            api_key: extract_api_key(headers),
            prefixes: cache_path.prefixes,
            breakpoints: cache_path.breakpoints,
            total_input_tokens,
        })
    }

    pub async fn lookup_or_create(&self, plan: Option<CachePlan>) -> Option<CacheResult> {
        let plan = plan?;
        let result = lookup_or_create(
            &self.cache,
            &plan.api_key,
            &plan.prefixes,
            &plan.breakpoints,
            plan.total_input_tokens,
        )
        .await;

        tracing::info!(
            prefixes = plan.prefixes.len(),
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
    prefixes: &[CachePrefix],
    breakpoints: &[CacheBreakpoint],
    total_input_tokens: i32,
) -> CacheResult {
    if prefixes.is_empty() || breakpoints.is_empty() {
        return CacheResult {
            uncached_input_tokens: total_input_tokens,
            ..CacheResult::default()
        };
    }

    let mut cache = cache.lock().await;
    let now = Instant::now();
    cache.retain(|_, entry| entry.expires_at > now);

    let mut result = CacheResult::default();
    let mut read_tokens = 0;

    'lookup: for breakpoint in breakpoints.iter().rev() {
        let start = breakpoint.prefix_index;
        let end = start.saturating_sub(CACHE_LOOKBACK_BLOCKS);
        for prefix_index in (end..=start).rev() {
            let prefix = &prefixes[prefix_index];
            let key = cache_key(api_key, &prefix.hash);
            if let Some(entry) = cache.get_mut(&key) {
                entry.expires_at = now + Duration::from_secs(breakpoint.ttl_secs);
                read_tokens = entry.tokens;
                break 'lookup;
            }
        }
    }
    result.cache_read_input_tokens = read_tokens;

    let mut largest_written_tokens = read_tokens;
    for breakpoint in breakpoints {
        let prefix = &prefixes[breakpoint.prefix_index];
        if prefix.tokens > read_tokens {
            cache.insert(
                cache_key(api_key, &prefix.hash),
                CacheEntry {
                    tokens: prefix.tokens,
                    expires_at: now + Duration::from_secs(breakpoint.ttl_secs),
                },
            );
            largest_written_tokens = largest_written_tokens.max(prefix.tokens);
        }
    }
    result.cache_creation_input_tokens = (largest_written_tokens - read_tokens).max(0);

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

fn compute_cache_path(request: &Value) -> CachePath {
    let mut hasher = Sha256::new();
    let mut path = CachePath::default();
    let mut cumulative_tokens = 0;

    if let Some(tools) = request.get("tools").and_then(Value::as_array) {
        let mut sorted_tools: Vec<&Value> = tools.iter().collect();
        sorted_tools.sort_by(|left, right| tool_name(left).cmp(tool_name(right)));

        for tool in sorted_tools {
            let normalized = normalize_tool(tool);
            let token_count = count_tokens(&normalized);
            push_prefix(
                &mut path,
                &mut hasher,
                &mut cumulative_tokens,
                &normalized,
                token_count,
            );

            if let Some(cache_control) = tool.get("cache_control") {
                push_breakpoint(&mut path, parse_ttl(cache_control));
            }
        }
    }

    match request.get("system") {
        Some(Value::String(text)) => {
            push_prefix(
                &mut path,
                &mut hasher,
                &mut cumulative_tokens,
                text,
                count_tokens(text),
            );
        }
        Some(Value::Array(system_messages)) => {
            for message in system_messages {
                let text = message
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                push_prefix(
                    &mut path,
                    &mut hasher,
                    &mut cumulative_tokens,
                    text,
                    count_tokens(text),
                );

                if let Some(cache_control) = message.get("cache_control") {
                    push_breakpoint(&mut path, parse_ttl(cache_control));
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
                        let block_json = serde_json::to_string(&block_cache_hash_material(block))
                            .unwrap_or_default();
                        let block_tokens = block
                            .get("text")
                            .and_then(Value::as_str)
                            .map(count_tokens)
                            .unwrap_or_default();
                        push_prefix(
                            &mut path,
                            &mut hasher,
                            &mut cumulative_tokens,
                            &block_json,
                            block_tokens,
                        );

                        if let Some(cache_control) = block.get("cache_control") {
                            push_breakpoint(&mut path, parse_ttl(cache_control));
                        }
                    }
                }
                Some(Value::String(text)) => {
                    push_prefix(
                        &mut path,
                        &mut hasher,
                        &mut cumulative_tokens,
                        text,
                        count_tokens(text),
                    );
                }
                _ => {}
            }
        }
    }

    if let Some(cache_control) = request.get("cache_control") {
        push_breakpoint(&mut path, parse_ttl(cache_control));
    }

    path
}

pub fn strip_cch_from_request(request: &mut Value) -> bool {
    match request.get_mut("system") {
        Some(Value::String(text)) => strip_cch_from_string(text),
        Some(Value::Array(system_messages)) => {
            let mut changed = false;
            for message in system_messages {
                if let Some(Value::String(text)) = message.get_mut("text") {
                    changed |= strip_cch_from_string(text);
                }
            }
            changed
        }
        _ => false,
    }
}

fn strip_cch_from_string(text: &mut String) -> bool {
    if !text.contains("x-anthropic-billing-header") {
        return false;
    }
    let stripped = strip_cch_segments(text);
    if stripped == *text {
        false
    } else {
        *text = stripped;
        true
    }
}

fn strip_cch_segments(text: &str) -> String {
    let mut changed = false;
    let segments = text
        .split(';')
        .filter(|segment| {
            let is_cch = segment.trim_start().starts_with("cch=");
            changed |= is_cch;
            !is_cch
        })
        .collect::<Vec<_>>();

    if changed {
        segments.join(";")
    } else {
        text.to_string()
    }
}

fn push_prefix(
    path: &mut CachePath,
    hasher: &mut Sha256,
    cumulative_tokens: &mut i32,
    hash_material: &str,
    token_count: i32,
) {
    hasher.update(hash_material.as_bytes());
    *cumulative_tokens += token_count;
    path.prefixes.push(CachePrefix {
        hash: finalize_hash(hasher),
        tokens: *cumulative_tokens,
    });
}

fn push_breakpoint(path: &mut CachePath, ttl_secs: u64) {
    let Some(prefix_index) = path.prefixes.len().checked_sub(1) else {
        return;
    };
    if path
        .breakpoints
        .iter()
        .any(|breakpoint| breakpoint.prefix_index == prefix_index)
    {
        return;
    }
    path.breakpoints.push(CacheBreakpoint {
        prefix_index,
        ttl_secs,
    });
}

fn block_cache_hash_material(block: &Value) -> Value {
    let Value::Object(map) = block else {
        return block.clone();
    };

    let mut hashable = map.clone();
    hashable.remove("cache_control");
    sort_json_value(&Value::Object(hashable))
}

fn minimum_cache_tokens(request: &Value) -> i32 {
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();

    if contains_any(
        &model,
        &[
            "opus-4-6",
            "opus-4.6",
            "opus-4-5",
            "opus-4.5",
            "opus-4-7",
            "opus-4.7",
            "haiku-4-5",
            "haiku-4.5",
        ],
    ) {
        4096
    } else if contains_any(&model, &["sonnet-4-6", "sonnet-4.6"]) {
        2048
    } else {
        1024
    }
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
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
    if usage_has_cache_fields(usage) {
        return;
    }

    usage.insert(
        "cache_creation_input_tokens".to_string(),
        Value::from(cache.cache_creation_input_tokens),
    );
    usage.insert(
        "cache_read_input_tokens".to_string(),
        Value::from(cache.cache_read_input_tokens),
    );

    usage.insert(
        "input_tokens".to_string(),
        Value::from(cache.uncached_input_tokens),
    );
}

fn usage_has_cache_fields(usage: &Map<String, Value>) -> bool {
    usage.contains_key("cache_creation_input_tokens")
        || usage.contains_key("cache_read_input_tokens")
        || usage.contains_key("cache_creation")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn memory_cache_records_then_hits_breakpoint() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "system": [{"text": long_text(), "cache_control": {"type": "ephemeral"}}],
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

    #[tokio::test]
    async fn lookback_reuses_prior_prefix_and_writes_extension() {
        let prefix = long_text();
        let extension = long_text();
        let first_request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": prefix, "cache_control": {"type": "ephemeral"}}]
            }]
        });
        let second_request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": prefix, "cache_control": {"type": "ephemeral"}},
                    {"type": "text", "text": extension, "cache_control": {"type": "ephemeral"}}
                ]
            }]
        });
        let simulator = PromptCacheSimulator::new();
        let first_plan = simulator
            .plan_from_request(&HeaderMap::new(), &first_request)
            .expect("cache plan should be prepared");
        let second_plan = simulator
            .plan_from_request(&HeaderMap::new(), &second_request)
            .expect("cache plan should be prepared");

        let first = simulator
            .lookup_or_create(Some(first_plan))
            .await
            .expect("cache result");
        assert!(first.cache_creation_input_tokens > 0);

        let second = simulator
            .lookup_or_create(Some(second_plan))
            .await
            .expect("cache result");
        assert!(second.cache_read_input_tokens > 0);
        assert!(second.cache_creation_input_tokens > 0);
        assert!(second.cache_creation_input_tokens < first.cache_creation_input_tokens * 2);
    }

    #[tokio::test]
    async fn cache_control_metadata_does_not_change_message_block_hash() {
        let prompt = long_text();
        let first_request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": prompt,
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });
        let second_request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": prompt,
                    "cache_control": {"type": "ephemeral", "ttl": "1h"}
                }]
            }]
        });
        let simulator = PromptCacheSimulator::new();

        let first_plan = simulator
            .plan_from_request(&HeaderMap::new(), &first_request)
            .expect("cache plan should be prepared");
        let second_plan = simulator
            .plan_from_request(&HeaderMap::new(), &second_request)
            .expect("cache plan should be prepared");
        let first = simulator
            .lookup_or_create(Some(first_plan))
            .await
            .expect("cache result");
        assert!(first.cache_creation_input_tokens > 0);

        let second = simulator
            .lookup_or_create(Some(second_plan))
            .await
            .expect("cache result");
        assert_eq!(second.cache_creation_input_tokens, 0);
        assert!(second.cache_read_input_tokens > 0);
    }

    #[tokio::test]
    async fn message_block_object_key_order_does_not_change_hash() {
        let prompt = long_text();
        let first_request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": prompt,
                    "cache_control": {"type": "ephemeral"}
                }]
            }]
        });
        let second_request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [{
                    "cache_control": {"type": "ephemeral"},
                    "text": prompt,
                    "type": "text"
                }]
            }]
        });
        let simulator = PromptCacheSimulator::new();

        let first_plan = simulator
            .plan_from_request(&HeaderMap::new(), &first_request)
            .expect("cache plan should be prepared");
        let second_plan = simulator
            .plan_from_request(&HeaderMap::new(), &second_request)
            .expect("cache plan should be prepared");
        let first = simulator
            .lookup_or_create(Some(first_plan))
            .await
            .expect("cache result");
        assert!(first.cache_creation_input_tokens > 0);

        let second = simulator
            .lookup_or_create(Some(second_plan))
            .await
            .expect("cache result");
        assert_eq!(second.cache_creation_input_tokens, 0);
        assert!(second.cache_read_input_tokens > 0);
    }

    #[test]
    fn short_prompts_do_not_create_breakpoints_under_minimum() {
        let request = json!({
            "model": "claude-sonnet-4-6",
            "system": [{"text": "short", "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": "hello"}]
        });
        let simulator = PromptCacheSimulator::new();
        let plan = simulator
            .plan_from_request(&HeaderMap::new(), &request)
            .expect("cache plan should be prepared");

        assert!(plan.breakpoints.is_empty());
    }

    #[test]
    fn strip_cch_removes_dynamic_segment_before_cache_plan() {
        let simulator = PromptCacheSimulator::new();
        let mut first_request = claude_code_cch_request("dee71");
        let mut second_request = claude_code_cch_request("460a0");

        assert!(simulator.strip_cch_from_request(&mut first_request));
        assert!(simulator.strip_cch_from_request(&mut second_request));

        let system_text = first_request["system"][0]["text"]
            .as_str()
            .expect("system text exists");
        assert!(system_text.contains("x-anthropic-billing-header"));
        assert!(!system_text.contains("cch="));

        let first_plan = simulator
            .plan_from_request(&HeaderMap::new(), &first_request)
            .expect("cache plan should be prepared");
        let second_plan = simulator
            .plan_from_request(&HeaderMap::new(), &second_request)
            .expect("cache plan should be prepared");
        let first_breakpoint = &first_plan.breakpoints[0];
        let second_breakpoint = &second_plan.breakpoints[0];

        assert_eq!(
            first_plan.prefixes[first_breakpoint.prefix_index].hash,
            second_plan.prefixes[second_breakpoint.prefix_index].hash
        );
    }

    #[test]
    fn patch_json_usage_cache_fields() {
        let mut value = json!({"usage":{"input_tokens":123,"output_tokens":2}});
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
    fn leaves_usage_unchanged_when_cache_fields_already_exist() {
        let mut value = json!({
            "usage": {
                "input_tokens": 11,
                "cache_read_input_tokens": 99,
                "output_tokens": 2
            }
        });
        let cache = CacheResult {
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 4,
            uncached_input_tokens: 5,
        };

        patch_json_response_usage(&mut value, &cache);

        assert_eq!(value["usage"]["cache_read_input_tokens"], 99);
        assert!(value["usage"].get("cache_creation_input_tokens").is_none());
        assert_eq!(value["usage"]["input_tokens"], 11);
    }

    #[test]
    fn disabled_simulator_skips_cache_plan_and_cch_strip() {
        let simulator = PromptCacheSimulator::with_options(false, false);
        let mut request = claude_code_cch_request("abc12");

        assert!(!simulator.strip_cch_from_request(&mut request));
        assert!(
            simulator
                .plan_from_request(&HeaderMap::new(), &request)
                .is_none()
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

    fn long_text() -> String {
        "cacheable prompt material ".repeat(240)
    }

    fn claude_code_cch_request(cch: &str) -> Value {
        json!({
            "model": "claude-opus-4-7",
            "system": [
                {"text": format!("x-anthropic-billing-header: cc_version=2.1.138.9e3; cc_entrypoint=sdk-cli; cch={cch};")},
                {"text": long_text().repeat(4), "cache_control": {"type": "ephemeral"}}
            ],
            "messages": [{"role": "user", "content": "hello"}]
        })
    }
}
