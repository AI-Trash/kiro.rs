use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TlsBackend {
    Rustls,
    NativeTls,
}

impl Default for TlsBackend {
    fn default() -> Self {
        Self::Rustls
    }
}

impl FromStr for TlsBackend {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "rustls" => Ok(Self::Rustls),
            "native-tls" | "native_tls" | "nativetls" => Ok(Self::NativeTls),
            value => anyhow::bail!("不支持的 TLS 后端: {value}"),
        }
    }
}

/// KNA 应用配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_region")]
    pub region: String,

    /// Auth Region（用于 Token 刷新），未配置时回退到 region
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_region: Option<String>,

    /// API Region（用于 API 请求），未配置时回退到 region
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_region: Option<String>,

    #[serde(default = "default_kiro_version")]
    pub kiro_version: String,

    #[serde(default)]
    pub machine_id: Option<String>,

    #[serde(default)]
    pub api_key: Option<String>,

    #[serde(default = "default_system_version")]
    pub system_version: String,

    #[serde(default = "default_node_version")]
    pub node_version: String,

    #[serde(default = "default_tls_backend")]
    pub tls_backend: TlsBackend,

    /// 外部 count_tokens API 地址（可选）
    #[serde(default)]
    pub count_tokens_api_url: Option<String>,

    /// count_tokens API 密钥（可选）
    #[serde(default)]
    pub count_tokens_api_key: Option<String>,

    /// count_tokens API 认证类型（可选，"x-api-key" 或 "bearer"，默认 "x-api-key"）
    #[serde(default = "default_count_tokens_auth_type")]
    pub count_tokens_auth_type: String,

    /// HTTP 代理地址（可选）
    /// 支持格式: http://host:port, https://host:port, socks5://host:port
    #[serde(default)]
    pub proxy_url: Option<String>,

    /// 代理认证用户名（可选）
    #[serde(default)]
    pub proxy_username: Option<String>,

    /// 代理认证密码（可选）
    #[serde(default)]
    pub proxy_password: Option<String>,

    /// Admin API 密钥（可选，启用 Admin API 功能）
    #[serde(default)]
    pub admin_api_key: Option<String>,

    /// 负载均衡模式（"priority" 或 "balanced"）
    #[serde(default = "default_load_balancing_mode")]
    pub load_balancing_mode: String,

    /// 是否开启非流式响应的 thinking 块提取（默认 true）
    ///
    /// 启用后，非流式响应中的 `<thinking>...</thinking>` 标签会被解析为
    /// 独立的 `{"type": "thinking", ...}` 内容块,与流式响应行为一致。
    #[serde(default = "default_extract_thinking")]
    pub extract_thinking: bool,

    /// 默认端点名称（凭据未显式指定 endpoint 时使用，默认 "ide"）
    #[serde(default = "default_endpoint")]
    pub default_endpoint: String,

    /// 端点特定的配置
    ///
    /// 键为端点名（如 "ide" / "cli"），值为该端点自由定义的参数对象。
    /// 未在此表出现的端点沿用实现内置默认值。
    #[serde(default)]
    pub endpoints: HashMap<String, serde_json::Value>,

    /// 配置文件路径（运行时元数据，不写入 JSON）
    #[serde(skip)]
    config_path: Option<PathBuf>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_kiro_version() -> String {
    "0.11.107".to_string()
}

fn default_system_version() -> String {
    const SYSTEM_VERSIONS: &[&str] = &["darwin#24.6.0", "win32#10.0.22631"];
    SYSTEM_VERSIONS[fastrand::usize(..SYSTEM_VERSIONS.len())].to_string()
}

fn default_node_version() -> String {
    "22.22.0".to_string()
}

fn default_count_tokens_auth_type() -> String {
    "x-api-key".to_string()
}

fn default_tls_backend() -> TlsBackend {
    TlsBackend::Rustls
}

fn default_load_balancing_mode() -> String {
    "priority".to_string()
}

fn default_extract_thinking() -> bool {
    true
}

fn default_endpoint() -> String {
    crate::kiro::endpoint::ide::IDE_ENDPOINT_NAME.to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            region: default_region(),
            auth_region: None,
            api_region: None,
            kiro_version: default_kiro_version(),
            machine_id: None,
            api_key: None,
            system_version: default_system_version(),
            node_version: default_node_version(),
            tls_backend: default_tls_backend(),
            count_tokens_api_url: None,
            count_tokens_api_key: None,
            count_tokens_auth_type: default_count_tokens_auth_type(),
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            admin_api_key: None,
            load_balancing_mode: default_load_balancing_mode(),
            extract_thinking: default_extract_thinking(),
            default_endpoint: default_endpoint(),
            endpoints: HashMap::new(),
            config_path: None,
        }
    }
}

impl Config {
    /// 获取默认配置文件路径
    pub fn default_config_path() -> &'static str {
        "config.json"
    }

    /// 加载运行时配置。
    ///
    /// 优先级：命令行 `-c/--config` > `KIRO_RS_CONFIG` > 可选默认配置文件 > 默认值；
    /// 最后再用 `KIRO_RS_*` 环境变量覆盖具体字段。
    pub fn load_runtime(config_path: Option<&str>) -> anyhow::Result<Self> {
        Self::load_runtime_with_env(config_path, |key| env::var(key).ok())
    }

    /// 获取有效的 Auth Region（用于 Token 刷新）
    /// 优先使用 auth_region，未配置时回退到 region
    pub fn effective_auth_region(&self) -> &str {
        self.auth_region.as_deref().unwrap_or(&self.region)
    }

    /// 获取有效的 API Region（用于 API 请求）
    /// 优先使用 api_region，未配置时回退到 region
    pub fn effective_api_region(&self) -> &str {
        self.api_region.as_deref().unwrap_or(&self.region)
    }

    /// 从文件加载配置
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            // 配置文件不存在，返回默认配置
            let mut config = Self::default();
            config.config_path = Some(path.to_path_buf());
            return Ok(config);
        }

        let content = fs::read_to_string(path)?;
        let mut config: Config = serde_json::from_str(&content)?;
        config.config_path = Some(path.to_path_buf());
        Ok(config)
    }

    fn load_runtime_with_env<F>(config_path: Option<&str>, mut get_env: F) -> anyhow::Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let env_config_path = if config_path.is_none() {
            read_non_empty_env(&mut get_env, "KIRO_RS_CONFIG")
        } else {
            None
        };

        let mut config = if let Some(path) = config_path.or(env_config_path.as_deref()) {
            Self::load(path).with_context(|| format!("加载配置文件失败: {}", path))?
        } else if let Some(path) = Self::discover_default_config_path() {
            Self::load(&path).with_context(|| format!("加载配置文件失败: {}", path.display()))?
        } else {
            Self::default()
        };

        config.apply_env_overrides_from(&mut get_env)?;
        Ok(config)
    }

    fn discover_default_config_path() -> Option<PathBuf> {
        [Self::default_config_path(), "config/config.json"]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| path.exists())
    }

    fn apply_env_overrides_from<F>(&mut self, get_env: &mut F) -> anyhow::Result<()>
    where
        F: FnMut(&str) -> Option<String>,
    {
        override_string(&mut self.host, get_env, "KIRO_RS_HOST")?;
        override_parsed(&mut self.port, get_env, "KIRO_RS_PORT")?;
        override_string(&mut self.region, get_env, "KIRO_RS_REGION")?;
        override_optional_string(&mut self.auth_region, get_env, "KIRO_RS_AUTH_REGION");
        override_optional_string(&mut self.api_region, get_env, "KIRO_RS_API_REGION");
        override_string(&mut self.kiro_version, get_env, "KIRO_RS_KIRO_VERSION")?;
        override_optional_string(&mut self.machine_id, get_env, "KIRO_RS_MACHINE_ID");
        override_optional_string(&mut self.api_key, get_env, "KIRO_RS_API_KEY");
        override_string(&mut self.system_version, get_env, "KIRO_RS_SYSTEM_VERSION")?;
        override_string(&mut self.node_version, get_env, "KIRO_RS_NODE_VERSION")?;
        override_parsed(&mut self.tls_backend, get_env, "KIRO_RS_TLS_BACKEND")?;
        override_optional_string(
            &mut self.count_tokens_api_url,
            get_env,
            "KIRO_RS_COUNT_TOKENS_API_URL",
        );
        override_optional_string(
            &mut self.count_tokens_api_key,
            get_env,
            "KIRO_RS_COUNT_TOKENS_API_KEY",
        );
        override_string(
            &mut self.count_tokens_auth_type,
            get_env,
            "KIRO_RS_COUNT_TOKENS_AUTH_TYPE",
        )?;
        override_optional_string(&mut self.proxy_url, get_env, "KIRO_RS_PROXY_URL");
        override_optional_string(&mut self.proxy_username, get_env, "KIRO_RS_PROXY_USERNAME");
        override_optional_string(&mut self.proxy_password, get_env, "KIRO_RS_PROXY_PASSWORD");
        override_optional_string(&mut self.admin_api_key, get_env, "KIRO_RS_ADMIN_API_KEY");
        override_load_balancing_mode(&mut self.load_balancing_mode, get_env)?;
        override_bool(
            &mut self.extract_thinking,
            get_env,
            "KIRO_RS_EXTRACT_THINKING",
        )?;
        override_string(
            &mut self.default_endpoint,
            get_env,
            "KIRO_RS_DEFAULT_ENDPOINT",
        )?;
        override_endpoints(&mut self.endpoints, get_env)?;

        Ok(())
    }

    /// 获取配置文件路径（如果有）
    pub fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }

    /// 将当前配置写回原始配置文件
    pub fn save(&self) -> anyhow::Result<()> {
        let path = self
            .config_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("配置文件路径未知，无法保存配置"))?;

        let content = serde_json::to_string_pretty(self).context("序列化配置失败")?;
        fs::write(path, content)
            .with_context(|| format!("写入配置文件失败: {}", path.display()))?;
        Ok(())
    }
}

fn read_env<F>(get_env: &mut F, key: &str) -> Option<String>
where
    F: FnMut(&str) -> Option<String>,
{
    get_env(key).map(|value| value.trim().to_string())
}

fn read_non_empty_env<F>(get_env: &mut F, key: &str) -> Option<String>
where
    F: FnMut(&str) -> Option<String>,
{
    read_env(get_env, key).filter(|value| !value.is_empty())
}

fn override_string<F>(target: &mut String, get_env: &mut F, key: &str) -> anyhow::Result<()>
where
    F: FnMut(&str) -> Option<String>,
{
    if let Some(value) = read_env(get_env, key) {
        if value.is_empty() {
            anyhow::bail!("环境变量 {key} 不能为空");
        }
        *target = value;
    }
    Ok(())
}

fn override_optional_string<F>(target: &mut Option<String>, get_env: &mut F, key: &str)
where
    F: FnMut(&str) -> Option<String>,
{
    if let Some(value) = read_env(get_env, key) {
        *target = if value.is_empty() { None } else { Some(value) };
    }
}

fn override_parsed<T, F>(target: &mut T, get_env: &mut F, key: &str) -> anyhow::Result<()>
where
    T: FromStr,
    T::Err: std::fmt::Display,
    F: FnMut(&str) -> Option<String>,
{
    if let Some(value) = read_env(get_env, key) {
        if value.is_empty() {
            anyhow::bail!("环境变量 {key} 不能为空");
        }
        *target = value
            .parse::<T>()
            .map_err(|err| anyhow::anyhow!("解析环境变量 {key} 失败: {err}"))?;
    }
    Ok(())
}

fn override_bool<F>(target: &mut bool, get_env: &mut F, key: &str) -> anyhow::Result<()>
where
    F: FnMut(&str) -> Option<String>,
{
    if let Some(value) = read_env(get_env, key) {
        *target = parse_bool_env(key, &value)?;
    }
    Ok(())
}

fn parse_bool_env(key: &str, value: &str) -> anyhow::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" | "on" => Ok(true),
        "false" | "0" | "no" | "n" | "off" => Ok(false),
        _ => anyhow::bail!("解析环境变量 {key} 失败: 期望 true/false、1/0、yes/no 或 on/off"),
    }
}

fn override_load_balancing_mode<F>(target: &mut String, get_env: &mut F) -> anyhow::Result<()>
where
    F: FnMut(&str) -> Option<String>,
{
    if let Some(value) = read_env(get_env, "KIRO_RS_LOAD_BALANCING_MODE") {
        match value.as_str() {
            "priority" | "balanced" => *target = value,
            _ => anyhow::bail!("环境变量 KIRO_RS_LOAD_BALANCING_MODE 只能是 priority 或 balanced"),
        }
    }
    Ok(())
}

fn override_endpoints<F>(
    target: &mut HashMap<String, serde_json::Value>,
    get_env: &mut F,
) -> anyhow::Result<()>
where
    F: FnMut(&str) -> Option<String>,
{
    if let Some(value) = read_env(get_env, "KIRO_RS_ENDPOINTS") {
        if value.is_empty() {
            target.clear();
        } else {
            *target = serde_json::from_str(&value)
                .context("解析环境变量 KIRO_RS_ENDPOINTS 失败，应为 JSON 对象")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_map(values: &[(&str, &str)]) -> impl FnMut(&str) -> Option<String> {
        let values: HashMap<String, String> = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |key| values.get(key).cloned()
    }

    #[test]
    fn test_apply_env_overrides_from_env_vars() {
        let mut config = Config::default();
        let mut env = env_map(&[
            ("KIRO_RS_HOST", "0.0.0.0"),
            ("KIRO_RS_PORT", "8990"),
            ("KIRO_RS_API_KEY", "sk-test"),
            ("KIRO_RS_REGION", "eu-west-1"),
            ("KIRO_RS_AUTH_REGION", "us-east-1"),
            ("KIRO_RS_API_REGION", "us-west-2"),
            ("KIRO_RS_TLS_BACKEND", "native-tls"),
            ("KIRO_RS_ADMIN_API_KEY", "sk-admin"),
            ("KIRO_RS_LOAD_BALANCING_MODE", "balanced"),
            ("KIRO_RS_EXTRACT_THINKING", "false"),
            ("KIRO_RS_DEFAULT_ENDPOINT", "ide"),
            (
                "KIRO_RS_ENDPOINTS",
                r#"{"ide":{"baseUrl":"https://example.com"}}"#,
            ),
        ]);

        config.apply_env_overrides_from(&mut env).unwrap();

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 8990);
        assert_eq!(config.api_key.as_deref(), Some("sk-test"));
        assert_eq!(config.region, "eu-west-1");
        assert_eq!(config.auth_region.as_deref(), Some("us-east-1"));
        assert_eq!(config.api_region.as_deref(), Some("us-west-2"));
        assert_eq!(config.tls_backend, TlsBackend::NativeTls);
        assert_eq!(config.admin_api_key.as_deref(), Some("sk-admin"));
        assert_eq!(config.load_balancing_mode, "balanced");
        assert!(!config.extract_thinking);
        assert_eq!(
            config.endpoints["ide"]["baseUrl"].as_str(),
            Some("https://example.com")
        );
    }

    #[test]
    fn test_empty_optional_env_clears_file_value() {
        let mut config = Config::default();
        config.admin_api_key = Some("sk-admin".to_string());
        let mut env = env_map(&[("KIRO_RS_ADMIN_API_KEY", "")]);

        config.apply_env_overrides_from(&mut env).unwrap();

        assert_eq!(config.admin_api_key, None);
    }

    #[test]
    fn test_invalid_bool_env_is_rejected() {
        let mut config = Config::default();
        let mut env = env_map(&[("KIRO_RS_EXTRACT_THINKING", "maybe")]);

        assert!(config.apply_env_overrides_from(&mut env).is_err());
    }
}
