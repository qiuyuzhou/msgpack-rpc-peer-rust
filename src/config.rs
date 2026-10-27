use std::time::Duration;

/// Connection resource and lifecycle configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Deadline covering pending capacity, queue capacity, and the response wait.
    pub default_call_timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            default_call_timeout: Duration::from_secs(30),
        }
    }
}
impl Config {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        if self.default_call_timeout.is_zero() {
            return Err(crate::Error::InvalidConfig("call timeout must be positive"));
        }
        Ok(())
    }
}
