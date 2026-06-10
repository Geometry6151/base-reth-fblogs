use std::sync::Arc;

use url::Url;

use crate::{FlashblocksMode, FlashblocksState};

/// Flashblocks-specific configuration knobs.
#[derive(Debug, Clone)]
pub struct FlashblocksConfig {
    /// The websocket endpoint that streams flashblock updates.
    pub websocket_url: Url,
    /// Maximum number of pending flashblocks to retain in memory.
    pub max_pending_blocks_depth: u64,
    /// Whether to enable cached execution via the flashblocks-aware engine validator.
    pub cached_execution: bool,
    /// Shared Flashblocks state.
    pub state: Arc<FlashblocksState>,
}

impl FlashblocksConfig {
    /// Create a new Flashblocks configuration using the legacy runtime mode.
    pub fn new(websocket_url: Url, max_pending_blocks_depth: u64) -> Self {
        Self::new_with_mode(websocket_url, max_pending_blocks_depth, FlashblocksMode::Legacy)
    }

    /// Create a new Flashblocks configuration with an explicit runtime mode.
    pub fn new_with_mode(
        websocket_url: Url,
        max_pending_blocks_depth: u64,
        mode: FlashblocksMode,
    ) -> Self {
        let state = Arc::new(FlashblocksState::new_with_mode(max_pending_blocks_depth, mode));
        Self { websocket_url, max_pending_blocks_depth, cached_execution: false, state }
    }

    /// Returns the configured flashblocks runtime mode.
    ///
    /// The shared [`FlashblocksState`] is the single source of truth for the selected mode.
    pub fn mode(&self) -> FlashblocksMode {
        self.state.mode()
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;

    #[test]
    fn flashblocks_config_new_defaults_to_legacy_mode() {
        let config = FlashblocksConfig::new(Url::parse("ws://localhost:12345").unwrap(), 5);

        assert_eq!(config.mode(), FlashblocksMode::Legacy);
    }

    #[test]
    fn flashblocks_config_new_with_mode_stores_hot_mode() {
        let config = FlashblocksConfig::new_with_mode(
            Url::parse("ws://localhost:12345").unwrap(),
            5,
            FlashblocksMode::HotOnly,
        );

        assert_eq!(config.mode(), FlashblocksMode::HotOnly);
        assert_eq!(config.state.mode(), FlashblocksMode::HotOnly);
    }
}
