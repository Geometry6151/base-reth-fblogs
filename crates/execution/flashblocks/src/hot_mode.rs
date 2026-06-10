//! Runtime mode for flashblock processing.

use core::{fmt, str::FromStr};

/// Runtime flashblock processing mode.
///
/// The selected mode is currently stored and plumbed through the flashblocks stack.
/// Behavior changes for non-legacy modes land separately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FlashblocksMode {
    /// Existing rebuild-based pending-state behavior.
    #[default]
    Legacy,
    /// Reserved for later append-only hot-path processing; currently stored/plumbed only.
    HotOnly,
}

impl FromStr for FlashblocksMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "legacy" => Ok(Self::Legacy),
            "hot-only" => Ok(Self::HotOnly),
            _ => Err("unknown flashblocks mode; expected legacy or hot-only"),
        }
    }
}

impl fmt::Display for FlashblocksMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Legacy => f.write_str("legacy"),
            Self::HotOnly => f.write_str("hot-only"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FlashblocksMode;

    #[test]
    fn parses_hot_only_mode() {
        assert_eq!("hot-only".parse::<FlashblocksMode>().unwrap(), FlashblocksMode::HotOnly);
    }

    #[test]
    fn rejects_async_materializer_in_first_slice() {
        assert!("hot-async-materializer".parse::<FlashblocksMode>().is_err());
    }
}
