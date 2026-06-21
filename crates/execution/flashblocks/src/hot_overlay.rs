//! Immutable sparse overlays derived from hot snapshot state overrides.

use std::{collections::BTreeMap, sync::Arc};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_rpc_types::state::{AccountOverride, StateOverride};
use revm::{
    Database,
    bytecode::BytecodeDecodeError,
    state::{AccountInfo, Bytecode},
};

/// Immutable sparse overlay for hot snapshot dry-run execution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HotOverlay {
    accounts: BTreeMap<Address, OverlayAccount>,
    bytecodes: BTreeMap<B256, Bytecode>,
}

/// Per-account override normalized for direct DB reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OverlayAccount {
    /// Balance override, when supplied by the snapshot.
    pub balance: Option<U256>,
    /// Nonce override, when supplied by the snapshot.
    pub nonce: Option<u64>,
    /// Code override, when supplied by the snapshot.
    pub code: Option<Bytecode>,
    /// Hash of the overlay code, when a code override exists.
    pub code_hash: Option<B256>,
    /// Storage values supplied by either `state` or `stateDiff`.
    pub storage: BTreeMap<B256, B256>,
    /// True when `state` replaced the full storage view for this account.
    pub replace_storage: bool,
}

/// Error returned while normalizing a hot overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotOverlayError(String);

impl HotOverlayError {
    fn both_state_and_state_diff(address: Address) -> Self {
        Self(format!("account {address:?} has both state and stateDiff overrides"))
    }

    fn invalid_bytecode(address: Address, error: BytecodeDecodeError) -> Self {
        Self(format!("account {address:?} has invalid code override: {error}"))
    }

    fn unsupported_move_precompile(address: Address) -> Self {
        Self(format!(
            "account {address:?} uses movePrecompileToAddress which is unsupported by hot dry-run"
        ))
    }
}

impl std::fmt::Display for HotOverlayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HotOverlayError {}

impl HotOverlay {
    /// Builds an immutable sparse overlay from RPC state overrides.
    pub fn from_state_override(overrides: &StateOverride) -> Result<Self, HotOverlayError> {
        let mut overlay = Self::default();

        for (&address, account_override) in overrides {
            if account_override.move_precompile_to.is_some() {
                return Err(HotOverlayError::unsupported_move_precompile(address));
            }

            let account =
                OverlayAccount::from_override(address, account_override, &mut overlay.bytecodes)?;
            overlay.accounts.insert(address, account);
        }

        Ok(overlay)
    }

    /// Returns the overlay account for an address.
    pub fn account(&self, address: Address) -> Option<&OverlayAccount> {
        self.accounts.get(&address)
    }

    /// Returns an overlay storage value if one was supplied.
    pub fn storage(&self, address: Address, slot: B256) -> Option<B256> {
        self.accounts.get(&address)?.storage.get(&slot).copied()
    }

    /// Returns bytecode by hash for code supplied by the overlay.
    pub fn bytecode_by_hash(&self, code_hash: B256) -> Option<Bytecode> {
        self.bytecodes.get(&code_hash).cloned()
    }

    /// Number of accounts represented in the overlay.
    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    /// Total number of storage slots represented across all overlay accounts.
    pub fn storage_slot_count(&self) -> usize {
        self.accounts.values().map(|account| account.storage.len()).sum()
    }
}

impl OverlayAccount {
    /// Normalizes a single account override for direct overlay-backed reads.
    pub fn from_override(
        address: Address,
        override_account: &AccountOverride,
        bytecodes: &mut BTreeMap<B256, Bytecode>,
    ) -> Result<Self, HotOverlayError> {
        if override_account.state.is_some() && override_account.state_diff.is_some() {
            return Err(HotOverlayError::both_state_and_state_diff(address));
        }

        let balance = override_account.balance;
        let nonce = override_account.nonce;

        let (code, code_hash) = if let Some(code) = override_account.code.as_ref() {
            let bytecode = Bytecode::new_raw_checked(code.clone())
                .map_err(|error| HotOverlayError::invalid_bytecode(address, error))?;
            let code_hash = keccak256(code.as_ref());

            bytecodes.insert(code_hash, bytecode.clone());
            (Some(bytecode), Some(code_hash))
        } else {
            (None, None)
        };

        let (replace_storage, storage_source) =
            match (override_account.state.as_ref(), override_account.state_diff.as_ref()) {
                (Some(state), None) => (true, Some(state)),
                (None, Some(diff)) => (false, Some(diff)),
                (None, None) => (false, None),
                (Some(_), Some(_)) => unreachable!("checked above"),
            };

        let storage = storage_source
            .into_iter()
            .flat_map(|storage| storage.iter().map(|(slot, value)| (*slot, *value)))
            .collect();

        Ok(Self { balance, nonce, code, code_hash, storage, replace_storage })
    }
}

/// Per-request revm DB that reads a hot overlay before canonical state.
#[derive(Debug)]
pub struct HotOverlayDb<DB> {
    canonical: DB,
    overlay: Arc<HotOverlay>,
}

impl<DB> HotOverlayDb<DB> {
    /// Creates a new hot overlay DB.
    pub fn new(canonical: DB, overlay: Arc<HotOverlay>) -> Self {
        Self { canonical, overlay }
    }
}

impl<DB> Database for HotOverlayDb<DB>
where
    DB: Database,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let Some(overlay_account) = self.overlay.account(address) else {
            return self.canonical.basic(address);
        };

        let mut account = self.canonical.basic(address)?.unwrap_or_default();

        if let Some(balance) = overlay_account.balance {
            account.balance = balance;
        }
        if let Some(nonce) = overlay_account.nonce {
            account.nonce = nonce;
        }
        if let Some(code_hash) = overlay_account.code_hash {
            account.code_hash = code_hash;
        }
        if let Some(code) = overlay_account.code.clone() {
            account.code = Some(code);
        }

        Ok(Some(account))
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(code) = self.overlay.bytecode_by_hash(code_hash) {
            return Ok(code);
        }

        self.canonical.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let slot = B256::new(index.to_be_bytes());

        if let Some(value) = self.overlay.storage(address, slot) {
            return Ok(U256::from_be_slice(value.as_slice()));
        }

        if self.overlay.account(address).is_some_and(|account| account.replace_storage) {
            return Ok(U256::ZERO);
        }

        self.canonical.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.canonical.block_hash(number)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, convert::Infallible, sync::Arc};

    use alloy_primitives::{Address, B256, Bytes, U256, address, keccak256};
    use alloy_rpc_types::state::{AccountOverride, StateOverride};
    use revm::{
        Database,
        state::{AccountInfo, Bytecode},
    };

    use super::{HotOverlay, HotOverlayDb};

    fn word(byte: u8) -> B256 {
        B256::with_last_byte(byte)
    }

    fn slot_key(slot: B256) -> U256 {
        U256::from_be_slice(slot.as_slice())
    }

    fn bytecode(bytes: &[u8]) -> Bytecode {
        Bytecode::new_raw_checked(Bytes::copy_from_slice(bytes)).expect("test bytecode is valid")
    }

    fn account_info(balance: u64, nonce: u64, code_bytes: &[u8]) -> AccountInfo {
        let code = bytecode(code_bytes);

        AccountInfo::default()
            .with_balance(U256::from(balance))
            .with_nonce(nonce)
            .with_code_and_hash(code, keccak256(code_bytes))
    }

    #[derive(Debug, Default)]
    struct FakeCanonicalDb {
        accounts: BTreeMap<Address, AccountInfo>,
        bytecodes: BTreeMap<B256, Bytecode>,
        storage: BTreeMap<(Address, B256), U256>,
        block_hashes: BTreeMap<u64, B256>,
    }

    impl Database for FakeCanonicalDb {
        type Error = Infallible;

        fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(self.accounts.get(&address).cloned())
        }

        fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(self.bytecodes.get(&code_hash).cloned().unwrap_or_default())
        }

        fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
            Ok(self
                .storage
                .get(&(address, B256::new(index.to_be_bytes())))
                .copied()
                .unwrap_or_default())
        }

        fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
            Ok(self.block_hashes.get(&number).copied().unwrap_or_default())
        }
    }

    #[test]
    fn overlay_captures_only_overridden_account_fields_and_code_hash_lookup() {
        let addr = address!("0000000000000000000000000000000000000011");
        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                balance: Some(U256::from(7)),
                nonce: Some(3),
                code: Some(Bytes::from_static(&[0x60, 0x00])),
                ..Default::default()
            },
        );

        let overlay = HotOverlay::from_state_override(&overrides).expect("normalizes");
        let account = overlay.account(addr).expect("account present");

        assert_eq!(overlay.account_count(), 1);
        assert_eq!(account.balance, Some(U256::from(7)));
        assert_eq!(account.nonce, Some(3));
        assert!(account.code.is_some());
        assert!(account.code_hash.is_some());
        assert_eq!(
            overlay.bytecode_by_hash(account.code_hash.expect("code hash present")),
            account.code
        );
    }

    #[test]
    fn overlay_distinguishes_state_replacement_from_state_diff() {
        let replace = address!("0000000000000000000000000000000000000021");
        let diff = address!("0000000000000000000000000000000000000022");
        let slot_a = word(0xaa);
        let slot_b = word(0xbb);
        let mut overrides = StateOverride::default();
        overrides.insert(
            replace,
            AccountOverride {
                state: Some([(slot_a, word(0x01))].into_iter().collect()),
                ..Default::default()
            },
        );
        overrides.insert(
            diff,
            AccountOverride {
                state_diff: Some([(slot_b, word(0x02))].into_iter().collect()),
                ..Default::default()
            },
        );

        let overlay = HotOverlay::from_state_override(&overrides).expect("normalizes");

        assert_eq!(overlay.account_count(), 2);
        assert_eq!(overlay.storage_slot_count(), 2);
        assert!(overlay.account(replace).expect("replace account").replace_storage);
        assert!(!overlay.account(diff).expect("diff account").replace_storage);
        assert_eq!(overlay.storage(replace, slot_a), Some(word(0x01)));
        assert_eq!(overlay.storage(diff, slot_b), Some(word(0x02)));
    }

    #[test]
    fn overlay_storage_only_override_preserves_canonical_account_fields() {
        let addr = address!("0000000000000000000000000000000000000044");
        let slot = word(0x44);
        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                state_diff: Some([(slot, word(0x03))].into_iter().collect()),
                ..Default::default()
            },
        );

        let overlay = HotOverlay::from_state_override(&overrides).expect("normalizes");
        let account = overlay.account(addr).expect("account present");

        assert_eq!(account.balance, None);
        assert_eq!(account.nonce, None);
        assert!(account.code.is_none());
        assert!(account.code_hash.is_none());
        assert_eq!(overlay.storage(addr, slot), Some(word(0x03)));
    }

    #[test]
    fn overlay_tracks_empty_account_override_for_missing_account_semantics() {
        let addr = address!("0000000000000000000000000000000000000033");
        let mut overrides = StateOverride::default();
        overrides.insert(addr, AccountOverride::default());

        let overlay = HotOverlay::from_state_override(&overrides).expect("normalizes");
        let account = overlay.account(addr).expect("account present");

        assert_eq!(overlay.account_count(), 1);
        assert_eq!(account.balance, None);
        assert_eq!(account.nonce, None);
        assert!(account.code.is_none());
        assert!(account.code_hash.is_none());
        assert!(account.storage.is_empty());
        assert!(!account.replace_storage);
    }

    #[test]
    fn overlay_zero_balance_override_is_distinct_from_no_balance_override() {
        let addr = address!("0000000000000000000000000000000000000055");
        let mut overrides = StateOverride::default();
        overrides.insert(addr, AccountOverride { balance: Some(U256::ZERO), ..Default::default() });

        let overlay = HotOverlay::from_state_override(&overrides).expect("normalizes");

        assert_eq!(overlay.account(addr).expect("account present").balance, Some(U256::ZERO));
    }

    #[test]
    fn overlay_rejects_account_with_both_state_and_state_diff() {
        let addr = address!("0000000000000000000000000000000000000066");
        let slot = word(0x01);
        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                state: Some([(slot, word(0x01))].into_iter().collect()),
                state_diff: Some([(slot, word(0x02))].into_iter().collect()),
                ..Default::default()
            },
        );

        let err = HotOverlay::from_state_override(&overrides)
            .expect_err("must reject ambiguous override");

        assert!(err.to_string().contains("both state and stateDiff"));
    }

    #[test]
    fn overlay_rejects_move_precompile_override() {
        let addr = address!("0000000000000000000000000000000000000077");
        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride { move_precompile_to: Some(Address::ZERO), ..Default::default() },
        );

        let err = HotOverlay::from_state_override(&overrides)
            .expect_err("must reject move precompile override");

        assert!(err.to_string().contains("movePrecompileToAddress"));
    }

    #[test]
    fn hot_overlay_db_reads_overlay_account_before_canonical() {
        let addr = address!("0000000000000000000000000000000000000088");
        let canonical_code_bytes = [0x60, 0x01, 0x00];
        let overlay_code_bytes = [0x60, 0x02, 0x00];
        let canonical_account = account_info(1, 3, &canonical_code_bytes);
        let canonical_code_hash = canonical_account.code_hash;
        let overlay_code_hash = keccak256(overlay_code_bytes);

        let mut canonical = FakeCanonicalDb::default();
        canonical.accounts.insert(addr, canonical_account);
        canonical.bytecodes.insert(canonical_code_hash, bytecode(&canonical_code_bytes));

        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                balance: Some(U256::from(7)),
                code: Some(Bytes::copy_from_slice(&overlay_code_bytes)),
                ..Default::default()
            },
        );

        let overlay = Arc::new(HotOverlay::from_state_override(&overrides).expect("normalizes"));
        let mut db = HotOverlayDb::new(canonical, Arc::clone(&overlay));

        let account = db.basic(addr).expect("loads account").expect("overlay account exists");

        assert_eq!(account.balance, U256::from(7));
        assert_eq!(account.nonce, 3);
        assert_eq!(account.code_hash, overlay_code_hash);
        assert_eq!(account.code, overlay.bytecode_by_hash(overlay_code_hash));
        assert_eq!(
            db.code_by_hash(overlay_code_hash).expect("loads overlay code"),
            overlay.bytecode_by_hash(overlay_code_hash).expect("overlay bytecode"),
        );
        assert_eq!(
            db.code_by_hash(canonical_code_hash).expect("loads canonical code"),
            bytecode(&canonical_code_bytes),
        );
    }

    #[test]
    fn hot_overlay_db_falls_back_to_canonical_storage_for_state_diff() {
        let addr = address!("0000000000000000000000000000000000000089");
        let slot_a = word(0xaa);
        let slot_b = word(0xbb);
        let mut canonical = FakeCanonicalDb::default();
        canonical.storage.insert((addr, slot_a), U256::from(1));

        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                state_diff: Some([(slot_b, word(0x02))].into_iter().collect()),
                ..Default::default()
            },
        );

        let overlay = Arc::new(HotOverlay::from_state_override(&overrides).expect("normalizes"));
        let mut db = HotOverlayDb::new(canonical, overlay);

        assert_eq!(db.storage(addr, slot_key(slot_a)).expect("canonical slot"), U256::from(1));
        assert_eq!(db.storage(addr, slot_key(slot_b)).expect("overlay slot"), U256::from(2));
    }

    #[test]
    fn hot_overlay_db_returns_zero_for_missing_slot_after_state_replacement() {
        let addr = address!("0000000000000000000000000000000000000090");
        let slot_a = word(0xaa);
        let slot_b = word(0xbb);
        let mut canonical = FakeCanonicalDb::default();
        canonical.storage.insert((addr, slot_a), U256::from(1));

        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                state: Some([(slot_b, word(0x02))].into_iter().collect()),
                ..Default::default()
            },
        );

        let overlay = Arc::new(HotOverlay::from_state_override(&overrides).expect("normalizes"));
        let mut db = HotOverlayDb::new(canonical, overlay);

        assert_eq!(db.storage(addr, slot_key(slot_a)).expect("missing slot"), U256::ZERO);
        assert_eq!(db.storage(addr, slot_key(slot_b)).expect("overlay slot"), U256::from(2));
    }

    #[test]
    fn hot_overlay_db_treats_overlay_only_account_as_existing() {
        let addr = address!("0000000000000000000000000000000000000091");
        let missing = address!("0000000000000000000000000000000000000092");
        let mut overrides = StateOverride::default();
        overrides.insert(addr, AccountOverride::default());

        let overlay = Arc::new(HotOverlay::from_state_override(&overrides).expect("normalizes"));
        let mut db = HotOverlayDb::new(FakeCanonicalDb::default(), overlay);

        let account = db.basic(addr).expect("loads overlay-only account").expect("account exists");

        assert_eq!(account, AccountInfo::default());
        assert!(db.basic(missing).expect("loads missing account").is_none());
    }
}
