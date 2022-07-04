use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub(crate) const BIN_REPLICATION_META: &str = "rmeta"; // FIXME used to be KEY_REPLICATION_META but now changed it to bin name so may have problem of whether (un)wrap or not
pub(crate) const LIST_ALIVE_IDS: &str = "alives";
pub(crate) const BIN_DEFAULT: &str = "";

pub(crate) const FREQ_CHECK_BACKEND_LIVENESS: u64 = 1;
pub(crate) const FREQ_CHECK_KEEPER_LIVENESS: u64 = 1;

pub(crate) const REPLICA_NUM: usize = 3;
pub(crate) const SCHEME_HTTP: &str = "http://";

pub(crate) const MIN_ALIVE_BACKENDS: u64 = 1;

pub(crate) fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}
