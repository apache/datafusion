use rocksdb::DB as RocksDB;
use std::sync::Arc;

pub struct DenormalizedRuntimeEnv {
    // Inherit all fields from RuntimeEnv
    runtime_env: RuntimeEnv,
    // Add RocksDB connection
    rocksdb: Arc<RocksDB>,
}

impl DenormalizedRuntimeEnv {
    pub fn new(config: RuntimeConfig, rocksdb: Arc<RocksDB>) -> Result<Self> {
        let runtime_env = RuntimeEnv::new(config)?;
        Ok(Self {
            runtime_env,
            rocksdb,
        })
    }

    // Getter for RocksDB connection
    pub fn rocksdb(&self) -> &Arc<RocksDB> {
        &self.rocksdb
    }
}

// Implement Deref to allow transparent access to RuntimeEnv methods
impl std::ops::Deref for DenormalizedRuntimeEnv {
    type Target = RuntimeEnv;

    fn deref(&self) -> &Self::Target {
        &self.runtime_env
    }
}

// Implement DerefMut if you need mutable access to RuntimeEnv fields
impl std::ops::DerefMut for DenormalizedRuntimeEnv {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime_env
    }
}

// Implement From<DenormalizedRuntimeEnv> for RuntimeEnv
impl From<DenormalizedRuntimeEnv> for RuntimeEnv {
    fn from(env: DenormalizedRuntimeEnv) -> Self {
        env.runtime_env
    }
}
