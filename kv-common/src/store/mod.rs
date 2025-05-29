
// WAL and transaction modules (existing)
mod wal;
mod transaction;
mod store_transaction;

// New modular structure
mod error;
mod data_types;
mod metadata;
mod memory;
mod expiry;
mod traits;
mod string_ops;
mod list_ops;
mod hash_ops;
mod set_ops;
mod store_core;
mod store_manager;

// Export WAL and transaction types (existing)
pub use wal::{
    WriteAheadLog, LogEntry, LogCommand, Checkpoint, 
    WalError, WalResult
};

pub use transaction::{
    Transaction, TransactionManager, TransactionState, StoreOperation
};

pub use self::store_transaction::StoreTransactionExt;
pub use self::store_transaction::TransactionStoreManager;

// Export new modular types
pub use error::{StoreError, StoreResult};
pub use data_types::DataType;
pub use metadata::DataMetadata;
pub use memory::{MemoryManager, OptimizationStrategy};
pub use expiry::ExpiryManager;
pub use traits::{
    StoreOperations, StringOperations, ListOperations, 
    HashOperations, SetOperations
};
pub use store_core::Store;
pub use store_manager::StoreManager;