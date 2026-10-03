//! SQLite 后端的多租户键值存储。
//!
//! 键按租户命名空间隔离；每个租户维护一个单调递增的修订号，
//! 插件的暂存写入只有在「基准修订号仍然有效」时才会一次性提交，
//! 否则整体撤销（乐观并发控制）。

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

/// 提交暂存写入时可能发生的错误。
#[derive(Debug)]
pub enum CommitError {
    /// 租户修订号已越过任务基准：冲突，什么都不应用。
    Conflict { expected: i64, actual: i64 },
    Sql(rusqlite::Error),
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitError::Conflict { expected, actual } => {
                write!(f, "revision conflict: baseline {expected}, current {actual}")
            }
            CommitError::Sql(e) => write!(f, "sqlite error: {e}"),
        }
    }
}

impl std::error::Error for CommitError {}

impl From<rusqlite::Error> for CommitError {
    fn from(e: rusqlite::Error) -> Self {
        CommitError::Sql(e)
    }
}

/// 基于 SQLite 的租户命名空间键值存储，带按租户的乐观修订号。
pub struct KvStore {
    conn: Mutex<Connection>,
}

impl KvStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> rusqlite::Result<Self> {
        Self::init(Connection::open(path)?)
    }

    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> rusqlite::Result<Self> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS kv (
                 tenant TEXT NOT NULL,
                 key    TEXT NOT NULL,
                 value  BLOB NOT NULL,
                 PRIMARY KEY (tenant, key)
             );
             CREATE TABLE IF NOT EXISTS tenant_rev (
                 tenant TEXT PRIMARY KEY,
                 rev    INTEGER NOT NULL
             );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// 租户当前已提交的修订号（从未提交过则为 0）。
    pub fn baseline(&self, tenant: &str) -> rusqlite::Result<i64> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT rev FROM tenant_rev WHERE tenant = ?1",
                [tenant],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }

    /// 读取已提交的值（看不到其他任务的暂存写入）。
    pub fn get(&self, tenant: &str, key: &str) -> rusqlite::Result<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM kv WHERE tenant = ?1 AND key = ?2",
            (tenant, key),
            |r| r.get(0),
        )
        .optional()
    }

    /// 测试/预置辅助：绕过暂存直接写入（不改动修订号）。
    pub fn seed(&self, tenant: &str, key: &str, value: &[u8]) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO kv (tenant, key, value) VALUES (?1, ?2, ?3)
             ON CONFLICT (tenant, key) DO UPDATE SET value = excluded.value",
            (tenant, key, value),
        )?;
        Ok(())
    }

    /// 当且仅当租户修订号仍等于 `baseline` 时，一次性提交全部暂存写入。
    /// 冲突或 SQL 错误时什么都不应用（事务回滚）。
    pub fn commit(
        &self,
        tenant: &str,
        baseline: i64,
        staged: &HashMap<String, Vec<u8>>,
    ) -> Result<i64, CommitError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual: i64 = tx
            .query_row(
                "SELECT rev FROM tenant_rev WHERE tenant = ?1",
                [tenant],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        if actual != baseline {
            return Err(CommitError::Conflict {
                expected: baseline,
                actual,
            });
        }
        for (key, value) in staged {
            tx.execute(
                "INSERT INTO kv (tenant, key, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT (tenant, key) DO UPDATE SET value = excluded.value",
                (tenant, key, value),
            )?;
        }
        tx.execute(
            "INSERT INTO tenant_rev (tenant, rev) VALUES (?1, ?2)
             ON CONFLICT (tenant) DO UPDATE SET rev = excluded.rev",
            (tenant, baseline + 1),
        )?;
        tx.commit()?;
        Ok(baseline + 1)
    }
}
