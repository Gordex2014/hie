//! The store's connection evidence from Tests, and what describe and sample keep.
//!
//! Empty until track S fills it. Its DDL and added columns are already
//! registered in `db/mod.rs` (`TABLES`, `ADDED_COLUMNS`), so adding a table
//! or a column here needs no change elsewhere.

/// The DDL of this area's tables (`CREATE TABLE IF NOT EXISTS ...`).
pub(super) const TABLES: &str = "";

/// Columns added to this area's tables after a store was first created.
pub(super) const ADDED_COLUMNS: &[(&str, &str, &str)] = &[];
