//! Subcommand implementations, one module per subcommand.
//!
//! `main.rs`'s dispatch is a small match on the subcommand name to the
//! matching module's `parse`/`run`; adding a new subcommand means adding a
//! module here plus one arm there, not touching anything else.

pub mod apply;
pub mod run;
pub mod status;
