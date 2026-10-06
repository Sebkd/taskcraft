//! Compiles the code of the READMEs as doc tests (change `readme-and-release`,
//! criterion 1): a README that falls behind the API fails `cargo test`.
//! Snippets that need a broker or a database are `no_run`. Never published.

#[doc = include_str!("../../../README.md")]
mod repository_readme {}

#[doc = include_str!("../../taskcraft-kafka/README.md")]
mod kafka_readme {}

#[doc = include_str!("../../taskcraft-postgres/README.md")]
mod postgres_readme {}
