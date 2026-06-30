use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_SOURCE_LINE_BUDGET: usize = 1_500;

#[test]
fn source_files_stay_within_reviewable_line_budgets() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let budgets = large_file_budgets();
    let mut violations = Vec::new();

    for path in collect_rust_sources(&root.join("src")) {
        let rel = path.strip_prefix(&root).expect("source path under root");
        let rel_key = rel.to_string_lossy().replace('\\', "/");
        let line_count = fs::read_to_string(&path)
            .expect("read source")
            .lines()
            .count();
        let budget = budgets
            .get(rel_key.as_str())
            .copied()
            .unwrap_or(DEFAULT_SOURCE_LINE_BUDGET);
        if line_count > budget {
            violations.push(format!("{rel_key}: {line_count} lines > budget {budget}"));
        }
    }

    assert!(
        violations.is_empty(),
        "source files exceeded reviewability budgets; split the file or add a documented budget in tests/source_shape.rs:\n{}",
        violations.join("\n")
    );
}

#[test]
fn readme_stays_product_focused() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let readme = fs::read_to_string(root.join("README.md")).expect("read README");
    let banned_terms = [
        "casino",
        "financial-grade",
        "bank-grade",
        "battle-tested",
        "enterprise-grade",
        "production-ready",
    ];

    for term in banned_terms {
        assert!(
            !readme.to_ascii_lowercase().contains(term),
            "README should stay storage-engine focused and avoid `{term}`"
        );
    }
}

#[test]
fn source_modules_do_not_use_parent_or_crate_glob_imports() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut violations = Vec::new();

    for path in collect_rust_sources(&root.join("src")) {
        let source = fs::read_to_string(&path).expect("read source");
        for (line_number, line) in source.lines().enumerate() {
            if matches!(line.trim(), "use super::*;" | "use crate::*;") {
                let rel = path
                    .strip_prefix(&root)
                    .expect("source path under root")
                    .to_string_lossy()
                    .replace('\\', "/");
                violations.push(format!("{}:{}", rel, line_number + 1));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "source modules should import explicit dependencies instead of parent globs:\n{}",
        violations.join("\n")
    );
}

#[test]
fn source_modules_do_not_use_wildcard_reexports() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut violations = Vec::new();

    for path in collect_rust_sources(&root.join("src")) {
        let source = fs::read_to_string(&path).expect("read source");
        for (line_number, line) in source.lines().enumerate() {
            let trimmed = line.trim();
            if (trimmed.starts_with("pub use ") || trimmed.starts_with("pub(crate) use "))
                && trimmed.ends_with("::*;")
            {
                let rel = path
                    .strip_prefix(&root)
                    .expect("source path under root")
                    .to_string_lossy()
                    .replace('\\', "/");
                violations.push(format!("{}:{}", rel, line_number + 1));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "source modules should re-export explicit items instead of wildcard exports:\n{}",
        violations.join("\n")
    );
}

#[test]
fn implementation_modules_keep_tests_in_dedicated_files() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut violations = Vec::new();

    for path in collect_rust_sources(&root.join("src")) {
        if is_dedicated_test_source(&path) {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read source");
        let lines: Vec<_> = source.lines().collect();
        for (line_index, pair) in lines.windows(2).enumerate() {
            if pair[0].trim() == "#[cfg(test)]" && pair[1].trim() == "mod tests {" {
                let rel = path
                    .strip_prefix(&root)
                    .expect("source path under root")
                    .to_string_lossy()
                    .replace('\\', "/");
                violations.push(format!("{}:{}", rel, line_index + 1));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "implementation modules should move inline test bodies into dedicated test files:\n{}",
        violations.join("\n")
    );
}

fn large_file_budgets() -> BTreeMap<&'static str, usize> {
    BTreeMap::from([
        // Central commit engine internals. Split only along scheduler/apply/replay-safe boundaries.
        // +index posting eviction step in the pre-WAL spill cascade.
        ("src/commit/executor/internals.rs", 5_520),
        ("src/commit/executor/tests.rs", 4_300),
        // +tier-aware secondary-index maintenance (tombstoning, cold-tier unique/FK checks).
        // System-table schema bootstrap moved to apply/system_schema.rs (budget lowered).
        ("src/commit/apply.rs", 3_700),
        // Row-spill support (StoredRow + spill_table_rows + materialize helpers) plus the
        // secondary-index cold tier (eviction, re-inline, composite-key helpers).
        // +`# Panics` docs steering the convenience KV reads to their `try_` twins.
        ("src/storage/keyspace.rs", 4_100),
        // Cold-tier eviction/re-inline + tier-read coverage for rows, KV, and indexes.
        ("src/storage/keyspace/tests.rs", 1_700),
        ("src/lib.rs", 3_800),
        ("src/catalog/mod.rs", 2_150),
        ("src/commit/validation.rs", 2_800),
        ("src/order_book.rs", 2_200),
        ("src/commit/executor/mod.rs", 1_900),
        ("src/lib_tests/auth.rs", 4_400),
        ("src/lib_tests/query_api.rs", 2_200),
        ("src/lib_tests/commit_ops.rs", 1_750),
        ("src/api/order_book_api.rs", 1_610),
        ("src/api/kv_api.rs", 1_550),
        ("src/api/reactive_processors.rs", 1_520),
        ("src/lib_tests/backup_restore.rs", 1_780),
        ("src/lib_tests/kv_api.rs", 1_650),
        // Helper facades should stay thin. Add narrowly named modules instead of rebuilding catch-alls.
        ("src/lib_helpers.rs", 55),
        // Query executor test coverage is split by behavior; keep the shared fixture module small.
        // Bumped for the expanded query-engine tests (joins/DISTINCT/computed projections).
        ("src/query/executor/tests.rs", 500),
    ])
}

fn collect_rust_sources(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_rust_sources_inner(dir, &mut out);
    out
}

fn collect_rust_sources_inner(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source dir") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources_inner(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn is_dedicated_test_source(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
}
