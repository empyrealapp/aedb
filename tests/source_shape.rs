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

fn large_file_budgets() -> BTreeMap<&'static str, usize> {
    BTreeMap::from([
        // Central commit engine internals. Split only along scheduler/apply/replay-safe boundaries.
        ("src/commit/executor/internals.rs", 5_200),
        ("src/commit/executor/tests.rs", 4_300),
        ("src/commit/apply.rs", 4_100),
        ("src/storage/keyspace.rs", 4_200),
        ("src/lib.rs", 3_700),
        ("src/catalog/mod.rs", 3_200),
        ("src/commit/validation.rs", 2_900),
        ("src/order_book.rs", 2_500),
        ("src/query/executor/tests.rs", 2_100),
        ("src/commit/executor/mod.rs", 2_000),
        ("src/lib_helpers.rs", 1_900),
        ("src/lib_tests/auth.rs", 4_400),
        ("src/lib_tests/query_api.rs", 2_200),
        ("src/lib_tests/commit_ops.rs", 1_800),
        ("src/api/order_book_api.rs", 1_700),
        ("src/api/kv_api.rs", 1_550),
        ("src/api/reactive_processors.rs", 1_500),
        ("src/lib_tests/backup_restore.rs", 1_600),
        ("src/lib_tests/kv_api.rs", 1_600),
        ("src/preflight/mod.rs", 1_650),
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
