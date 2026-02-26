use std::fs;
use std::path::{Path, PathBuf};

fn assert_no_ambiguous_locals(file: &str, source: &str) {
    let banned = [
        "let len =",
        "let mut len =",
        "let idx =",
        "let mut idx =",
        "let index =",
        "let mut index =",
        "let size =",
        "let mut size =",
        "let offset =",
        "let mut offset =",
        "for idx in",
        "for index in",
        "for size in",
        "for offset in",
    ];
    for (line_number, line) in source.lines().enumerate() {
        for pattern in banned {
            assert!(
                !line.contains(pattern),
                "{file}:{} contains banned pattern `{pattern}`: {line}",
                line_number + 1
            );
        }
    }
}

fn collect_rust_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn critical_modules_avoid_ambiguous_index_size_offset_len_bindings() {
    let files: [(&str, &str); 16] = [
        ("src/wal/frame.rs", include_str!("../src/wal/frame.rs")),
        ("src/wal/segment.rs", include_str!("../src/wal/segment.rs")),
        (
            "src/recovery/replay.rs",
            include_str!("../src/recovery/replay.rs"),
        ),
        (
            "src/recovery/scanner.rs",
            include_str!("../src/recovery/scanner.rs"),
        ),
        (
            "src/recovery/mod.rs",
            include_str!("../src/recovery/mod.rs"),
        ),
        (
            "src/checkpoint/loader.rs",
            include_str!("../src/checkpoint/loader.rs"),
        ),
        (
            "src/commit/executor/mod.rs",
            include_str!("../src/commit/executor/mod.rs"),
        ),
        (
            "src/commit/executor/internals.rs",
            include_str!("../src/commit/executor/internals.rs"),
        ),
        (
            "src/query/executor.rs",
            include_str!("../src/query/executor.rs"),
        ),
        (
            "src/query/operators.rs",
            include_str!("../src/query/operators.rs"),
        ),
        (
            "src/commit/validation.rs",
            include_str!("../src/commit/validation.rs"),
        ),
        (
            "src/commit/apply.rs",
            include_str!("../src/commit/apply.rs"),
        ),
        (
            "src/commit/executor/global_index.rs",
            include_str!("../src/commit/executor/global_index.rs"),
        ),
        (
            "src/storage/encoded_key.rs",
            include_str!("../src/storage/encoded_key.rs"),
        ),
        (
            "src/storage/keyspace.rs",
            include_str!("../src/storage/keyspace.rs"),
        ),
        (
            "src/storage/index.rs",
            include_str!("../src/storage/index.rs"),
        ),
    ];
    for (file, source) in files {
        assert_no_ambiguous_locals(file, source);
    }
}

#[test]
fn all_src_files_avoid_ambiguous_index_size_offset_len_bindings() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_files(&src_root, &mut files);
    files.sort();
    for file in files {
        let source = fs::read_to_string(&file).expect("read source file");
        let display = file.to_string_lossy().to_string();
        assert_no_ambiguous_locals(&display, &source);
    }
}
