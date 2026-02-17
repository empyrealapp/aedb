use aedb::backup::{BACKUP_MANIFEST_FILE, load_backup_manifest, verify_backup_files};
use aedb::config::{AedbConfig, RecoveryMode};
use aedb::offline;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return Err("missing command".into());
    }
    match args[1].as_str() {
        "backup" => match args.get(2).map(String::as_str) {
            Some("list") => cmd_backup_list(&args[3..]),
            Some("verify") => cmd_backup_verify(&args[3..]),
            Some(other) => Err(format!("unknown backup command: {other}")),
            None => Err("missing backup subcommand".into()),
        },
        "dump" => match args.get(2).map(String::as_str) {
            Some("export") => cmd_dump_export(&args[3..]),
            Some("restore") => cmd_dump_restore(&args[3..]),
            Some("parity") => cmd_dump_parity(&args[3..]),
            Some(other) => Err(format!("unknown dump command: {other}")),
            None => Err("missing dump subcommand".into()),
        },
        "check" => match args.get(2).map(String::as_str) {
            Some("invariants") => cmd_check_invariants(&args[3..]),
            Some(other) => Err(format!("unknown check command: {other}")),
            None => Err("missing check subcommand".into()),
        },
        other => {
            print_usage();
            Err(format!("unknown top-level command: {other}"))
        }
    }
}

fn cmd_backup_list(args: &[String]) -> Result<(), String> {
    let root = parse_flag_value(args, "--root").ok_or("--root is required")?;
    let hmac_key = parse_hmac_key_hex(args)?;
    let root = PathBuf::from(root);

    let mut entries = fs::read_dir(&root)
        .map_err(|e| format!("read_dir {}: {e}", root.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect::<Vec<_>>();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let dir = entry.path();
        if !dir.join(BACKUP_MANIFEST_FILE).exists() {
            continue;
        }
        match load_backup_manifest(&dir, hmac_key.as_deref()) {
            Ok(m) => {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    dir.display(),
                    m.backup_id,
                    m.backup_type,
                    m.checkpoint_seq,
                    m.wal_head_seq
                );
            }
            Err(err) => {
                println!("{}\tINVALID\t{}", dir.display(), err);
            }
        }
    }
    Ok(())
}

fn cmd_backup_verify(args: &[String]) -> Result<(), String> {
    let backup_dir = parse_flag_value(args, "--backup-dir").ok_or("--backup-dir is required")?;
    let hmac_key = parse_hmac_key_hex(args)?;
    let dir = Path::new(&backup_dir);
    let manifest = load_backup_manifest(dir, hmac_key.as_deref())
        .map_err(|e| format!("load manifest: {e}"))?;
    verify_backup_files(dir, &manifest).map_err(|e| format!("verify files: {e}"))?;
    println!(
        "ok\t{}\t{}\t{}\t{}",
        backup_dir, manifest.backup_id, manifest.checkpoint_seq, manifest.wal_head_seq
    );
    Ok(())
}

fn cmd_dump_export(args: &[String]) -> Result<(), String> {
    let data_dir = parse_flag_value(args, "--data-dir").ok_or("--data-dir is required")?;
    let out = parse_flag_value(args, "--out").ok_or("--out is required")?;
    let config = parse_recovery_config(args)?;
    let report = offline::export_snapshot_dump(Path::new(&data_dir), &config, Path::new(&out))
        .map_err(|e| format!("export dump: {e}"))?;
    println!(
        "ok\t{}\t{}\t{}\t{}",
        report.current_seq, report.parity_checksum_hex, report.table_rows, report.kv_entries
    );
    Ok(())
}

fn cmd_dump_restore(args: &[String]) -> Result<(), String> {
    let dump = parse_flag_value(args, "--dump").ok_or("--dump is required")?;
    let data_dir = parse_flag_value(args, "--data-dir").ok_or("--data-dir is required")?;
    let config = parse_recovery_config(args)?;
    let report = offline::restore_snapshot_dump(Path::new(&dump), Path::new(&data_dir), &config)
        .map_err(|e| format!("restore dump: {e}"))?;
    println!(
        "ok\t{}\t{}\t{}\t{}",
        report.current_seq, report.parity_checksum_hex, report.table_rows, report.kv_entries
    );
    Ok(())
}

fn cmd_dump_parity(args: &[String]) -> Result<(), String> {
    let dump = parse_flag_value(args, "--dump").ok_or("--dump is required")?;
    let data_dir = parse_flag_value(args, "--data-dir").ok_or("--data-dir is required")?;
    let config = parse_recovery_config(args)?;
    let report =
        offline::parity_report_against_data_dir(Path::new(&dump), Path::new(&data_dir), &config)
            .map_err(|e| format!("parity report: {e}"))?;
    println!(
        "{}\t{}\t{}",
        if report.matches { "ok" } else { "mismatch" },
        report.expected_checksum_hex,
        report.actual_checksum_hex
    );
    if report.matches {
        Ok(())
    } else {
        Err("parity mismatch".into())
    }
}

fn cmd_check_invariants(args: &[String]) -> Result<(), String> {
    let data_dir = parse_flag_value(args, "--data-dir").ok_or("--data-dir is required")?;
    let config = parse_recovery_config(args)?;
    let report = offline::invariant_report(Path::new(&data_dir), &config)
        .map_err(|e| format!("invariants: {e}"))?;
    println!(
        "{}\t{}\t{}\t{}",
        if report.ok { "ok" } else { "violations" },
        report.table_count,
        report.table_rows,
        report.kv_entries
    );
    for v in &report.violations {
        println!("violation\t{v}");
    }
    if report.ok {
        Ok(())
    } else {
        Err("invariant violations found".into())
    }
}

fn parse_flag_value(args: &[String], flag: &str) -> Option<String> {
    for idx in 0..args.len() {
        if args[idx] == flag {
            return args.get(idx + 1).cloned();
        }
    }
    None
}

fn parse_hmac_key_hex(args: &[String]) -> Result<Option<Vec<u8>>, String> {
    let Some(hex_key) = parse_flag_value(args, "--hmac-key-hex") else {
        return Ok(None);
    };
    hex::decode(hex_key)
        .map(Some)
        .map_err(|e| format!("invalid --hmac-key-hex: {e}"))
}

fn parse_recovery_config(args: &[String]) -> Result<AedbConfig, String> {
    let mut cfg = AedbConfig::default();
    if args.iter().any(|a| a == "--permissive") {
        cfg.recovery_mode = RecoveryMode::Permissive;
        cfg.hash_chain_required = false;
    }
    if let Some(key) = parse_hmac_key_hex(args)? {
        cfg = cfg.with_hmac_key(key);
    }
    Ok(cfg)
}

fn print_usage() {
    eprintln!("usage:");
    eprintln!("  aedb backup list --root <backup-root> [--hmac-key-hex <hex>]");
    eprintln!("  aedb backup verify --backup-dir <backup-dir> [--hmac-key-hex <hex>]");
    eprintln!(
        "  aedb dump export --data-dir <aedb-dir> --out <dump.aedbdump> [--hmac-key-hex <hex>] [--permissive]"
    );
    eprintln!(
        "  aedb dump restore --dump <dump.aedbdump> --data-dir <target-dir> [--hmac-key-hex <hex>] [--permissive]"
    );
    eprintln!(
        "  aedb dump parity --dump <dump.aedbdump> --data-dir <aedb-dir> [--hmac-key-hex <hex>] [--permissive]"
    );
    eprintln!(
        "  aedb check invariants --data-dir <aedb-dir> [--hmac-key-hex <hex>] [--permissive]"
    );
}
