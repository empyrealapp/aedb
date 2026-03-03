use aedb::catalog::types::Value;
use aedb::config::{AedbConfig, RecoveryMode};
use aedb::permission::CallerContext;
use aedb::query::plan::{ConsistencyMode, Query, QueryOptions};
use aedb::{AedbInstance, ScopeInfo, TableInfo};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        print_usage();
        return Err("missing command".into());
    }

    match args[0].as_str() {
        "projects" => cmd_projects(&args[1..]).await,
        "scopes" => cmd_scopes(&args[1..]).await,
        "tables" => cmd_tables(&args[1..]).await,
        "describe-table" => cmd_describe_table(&args[1..]).await,
        "scan-table" => cmd_scan_table(&args[1..]).await,
        "scan-kv" => cmd_scan_kv(&args[1..]).await,
        "help" | "--help" | "-h" => {
            print_usage();
            Ok(())
        }
        other => {
            print_usage();
            Err(format!("unknown command: {other}"))
        }
    }
}

async fn cmd_projects(args: &[String]) -> Result<(), String> {
    let opts = parse_global_options(args)?;
    let db = open_db(&opts)?;

    println!("project_id\tscope_count\tcreated_at_micros");
    for project in db.list_projects().await.map_err(|e| e.to_string())? {
        println!(
            "{}\t{}\t{}",
            project.project_id, project.scope_count, project.created_at_micros
        );
    }
    Ok(())
}

async fn cmd_scopes(args: &[String]) -> Result<(), String> {
    let opts = parse_global_options(args)?;
    let project_id = parse_required_flag(args, "--project")?;
    let db = open_db(&opts)?;

    let scopes = db
        .list_scopes_info(&project_id)
        .await
        .map_err(|e| e.to_string())?;
    print_scopes(&scopes);
    Ok(())
}

async fn cmd_tables(args: &[String]) -> Result<(), String> {
    let opts = parse_global_options(args)?;
    let project_id = parse_required_flag(args, "--project")?;
    let scope_id = parse_required_flag(args, "--scope")?;
    let db = open_db(&opts)?;

    let tables = db
        .list_tables_info(&project_id, &scope_id)
        .await
        .map_err(|e| e.to_string())?;
    print_tables(&tables);
    Ok(())
}

async fn cmd_describe_table(args: &[String]) -> Result<(), String> {
    let opts = parse_global_options(args)?;
    let project_id = parse_required_flag(args, "--project")?;
    let scope_id = parse_required_flag(args, "--scope")?;
    let table_name = parse_required_flag(args, "--table")?;
    let db = open_db(&opts)?;

    let schema = db
        .describe_table(&project_id, &scope_id, &table_name)
        .await
        .map_err(|e| e.to_string())?;

    println!("table\t{}.{}.{}", project_id, scope_id, schema.table_name);
    println!("primary_key\t{}", schema.primary_key.join(","));
    println!("columns");
    println!("name\ttype\tnullable");
    for column in &schema.columns {
        println!(
            "{}\t{:?}\t{}",
            column.name, column.col_type, column.nullable
        );
    }
    Ok(())
}

async fn cmd_scan_table(args: &[String]) -> Result<(), String> {
    let opts = parse_global_options(args)?;
    let project_id = parse_required_flag(args, "--project")?;
    let scope_id = parse_required_flag(args, "--scope")?;
    let table_name = parse_required_flag(args, "--table")?;
    let limit = parse_u64_flag(args, "--limit")?.unwrap_or(50);
    let limit_usize = usize::try_from(limit).map_err(|_| "--limit is too large")?;
    if limit_usize == 0 {
        return Err("--limit must be greater than 0".into());
    }

    let db = open_db(&opts)?;
    let schema = db
        .describe_table(&project_id, &scope_id, &table_name)
        .await
        .map_err(|e| e.to_string())?;

    let query = Query::select(&["*"]).from(&table_name).limit(limit_usize);
    let options = QueryOptions {
        consistency: ConsistencyMode::AtLatest,
        allow_full_scan: true,
        ..Default::default()
    };

    let result = if let Some(caller) = caller_context(&opts) {
        db.query_with_options_as(Some(&caller), &project_id, &scope_id, query, options)
            .await
    } else {
        db.query_with_options(&project_id, &scope_id, query, options)
            .await
    }
    .map_err(|e| e.to_string())?;

    let headers: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    println!("{}", headers.join("\t"));
    for row in &result.rows {
        let rendered: Vec<String> = row.values.iter().map(value_to_field).collect();
        println!("{}", rendered.join("\t"));
    }
    println!(
        "rows_returned={} rows_examined={} truncated={} snapshot_seq={}",
        result.rows.len(),
        result.rows_examined,
        result.truncated,
        result.snapshot_seq
    );
    Ok(())
}

async fn cmd_scan_kv(args: &[String]) -> Result<(), String> {
    let opts = parse_global_options(args)?;
    let project_id = parse_required_flag(args, "--project")?;
    let scope_id = parse_required_flag(args, "--scope")?;
    let limit = parse_u64_flag(args, "--limit")?.unwrap_or(50);
    if limit == 0 {
        return Err("--limit must be greater than 0".into());
    }
    let prefix = parse_hex_flag(args, "--prefix-hex")?.unwrap_or_default();

    let db = open_db(&opts)?;
    let result = if let Some(caller) = caller_context(&opts) {
        db.kv_scan_prefix(
            &project_id,
            &scope_id,
            &prefix,
            limit,
            None,
            ConsistencyMode::AtLatest,
            &caller,
        )
        .await
        .map(|scan| (scan.entries, scan.truncated, scan.snapshot_seq))
    } else {
        db.kv_scan_prefix_no_auth(
            &project_id,
            &scope_id,
            &prefix,
            limit,
            ConsistencyMode::AtLatest,
        )
        .await
        .map(|entries| (entries, false, 0))
    }
    .map_err(|e| e.to_string())?;

    println!("key_hex\tvalue_hex\tversion\tcreated_at");
    for (key, entry) in &result.0 {
        println!(
            "{}\t{}\t{}\t{}",
            hex::encode(key),
            hex::encode(&entry.value),
            entry.version,
            entry.created_at
        );
    }
    if result.2 == 0 {
        println!(
            "rows_returned={} truncated={} snapshot_seq=unknown",
            result.0.len(),
            result.1
        );
    } else {
        println!(
            "rows_returned={} truncated={} snapshot_seq={}",
            result.0.len(),
            result.1,
            result.2
        );
    }
    Ok(())
}

fn parse_global_options(args: &[String]) -> Result<GlobalOptions, String> {
    let data_dir = parse_required_flag(args, "--data-dir")?;
    let permissive = has_flag(args, "--permissive");
    let secure = has_flag(args, "--secure");
    let caller_id = parse_flag_value(args, "--caller-id");
    let hmac_key = parse_hex_flag(args, "--hmac-key-hex")?;

    Ok(GlobalOptions {
        data_dir: PathBuf::from(data_dir),
        permissive,
        secure,
        caller_id,
        hmac_key,
    })
}

fn open_db(opts: &GlobalOptions) -> Result<AedbInstance, String> {
    let mut config = AedbConfig::default();
    if opts.permissive {
        config.recovery_mode = RecoveryMode::Permissive;
        config.hash_chain_required = false;
    }
    if let Some(hmac_key) = &opts.hmac_key {
        config = config.with_hmac_key(hmac_key.clone());
    }

    if opts.secure {
        AedbInstance::open_secure(config, &opts.data_dir).map_err(|e| e.to_string())
    } else {
        AedbInstance::open(config, &opts.data_dir).map_err(|e| e.to_string())
    }
}

fn caller_context(opts: &GlobalOptions) -> Option<CallerContext> {
    opts.caller_id
        .as_ref()
        .map(|caller_id| CallerContext::new(caller_id.clone()))
}

fn parse_required_flag(args: &[String], flag: &str) -> Result<String, String> {
    parse_flag_value(args, flag).ok_or_else(|| format!("{flag} is required"))
}

fn parse_hex_flag(args: &[String], flag: &str) -> Result<Option<Vec<u8>>, String> {
    let Some(value) = parse_flag_value(args, flag) else {
        return Ok(None);
    };
    hex::decode(&value)
        .map(Some)
        .map_err(|e| format!("invalid {flag}: {e}"))
}

fn parse_u64_flag(args: &[String], flag: &str) -> Result<Option<u64>, String> {
    let Some(value) = parse_flag_value(args, flag) else {
        return Ok(None);
    };
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|e| format!("invalid {flag}: {e}"))
}

fn parse_flag_value(args: &[String], flag: &str) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == flag {
            return args.get(i + 1).cloned();
        }
    }
    None
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn print_scopes(scopes: &[ScopeInfo]) {
    println!("scope_id\ttable_count\tkv_key_count\tcreated_at_micros");
    for scope in scopes {
        println!(
            "{}\t{}\t{}\t{}",
            scope.scope_id, scope.table_count, scope.kv_key_count, scope.created_at_micros
        );
    }
}

fn print_tables(tables: &[TableInfo]) {
    println!("table_name\tcolumn_count\tindex_count\trow_count");
    for table in tables {
        println!(
            "{}\t{}\t{}\t{}",
            table.table_name, table.column_count, table.index_count, table.row_count
        );
    }
}

fn value_to_field(value: &Value) -> String {
    let raw = match value {
        Value::Text(v) => v.to_string(),
        Value::U8(v) => v.to_string(),
        Value::U64(v) => v.to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Boolean(v) => v.to_string(),
        Value::U256(v) => format!("0x{}", hex::encode(v)),
        Value::I256(v) => format!("0x{}", hex::encode(v)),
        Value::Blob(v) => format!("0x{}", hex::encode(v)),
        Value::Timestamp(v) => v.to_string(),
        Value::Json(v) => v.to_string(),
        Value::Null => "null".into(),
    };
    raw.replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn print_usage() {
    eprintln!("usage:");
    eprintln!(
        "  aedb-explorer projects --data-dir <aedb-dir> [--hmac-key-hex <hex>] [--permissive] [--secure] [--caller-id <id>]"
    );
    eprintln!(
        "  aedb-explorer scopes --data-dir <aedb-dir> --project <id> [--hmac-key-hex <hex>] [--permissive] [--secure] [--caller-id <id>]"
    );
    eprintln!(
        "  aedb-explorer tables --data-dir <aedb-dir> --project <id> --scope <id> [--hmac-key-hex <hex>] [--permissive] [--secure] [--caller-id <id>]"
    );
    eprintln!(
        "  aedb-explorer describe-table --data-dir <aedb-dir> --project <id> --scope <id> --table <name> [--hmac-key-hex <hex>] [--permissive] [--secure] [--caller-id <id>]"
    );
    eprintln!(
        "  aedb-explorer scan-table --data-dir <aedb-dir> --project <id> --scope <id> --table <name> [--limit <n>] [--hmac-key-hex <hex>] [--permissive] [--secure] [--caller-id <id>]"
    );
    eprintln!(
        "  aedb-explorer scan-kv --data-dir <aedb-dir> --project <id> --scope <id> [--prefix-hex <hex>] [--limit <n>] [--hmac-key-hex <hex>] [--permissive] [--secure] [--caller-id <id>]"
    );
}

struct GlobalOptions {
    data_dir: PathBuf,
    permissive: bool,
    secure: bool,
    caller_id: Option<String>,
    hmac_key: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::{parse_hex_flag, parse_u64_flag};

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_u64_flag_rejects_invalid_number() {
        let args = v(&["--limit", "abc"]);
        let err = parse_u64_flag(&args, "--limit").expect_err("invalid number");
        assert!(err.contains("invalid --limit"));
    }

    #[test]
    fn parse_hex_flag_decodes_bytes() {
        let args = v(&["--prefix-hex", "00ff"]);
        let bytes = parse_hex_flag(&args, "--prefix-hex")
            .expect("hex parse")
            .expect("flag should be present");
        assert_eq!(bytes, vec![0x00, 0xff]);
    }

    #[test]
    fn parse_hex_flag_rejects_invalid_hex() {
        let args = v(&["--hmac-key-hex", "zz"]);
        let err = parse_hex_flag(&args, "--hmac-key-hex").expect_err("invalid hex");
        assert!(err.contains("invalid --hmac-key-hex"));
    }
}
