# Authorization Model

AEDB authorization is caller-scoped. Authenticated APIs accept a `CallerContext`; secure mode requires authenticated `*_as` APIs and disables no-auth read/write shortcuts.

## Caller Contexts

- Anonymous context: APIs without a `CallerContext`. These are allowed only when the instance does not require authenticated calls.
- External caller: `CallerContext::new("caller-id")`. The ID must be non-empty and must not be the reserved `system` caller.
- Internal system caller: trusted internal paths can use an internal `system` caller flag that cannot be deserialized from untrusted input. External callers cannot claim `caller_id = "system"`.

Relevant checks:

- `src/lib_helpers.rs::ensure_external_caller_allowed`
- `src/lib_helpers.rs::ensure_query_caller_allowed`
- `src/permission.rs::CallerContext`
- `src/commit/executor/tests.rs::trusted_submit_requires_system_caller`
- `src/lib_tests.rs::query_with_options_as_rejects_reserved_system_caller`

## Secure Mode

`AedbInstance::open_secure(...)` validates hardened storage settings and requires authenticated calls for caller-facing APIs. In secure mode:

- anonymous `commit`, `commit_with_preflight`, `query`, `query_with_options`, `kv_get_no_auth`, and `kv_scan_prefix_no_auth` are rejected;
- `commit_as`, `commit_as_with_preflight`, `commit_envelope` with `envelope.caller`, and `query_with_options_as` remain available subject to permissions;
- reactive processors require an explicit `caller_id`.

Tests:

- `src/lib_tests.rs::secure_profile_requires_hardened_storage_settings`
- `src/lib_tests.rs::secure_mode_requires_authenticated_apis`
- `src/lib_tests.rs::kv_no_auth_apis_in_secure_mode_return_structured_error`
- `src/lib_tests.rs::reactive_processor_scheduler_requires_caller_id_in_secure_mode`
- `tests/security_properties.rs::security_secure_mode_enforces_authenticated_commit_calls`

## Permission Types

Permissions are defined in `src/permission.rs`.

| Permission | Scope |
| --- | --- |
| `GlobalAdmin` | All projects/resources. Intended for bootstrap and break-glass paths. |
| `ProjectAdmin { project_id }` | Project and its scopes/tables/indexes. Does not imply global privileges. |
| `ScopeAdmin { project_id, scope_id }` | One scope and its tables/indexes. Does not spill across scopes. |
| `TableDdl { project_id }` | Schema, read-policy, and projection DDL in a project. |
| `TableRead { project_id, scope_id, table_name }` | Reads from one table. |
| `TableWrite { project_id, scope_id, table_name }` | Writes to one table. |
| `IndexRead { project_id, scope_id, table_name, index_name }` | Reads through one secondary index. |
| `KvRead { project_id, scope_id, prefix }` | KV reads, optionally narrowed by exact scope and binary key prefix. |
| `KvWrite { project_id, scope_id, prefix }` | KV writes, with the same scope/prefix semantics as `KvRead`. |
| `PolicyBypass { project_id, table_name }` | Bypasses row-level read policy for a project or one table. |

KV prefix matching is byte-level. `prefix: None` grants all keys in the scope domain; `prefix: Some(bytes)` grants keys that start with those bytes. `scope_id: None` is project-wide; `scope_id: Some(id)` is exact-scope only.

## Ownership And Delegation

Projects, scopes, and tables can have `owner_id`.

- Project owners can administer project-level grants and transfer project ownership.
- Scope owners can administer permissions for that scope.
- Table owners can administer table-scoped read/write/index/policy-bypass permissions for their table.
- Delegable grants can authorize a caller to grant/revoke that same permission.
- Ownership transfer requires the relevant owner or `GlobalAdmin`.

Tests:

- `src/lib_tests.rs::project_owner_and_delegable_grants_control_authz_delegation`
- `src/lib_tests.rs::grant_and_revoke_authorization_matrix_is_enforced`
- `src/lib_tests.rs::ownership_transfer_requires_owner_or_global_admin`
- `src/lib_tests.rs::authz_grant_revoke_events_are_persisted_in_system_audit_table`

## Table, KV, And Index Enforcement

- Table mutations require `TableWrite` for the target table, unless an admin permission implies it.
- Queries require `TableRead` for the base table and every joined table.
- Indexed queries require `TableRead`; async index APIs also require `IndexRead` where the API specifically exposes index access.
- KV point reads, prefix scans, and range scans require matching `KvRead`.
- KV writes and event emits require matching `KvWrite`.
- Assertions inside authenticated envelopes require read permission for the asserted KV key or table.

Tests:

- `src/lib_tests.rs::permissions_enforced_at_preflight_commit_and_query`
- `src/lib_tests.rs::join_queries_require_permissions_for_all_referenced_tables`
- `src/lib_tests.rs::kv_query_apis_enforce_permissions_and_paginate`
- `src/lib_tests.rs::kv_permissions_scope_reads_and_writes_with_prefix_filters`
- `src/lib_tests.rs::postflight_assertions_require_read_permission_in_secure_mode`
- `src/lib_tests.rs::envelope_assertions_require_read_permission_in_secure_mode`
- `src/lib_tests.rs::async_index_query_requires_table_read_permission`
- `src/lib_tests.rs::event_stream_and_processor_lag_as_require_explicit_permissions`

## Row Policies And Policy Bypass

Table read policies are bound to the caller. The `$caller_id` placeholder is substituted during query authorization. Policies apply to joined tables as well as the base table.

`PolicyBypass` skips row-policy filtering but does not replace `TableRead`; a caller still needs table read permission.

Tests:

- `src/lib_tests.rs::read_policies_filter_rows_for_caller_and_can_be_cleared`
- `src/lib_tests.rs::read_policy_like_patterns_substitute_caller_id`
- `src/lib_tests.rs::read_policy_applies_to_joined_tables`
- `src/lib_tests.rs::table_policy_bypass_permission_skips_row_policy_filtering`
- `src/lib_tests.rs::project_policy_bypass_permission_skips_joined_table_policies`
- `src/lib_tests.rs::delete_where_respects_row_level_read_policy`

## Allowed And Denied Examples

Allowed:

```rust
// Assumes "writer" has already been granted TableWrite for p.app.orders.
let caller = CallerContext::new("writer");
db.commit_as(caller, Mutation::Upsert {
    project_id: "p".into(),
    scope_id: "app".into(),
    table_name: "orders".into(),
    primary_key,
    row,
}).await?;
```

Denied:

```rust
let caller = CallerContext::new("reader");
let err = db.commit_as(caller, Mutation::Upsert {
    project_id: "p".into(),
    scope_id: "app".into(),
    table_name: "orders".into(),
    primary_key,
    row,
}).await.expect_err("reader cannot write orders without TableWrite");
assert!(matches!(err, AedbError::PermissionDenied(_)));
```

Allowed KV prefix:

```rust
Permission::KvRead {
    project_id: "p".into(),
    scope_id: Some("app".into()),
    prefix: Some(b"tenant/a/".to_vec()),
}
```

This permits reading `tenant/a/order-1` in `p.app`, but denies `tenant/b/order-1` and the same key in another scope.

Denied reserved caller:

```rust
db.query_with_options_as(
    Some(&CallerContext::new("system")),
    "p",
    "app",
    query,
    Default::default(),
).await.expect_err("system is reserved for internal trusted paths");
```

## Synchronization With Tests

When changing authorization semantics, update this document and run:

```bash
cargo test --lib grant_and_revoke_authorization_matrix_is_enforced -- --test-threads=1
cargo test --lib permissions_enforced_at_preflight_commit_and_query -- --test-threads=1
cargo test --lib kv_permissions_scope_reads_and_writes_with_prefix_filters -- --test-threads=1
cargo test --lib read_policies_filter_rows_for_caller_and_can_be_cleared -- --test-threads=1
cargo test --test security_properties -- --test-threads=1
```
