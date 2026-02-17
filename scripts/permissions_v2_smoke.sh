#!/usr/bin/env bash
set -euo pipefail

echo "[permissions-v2] cargo check"
cargo check

echo "[permissions-v2] targeted authz tests"
cargo test kv_query_apis_enforce_permissions_and_paginate -- --nocapture
cargo test kv_prefix_permissions_scope_reads_and_writes -- --nocapture
cargo test kv_query_apis_are_scope_aware -- --nocapture
cargo test scoped_admin_permissions_bound_grant_revoke -- --nocapture
cargo test project_owner_and_delegable_grants_control_authz_delegation -- --nocapture
cargo test authz_grant_revoke_events_are_persisted_in_system_audit_table -- --nocapture
cargo test read_policies_filter_rows_for_caller_and_can_be_cleared -- --nocapture

echo "[permissions-v2] durability/query safety net"
cargo test --test query_integration
cargo test --test backup_restore
cargo test --test crash_matrix
cargo test --test stress

echo "[permissions-v2] smoke complete"
