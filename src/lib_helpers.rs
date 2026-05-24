use crate::SYSTEM_CALLER_ID;
use crate::error::AedbError;
use crate::permission::Permission;
use std::collections::BTreeSet;

#[cfg(test)]
pub(crate) use crate::backup_chain::{
    read_segments, read_segments_for_checkpoint, scan_segment_seq_range,
};
#[cfg(test)]
pub(crate) use crate::config_validation::{validate_arcana_config, validate_config};
pub(crate) use crate::ddl_lifecycle::{
    LifecycleEventTemplate, ddl_would_apply, lifecycle_template_for_ddl,
    lifecycle_templates_for_mutation,
};
pub(crate) use crate::query_authorization::{
    authorize_and_bind_query_for_caller, bind_policy_expr,
};
pub(crate) use crate::query_runtime::resolve_query_table_ref;

pub(crate) fn seed_system_global_admin(catalog: &mut crate::catalog::Catalog) {
    let mut perms = catalog
        .permissions
        .get(SYSTEM_CALLER_ID)
        .cloned()
        .unwrap_or_else(BTreeSet::new);
    perms.insert(Permission::GlobalAdmin);
    catalog.permissions.insert(SYSTEM_CALLER_ID.into(), perms);
}

pub(crate) fn should_fallback_to_recovery(err: &AedbError) -> bool {
    match err {
        AedbError::Validation(msg) => {
            msg.contains("garbage collected") || msg.contains("not found in version store")
        }
        _ => false,
    }
}

pub(crate) fn next_prefix_bytes(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = prefix.to_vec();
    for i in (0..bytes.len()).rev() {
        if bytes[i] != u8::MAX {
            bytes[i] += 1;
            bytes.truncate(i + 1);
            return Some(bytes);
        }
    }
    None
}
