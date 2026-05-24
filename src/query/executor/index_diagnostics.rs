pub(super) fn selected_indexes_if_diagnostic(
    include_diagnostics: bool,
    index_name: &str,
) -> Vec<String> {
    if include_diagnostics {
        vec![index_name.to_string()]
    } else {
        Vec::new()
    }
}

pub(super) fn plan_trace_if_diagnostic<F>(include_diagnostics: bool, trace: F) -> Vec<String>
where
    F: FnOnce() -> String,
{
    if include_diagnostics {
        vec![trace()]
    } else {
        Vec::new()
    }
}

pub(super) fn merge_selected_indexes_if_diagnostic(
    include_diagnostics: bool,
    left: Vec<String>,
    right: Vec<String>,
) -> Vec<String> {
    if !include_diagnostics {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(left.len() + right.len());
    for name in left.into_iter().chain(right) {
        if !out.contains(&name) {
            out.push(name);
        }
    }
    out
}

pub(super) fn merge_trace_if_diagnostic(
    include_diagnostics: bool,
    header: &str,
    mut left: Vec<String>,
    right: Vec<String>,
) -> Vec<String> {
    if !include_diagnostics {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(1 + left.len() + right.len());
    out.push(header.to_string());
    out.append(&mut left);
    out.extend(right);
    out
}

pub(super) fn merge_trace_single_if_diagnostic(
    include_diagnostics: bool,
    header: &str,
    mut trace: Vec<String>,
) -> Vec<String> {
    if !include_diagnostics {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(1 + trace.len());
    out.push(header.to_string());
    out.append(&mut trace);
    out
}
