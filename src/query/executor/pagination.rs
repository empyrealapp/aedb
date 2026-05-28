use super::cursor::CursorToken;
use crate::query::error::QueryError;
use crate::query::plan::Query;

/// 75% of the effective scan budget; once `rows_examined` crosses this
/// threshold we hint to the caller that they should paginate.
const SOFT_LIMIT_PCT_NUM: usize = 3;
const SOFT_LIMIT_PCT_DEN: usize = 4;

#[inline]
pub(in crate::query::executor) fn compute_split_recommended(
    rows_examined: usize,
    budget: usize,
) -> bool {
    if budget == 0 {
        return false;
    }
    // rows_examined * 4 >= budget * 3; integer-safe comparison for >= 75%.
    rows_examined
        .saturating_mul(SOFT_LIMIT_PCT_DEN)
        .ge(&budget.saturating_mul(SOFT_LIMIT_PCT_NUM))
}

/// Compute `remaining_limit` for the cursor we are about to emit.
///
/// If the user did not supply a `query.limit` and the incoming cursor did not
/// carry one either, the chain is uncapped and we propagate `None`. Otherwise
/// we subtract this page's returned rows from the smallest applicable cap.
#[inline]
pub(crate) fn compute_remaining_limit_after_page(
    query_limit: Option<usize>,
    cursor_remaining: Option<usize>,
    returned_this_page: usize,
) -> Option<usize> {
    match (query_limit, cursor_remaining) {
        (None, None) => None,
        (Some(q), None) => Some(q.saturating_sub(returned_this_page)),
        (None, Some(c)) => Some(c.saturating_sub(returned_this_page)),
        (Some(q), Some(c)) => Some(q.min(c).saturating_sub(returned_this_page)),
    }
}

pub(in crate::query::executor) struct PageWindow {
    pub page_size: usize,
    pub effective_page_size: usize,
    pub row_offset_count: usize,
    pub page_read_limit: usize,
    pub row_source_window_limit: usize,
}

pub(in crate::query::executor) fn compute_page_window(
    query: &Query,
    cursor_state: &Option<CursorToken>,
    max_scan_rows: usize,
) -> Result<PageWindow, QueryError> {
    let page_size = query.limit.unwrap_or_else(|| {
        cursor_state
            .as_ref()
            .map(|c| c.page_size)
            .unwrap_or(max_scan_rows.min(100))
    });
    let effective_page_size = page_size.min(max_scan_rows);
    let row_offset_count = query.offset.unwrap_or(0);
    let lookahead_row_count = usize::from(effective_page_size > 0);
    let page_read_limit = effective_page_size.checked_add(lookahead_row_count).ok_or(
        QueryError::ScanBoundExceeded {
            estimated_rows: u64::MAX,
            max_scan_rows: max_scan_rows as u64,
        },
    )?;
    let row_source_window_limit =
        row_offset_count
            .checked_add(page_read_limit)
            .ok_or(QueryError::ScanBoundExceeded {
                estimated_rows: u64::MAX,
                max_scan_rows: max_scan_rows as u64,
            })?;
    let requested_rows =
        row_offset_count
            .checked_add(effective_page_size)
            .ok_or(QueryError::ScanBoundExceeded {
                estimated_rows: u64::MAX,
                max_scan_rows: max_scan_rows as u64,
            })?;
    if requested_rows > max_scan_rows {
        return Err(QueryError::ScanBoundExceeded {
            estimated_rows: requested_rows as u64,
            max_scan_rows: max_scan_rows as u64,
        });
    }

    Ok(PageWindow {
        page_size,
        effective_page_size,
        row_offset_count,
        page_read_limit,
        row_source_window_limit,
    })
}
