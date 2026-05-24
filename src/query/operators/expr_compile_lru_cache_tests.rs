use super::{CompiledExpr, ExprCacheKey, ExprCompileLruCache};

fn key(name: &str) -> ExprCacheKey {
    (
        crate::query::plan::Expr::IsNull(name.to_string()),
        vec!["c".to_string()],
        "t".to_string(),
    )
}

#[test]
fn evicts_oldest_entry_when_capacity_reached() {
    let mut cache = ExprCompileLruCache::new(2);
    cache.put(key("a"), CompiledExpr::IsNull(0));
    cache.put(key("b"), CompiledExpr::IsNull(1));
    cache.put(key("c"), CompiledExpr::IsNull(2));

    let ka = key("a");
    let kb = key("b");
    let kc = key("c");
    assert!(cache.get(&ka.0, &ka.1, &ka.2).is_none());
    assert_eq!(
        cache.get(&kb.0, &kb.1, &kb.2),
        Some(CompiledExpr::IsNull(1))
    );
    assert_eq!(
        cache.get(&kc.0, &kc.1, &kc.2),
        Some(CompiledExpr::IsNull(2))
    );
}

#[test]
fn hit_promotes_entry_and_prevents_eviction() {
    let mut cache = ExprCompileLruCache::new(2);
    cache.put(key("a"), CompiledExpr::IsNull(0));
    cache.put(key("b"), CompiledExpr::IsNull(1));

    let ka = key("a");
    let kb = key("b");
    assert_eq!(
        cache.get(&ka.0, &ka.1, &ka.2),
        Some(CompiledExpr::IsNull(0))
    );
    cache.put(key("c"), CompiledExpr::IsNull(2));

    let kc = key("c");
    assert_eq!(
        cache.get(&ka.0, &ka.1, &ka.2),
        Some(CompiledExpr::IsNull(0))
    );
    assert!(cache.get(&kb.0, &kb.1, &kb.2).is_none());
    assert_eq!(
        cache.get(&kc.0, &kc.1, &kc.2),
        Some(CompiledExpr::IsNull(2))
    );
}
