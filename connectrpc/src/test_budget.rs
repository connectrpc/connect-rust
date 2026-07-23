//! Sizing helper for tests that need to overrun buffa's element-memory budget.

/// Repeated-element count that overruns buffa's default element-memory
/// budget when decoded as `T`.
///
/// The budget charges `size_of::<T>()` per element, so a fixture written as a
/// literal count silently stops overrunning it the moment `T` shrinks. buffa
/// 0.9.1 did exactly that — moving view unknown-field state behind a pointer
/// took `ValueView` from 48 bytes to 32, and a hardcoded 800_000 fell from
/// 1.14x the budget to 0.76x, so tests asserting a rejection began decoding
/// successfully. Deriving the count from the live size cannot go stale that
/// way.
///
/// The charge is a deterministic `n * size_of::<T>()`, so strictly the count
/// only has to clear the division remainder. The quarter of headroom is margin
/// against buffa retuning the default or the per-element charge; the exact
/// multiple does not matter, only that it is safely over.
///
/// Pass the **smallest** type any decode in the test materialises. Where a
/// test has both a control and a subject they often differ — an owned `Value`
/// is 48 bytes against `ValueView`'s 32 — and only the smaller clears the
/// budget for both. Sizing by the larger leaves the view decode at 0.83x, and
/// a test written that way passes with the behaviour it checks deleted
/// outright.
pub(crate) fn elements_over_default_budget<T>() -> usize {
    let per_element = std::mem::size_of::<T>().max(1);
    (buffa::DEFAULT_ELEMENT_MEMORY_LIMIT / per_element) * 5 / 4
}
