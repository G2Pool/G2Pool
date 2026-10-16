use crate::objects::Key;
use std::{collections::HashSet, sync::Arc};

#[derive(Default)]
pub(super) struct Trace {
    pub(super) keys: Vec<Key>,
    pub(super) children: Vec<Arc<Trace>>,
}
impl Trace {
    pub(super) fn leaf(key: Key) -> Arc<Self> {
        Arc::new(Self {
            keys: vec![key],
            children: vec![],
        })
    }
    pub(super) fn join(children: impl IntoIterator<Item = Arc<Self>>) -> Arc<Self> {
        let mut seen = HashSet::new();
        let children = children
            .into_iter()
            .filter(|child| seen.insert(Arc::as_ptr(child) as usize))
            .collect();
        Arc::new(Self {
            keys: vec![],
            children,
        })
    }
    pub(super) fn own_metadata(&self) -> usize {
        std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + self.keys.capacity() * std::mem::size_of::<Key>()
            + self.children.capacity() * std::mem::size_of::<Arc<Self>>()
    }
}
