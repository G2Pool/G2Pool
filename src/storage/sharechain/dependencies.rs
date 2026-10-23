use crate::storage::sharechain::{Error, Id, Result, RoutingKey};
use heed::{types::Bytes, Database, Env, RoTxn, RwTxn, WithoutTls};

const KEY_BITS: usize = 264;
pub struct DependencyManifest<'a> {
    pub id: Id,
    pub parent: Option<Id>,
    pub added: &'a [RoutingKey],
    pub removed: &'a [RoutingKey],
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DependencyGcStats {
    pub processed_nodes: usize,
    pub reclaimed_nodes: usize,
    pub pending_nodes: u64,
}
#[derive(Clone)]
pub(crate) struct DependencyDb {
    nodes: Database<Bytes, Bytes>,
    gc: Database<Bytes, Bytes>,
    refs: Database<Bytes, Bytes>,
    roots: Database<Bytes, Bytes>,
    manifest_digests: Database<Bytes, Bytes>,
}
#[derive(Clone)]
struct TrieNode {
    branch: bool,
    depth: u16,
    key: RoutingKey,
    children: Vec<Id>,
}
impl TrieNode {
    fn leaf(key: RoutingKey) -> Self {
        Self {
            branch: false,
            depth: KEY_BITS as u16,
            key,
            children: vec![],
        }
    }
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![u8::from(self.branch)];
        out.extend(self.depth.to_be_bytes());
        out.extend(self.key.bytes());
        for child in &self.children {
            out.extend(child);
        }
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 36 || bytes[0] > 1 {
            return Err(Error::Invalid("dependency trie node"));
        }
        let branch = bytes[0] == 1;
        let depth = u16::from_be_bytes(bytes[1..3].try_into().unwrap());
        if (branch && (bytes.len() != 100 || depth as usize >= KEY_BITS))
            || (!branch && (bytes.len() != 36 || depth as usize != KEY_BITS))
        {
            return Err(Error::Invalid("dependency trie shape"));
        }
        let key = RoutingKey::decode(&bytes[3..36])?;
        let children: Vec<Id> = if branch {
            bytes[36..].as_chunks::<32>().0.to_vec()
        } else {
            vec![]
        };
        if branch && (children.contains(&[0; 32]) || prefix(key, depth as usize) != key) {
            return Err(Error::Invalid("dependency trie branch"));
        }
        Ok(Self {
            branch,
            depth,
            key,
            children,
        })
    }
}

fn node_hash(b: &[u8]) -> Id {
    crate::codec::hash("DEPENDENCY_TRIE_NODE", b)
}
fn count(db: Database<Bytes, Bytes>, tx: &RoTxn<'_, WithoutTls>, k: &[u8]) -> Result<u64> {
    match db.get(tx, k)? {
        None => Ok(0),
        Some(v) if v.len() == 8 => Ok(u64::from_be_bytes(v.try_into().unwrap())),
        _ => Err(Error::Invalid("dependency refcount encoding")),
    }
}
fn change(db: Database<Bytes, Bytes>, tx: &mut RwTxn<'_>, k: &[u8], add: bool) -> Result<u64> {
    let n = count(db, tx, k)?;
    let n = if add {
        n.checked_add(1)
    } else {
        n.checked_sub(1)
    }
    .ok_or(Error::Invalid("dependency refcount overflow or underflow"))?;
    if n == 0 {
        db.delete(tx, k)?;
    } else {
        db.put(tx, k, &n.to_be_bytes())?;
    };
    Ok(n)
}
fn sorted(k: &[RoutingKey]) -> Result<Vec<RoutingKey>> {
    let mut x = k.to_vec();
    x.sort_unstable();
    if x.windows(2).any(|w| w[0] == w[1]) {
        return Err(Error::Invalid("duplicate dependency"));
    }
    Ok(x)
}
fn bit(key: RoutingKey, depth: usize) -> usize {
    let bytes = key.bytes();
    ((bytes[depth / 8] >> (7 - depth % 8)) & 1) as usize
}
fn prefix(key: RoutingKey, depth: usize) -> RoutingKey {
    let mut bytes = key.bytes();
    for d in depth..KEY_BITS {
        bytes[d / 8] &= !(1 << (7 - d % 8));
    }
    RoutingKey::decode(&bytes).unwrap()
}

impl DependencyDb {
    pub(crate) fn open_runtime(env: &Env<WithoutTls>, tx: &mut RwTxn<'_>) -> Result<Self> {
        Ok(Self {
            nodes: env.create_database(tx, Some("runtime_trie_nodes"))?,
            gc: env.create_database(tx, Some("runtime_trie_gc"))?,
            refs: env.create_database(tx, Some("runtime_trie_refs"))?,
            roots: env.create_database(tx, Some("runtime_trie_roots"))?,
            manifest_digests: env.create_database(tx, Some("runtime_trie_manifests"))?,
        })
    }
    pub(crate) fn protected_key(
        &self,
        tx: &RoTxn<'_, WithoutTls>,
        key: RoutingKey,
    ) -> Result<bool> {
        Ok(self
            .nodes
            .get(tx, &node_hash(&TrieNode::leaf(key).encode()))?
            .is_some())
    }
    fn node(&self, tx: &RoTxn<'_, WithoutTls>, id: &Id) -> Result<TrieNode> {
        let encoded = self.nodes.get(tx, id)?.ok_or(Error::Missing)?;
        if node_hash(encoded) != *id {
            return Err(Error::Invalid("dependency trie node hash"));
        }
        let node = TrieNode::decode(encoded)?;
        for (side, child) in node.children.iter().enumerate() {
            let encoded = self.nodes.get(tx, child)?.ok_or(Error::Missing)?;
            if node_hash(encoded) != *child {
                return Err(Error::Invalid("dependency trie child hash"));
            }
            let child = TrieNode::decode(encoded)?;
            if child.depth <= node.depth
                || prefix(child.key, node.depth as usize) != node.key
                || bit(child.key, node.depth as usize) != side
            {
                return Err(Error::Invalid("dependency trie edge"));
            }
        }
        Ok(node)
    }
    pub(crate) fn retained(&self, tx: &RoTxn<'_, WithoutTls>, id: &Id) -> Result<bool> {
        Ok(self.roots.get(tx, id)?.is_some())
    }
    pub(crate) fn root(&self, tx: &RoTxn<'_, WithoutTls>, id: &Id) -> Result<Id> {
        self.roots
            .get(tx, id)?
            .ok_or(Error::Missing)
            .and_then(|x| x.try_into().map_err(|_| Error::Invalid("dependency root")))
    }
    fn has(&self, tx: &RoTxn<'_, WithoutTls>, root: &Id, k: RoutingKey) -> Result<bool> {
        let mut id = *root;
        for d in 0..=KEY_BITS {
            if id == [0; 32] {
                return Ok(false);
            };
            let n = self.node(tx, &id)?;
            if (n.depth as usize) < d {
                return Err(Error::Invalid("dependency trie depth"));
            }
            if !n.branch {
                return Ok(n.key == k);
            };
            if d == KEY_BITS {
                return Err(Error::Invalid("dependency trie depth"));
            }
            id = n.children[bit(k, n.depth as usize)]
        }
        Ok(false)
    }
    fn resolve_rec(
        &self,
        tx: &RoTxn<'_, WithoutTls>,
        id: &Id,
        out: &mut Vec<RoutingKey>,
        depth: usize,
    ) -> Result<()> {
        if *id == [0; 32] {
            return Ok(());
        };
        let n = self.node(tx, id)?;
        if depth > KEY_BITS || (n.depth as usize) < depth {
            return Err(Error::Invalid("dependency trie depth"));
        }
        if n.branch {
            for c in n.children {
                self.resolve_rec(tx, &c, out, n.depth as usize + 1)?
            }
        } else {
            out.push(n.key)
        };
        Ok(())
    }
    pub(crate) fn resolve(&self, tx: &RoTxn<'_, WithoutTls>, id: &Id) -> Result<Vec<RoutingKey>> {
        let r = self.root(tx, id)?;
        let mut o = vec![];
        self.resolve_rec(tx, &r, &mut o, 0)?;
        Ok(o)
    }
    fn put_node(&self, tx: &mut RwTxn<'_>, n: &TrieNode) -> Result<Id> {
        let enc = n.encode();
        let id = node_hash(&enc);
        if id == [0; 32] {
            return Err(Error::Invalid("dependency trie reserved hash"));
        }
        if self.nodes.get(tx, &id)?.is_none() {
            self.nodes.put(tx, &id, &enc)?;
            for c in &n.children {
                if *c != [0; 32] {
                    change(self.refs, tx, c, true)?;
                }
            }
        }
        Ok(id)
    }
    fn update(
        &self,
        tx: &mut RwTxn<'_>,
        old: Id,
        k: RoutingKey,
        add: bool,
        d: usize,
    ) -> Result<Id> {
        if old == [0; 32] {
            if !add {
                return Err(Error::Invalid("dependency removal absent"));
            }
            return self.put_node(tx, &TrieNode::leaf(k));
        }
        let mut node = self.node(tx, &old)?;
        let split = node.depth as usize;
        if split < d {
            return Err(Error::Invalid("dependency trie depth"));
        }
        let existing = node.key;
        let divergence = (d..split).find(|&b| bit(k, b) != bit(existing, b));
        if let Some(at) = divergence {
            if !add {
                return Err(Error::Invalid("dependency removal absent"));
            }
            let leaf = self.put_node(tx, &TrieNode::leaf(k))?;
            let mut children = vec![old; 2];
            children[bit(k, at)] = leaf;
            return self.put_node(
                tx,
                &TrieNode {
                    branch: true,
                    depth: at as u16,
                    key: prefix(k, at),
                    children,
                },
            );
        }
        if !node.branch {
            if add {
                return Err(Error::Invalid("dependency already present"));
            }
            return Ok([0; 32]);
        }
        let side = bit(k, split);
        node.children[side] = self.update(tx, node.children[side], k, add, split + 1)?;
        if node.children[side] == [0; 32] {
            return Ok(node.children[1 - side]);
        }
        self.put_node(tx, &node)
    }

    fn drop_node(&self, tx: &mut RwTxn<'_>, id: Id) -> Result<()> {
        if id != [0; 32] && change(self.refs, tx, &id, false)? == 0 {
            self.gc.put(tx, &id, &[])?;
        }
        Ok(())
    }
    pub(crate) fn drain_gc(
        &self,
        tx: &mut RwTxn<'_>,
        max_nodes: usize,
    ) -> Result<DependencyGcStats> {
        let mut stats = DependencyGcStats::default();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
        for _ in 0..max_nodes {
            if stats.processed_nodes > 0 && std::time::Instant::now() >= deadline {
                break;
            }
            let entry = self
                .gc
                .iter(tx)?
                .next()
                .transpose()?
                .map(|(key, value)| (key.to_vec(), value.to_vec()));
            let Some((key, value)) = entry else { break };
            let id: Id = key
                .as_slice()
                .try_into()
                .map_err(|_| Error::Invalid("dependency gc key"))?;
            if id == [0; 32] || !value.is_empty() {
                return Err(Error::Invalid("dependency gc entry"));
            }
            let node = self.node(tx, &id)?;
            stats.processed_nodes += 1;
            if count(self.refs, tx, &id)? != 0 {
                self.gc.delete(tx, &id)?;
                continue;
            }
            if node.branch {
                for child in &node.children {
                    if count(self.refs, tx, child)? == 0 {
                        return Err(Error::Invalid("dependency gc child refcount"));
                    }
                }
                for child in node.children {
                    self.drop_node(tx, child)?;
                }
            }
            self.nodes.delete(tx, &id)?;
            self.gc.delete(tx, &id)?;
            stats.reclaimed_nodes += 1;
        }
        stats.pending_nodes = self.gc.len(tx)?;
        Ok(stats)
    }
    pub(crate) fn release(&self, tx: &mut RwTxn<'_>, id: &Id) -> Result<()> {
        let root = self
            .root(tx, id)
            .map_err(|_| Error::Invalid("dependency root not retained"))?;
        self.roots.delete(tx, id)?;
        self.manifest_digests.delete(tx, id)?;
        self.drop_node(tx, root)
    }

    pub(crate) fn publish(&self, tx: &mut RwTxn<'_>, m: &DependencyManifest<'_>) -> Result<()> {
        let a = sorted(m.added)?;
        let r = sorted(m.removed)?;
        if a.iter().any(|k| r.binary_search(k).is_ok()) || m.parent == Some(m.id) {
            return Err(Error::Invalid("invalid dependency delta"));
        };
        let mut raw = b"dependency-manifest".to_vec();
        raw.push(u8::from(m.parent.is_some()));
        raw.extend(m.parent.unwrap_or([0; 32]));
        for keys in [&a, &r] {
            raw.extend((keys.len() as u64).to_be_bytes());
            for k in keys {
                raw.extend(k.bytes());
            }
        }
        let dg = crate::codec::hash("DEPENDENCY_MANIFEST", &raw);
        if let Some(x) = self.manifest_digests.get(tx, &m.id)? {
            if x != dg {
                return Err(Error::Invalid("immutable dependency manifest changed"));
            }
        } else {
            self.manifest_digests.put(tx, &m.id, &dg)?
        };
        if self.roots.get(tx, &m.id)?.is_some() {
            return Ok(());
        };
        let old = m
            .parent
            .map(|p| self.root(tx, &p))
            .transpose()?
            .unwrap_or([0; 32]);
        for k in &a {
            if self.has(tx, &old, *k)? {
                return Err(Error::Invalid("dependency addition"));
            }
        }
        for k in &r {
            if !self.has(tx, &old, *k)? {
                return Err(Error::Invalid("dependency removal absent"));
            }
        }
        let mut root = old;
        if root != [0; 32] {
            change(self.refs, tx, &root, true)?;
        }
        for (key, add) in r
            .into_iter()
            .map(|k| (k, false))
            .chain(a.into_iter().map(|k| (k, true)))
        {
            let next = self.update(tx, root, key, add, 0)?;
            if next != [0; 32] {
                change(self.refs, tx, &next, true)?;
            }
            self.drop_node(tx, root)?;
            root = next;
        }
        self.roots.put(tx, &m.id, &root)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "dependencies_tests.rs"]
mod tests;
