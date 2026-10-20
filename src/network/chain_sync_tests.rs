use super::*;
use std::collections::BTreeMap;

fn id(n: u8) -> Id {
    [n; 32]
}
fn header(tag: u8, parent: Option<Id>, height: u64, work: u64, cumulative: u128) -> Header {
    Header {
        id: id(tag),
        parent,
        height,
        credited_work: u128::from(work),
        cumulative_work: cumulative,
        parent_state_root: parent
            .map(|parent| id(parent[0].wrapping_add(100)))
            .unwrap_or([0; 32]),
        state_root: id(tag.wrapping_add(100)),
    }
}
struct Index(BTreeMap<Id, Header>);
impl ChainIndex for Index {
    fn header(&self, id: Id) -> Result<Option<Header>> {
        Ok(self.0.get(&id).copied())
    }
    fn ancestor(&self, mut tip: Id, height: u64) -> Result<Option<Header>> {
        let current = self.0.get(&tip).copied().ok_or(Error::Missing(tip))?;
        if height > current.height {
            return Ok(None);
        }
        while self.0.get(&tip).unwrap().height > height {
            tip = self
                .0
                .get(&tip)
                .unwrap()
                .parent
                .ok_or(Error::Invalid("ancestor root"))?;
        }
        Ok(self.0.get(&tip).copied())
    }
}
fn index() -> Index {
    let root = header(1, None, 0, 5, 5);
    let a = header(2, Some(root.id), 1, 7, 12);
    let b = header(3, Some(a.id), 2, 11, 23);
    let fork = header(4, Some(a.id), 2, 13, 25);
    Index(BTreeMap::from([
        (root.id, root),
        (a.id, a),
        (b.id, b),
        (fork.id, fork),
    ]))
}
#[test]
fn locator_and_common_ancestor_are_branch_relative() {
    let index = index();
    let locator = vec![id(3), id(2), id(1)];
    assert_eq!(locator[0], id(3));
    assert_eq!(
        common_ancestor(&index, id(4), &locator)
            .unwrap()
            .unwrap()
            .id,
        id(2)
    );
    let page = header_range(&index, id(3), id(2), 8).unwrap();
    assert_eq!(page.headers, vec![index.0[&id(3)]]);
}
#[test]
fn durable_lmdb_snapshot_is_a_chain_index_for_sync() {
    let dir = tempfile::tempdir().unwrap();
    let db =
        crate::storage::sharechain::SharechainDb::open(dir.path(), 1 << 24, 4096, id(240)).unwrap();
    let root = id(1);
    let child = id(2);
    let tip = id(3);
    let root_header = crate::storage::sharechain::Header {
        parent: None,
        height: 0,
        work: 5,
        cumulative_work: 5,
        anchor: id(90),
        parent_state_root: [0; 32],
        state_root: id(101),
    };
    let child_header = crate::storage::sharechain::Header {
        parent: Some(root),
        height: 1,
        work: 7,
        cumulative_work: 12,
        anchor: id(90),
        parent_state_root: id(101),
        state_root: id(102),
    };
    let tip_header = crate::storage::sharechain::Header {
        parent: Some(child),
        height: 2,
        work: 11,
        cumulative_work: 23,
        anchor: id(90),
        parent_state_root: id(102),
        state_root: id(103),
    };
    let body = [0u8];
    let delta = [1u8];
    let records = [
        crate::storage::sharechain::Record {
            id: root,
            header: root_header,
            body: &body,
            delta: &delta,
            state: &[],
        },
        crate::storage::sharechain::Record {
            id: child,
            header: child_header,
            body: &body,
            delta: &delta,
            state: &[],
        },
        crate::storage::sharechain::Record {
            id: tip,
            header: tip_header,
            body: &body,
            delta: &delta,
            state: &[],
        },
    ];
    db.publish_validated(0, &records, tip).unwrap();
    let snapshot = db.read().unwrap();
    let locator = snapshot.locator(tip, MAX_LOCATOR).unwrap();
    assert_eq!(locator, vec![tip, child, root]);
    let page = header_range(&snapshot, tip, root, MAX_HEADERS).unwrap();
    assert_eq!(page.headers.len(), 2);
    assert_eq!(page.headers[1].cumulative_work, 23);
}
