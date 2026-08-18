use std::collections::BTreeSet;

use nx_sync::{LwwMap, NodeId, ORSet, Rga};
use proptest::prelude::*;

const PROPERTY_CASES: u32 = 256;

fn small_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..8)
}

#[derive(Clone, Debug)]
enum LwwAction {
    Set {
        field: u8,
        value: Vec<u8>,
        timestamp: u64,
        writer: u8,
    },
    Remove {
        field: u8,
        timestamp: u64,
        writer: u8,
    },
}

fn lww_actions() -> impl Strategy<Value = Vec<LwwAction>> {
    let action = prop_oneof![
        (0u8..4, small_bytes(), 0u64..32, 0u8..3).prop_map(|(field, value, timestamp, writer)| {
            LwwAction::Set {
                field,
                value,
                timestamp,
                writer,
            }
        },),
        (0u8..4, 0u64..32, 0u8..3).prop_map(|(field, timestamp, writer)| {
            LwwAction::Remove {
                field,
                timestamp,
                writer,
            }
        }),
    ];
    prop::collection::vec(action, 0..24)
}

fn apply_lww<'a>(actions: impl IntoIterator<Item = &'a LwwAction>) -> LwwMap {
    let mut map = LwwMap::new();
    for action in actions {
        match action {
            LwwAction::Set {
                field,
                value,
                timestamp,
                writer,
            } => {
                map.set(
                    format!("field-{field}"),
                    value.clone(),
                    *timestamp,
                    NodeId::new(format!("node-{writer}")),
                );
            }
            LwwAction::Remove {
                field,
                timestamp,
                writer,
            } => {
                map.remove(
                    format!("field-{field}"),
                    *timestamp,
                    NodeId::new(format!("node-{writer}")),
                );
            }
        }
    }
    map
}

#[derive(Clone, Debug)]
enum ORAction {
    Add(u8),
    Remove(u8),
}

fn or_actions() -> impl Strategy<Value = Vec<ORAction>> {
    prop::collection::vec(
        prop_oneof![
            (0u8..5).prop_map(ORAction::Add),
            (0u8..5).prop_map(ORAction::Remove),
        ],
        0..24,
    )
}

fn build_orset(actions: &[ORAction], namespace: &str) -> ORSet {
    let mut set = ORSet::new();
    for (index, action) in actions.iter().enumerate() {
        let element = match action {
            ORAction::Add(element) | ORAction::Remove(element) => format!("element-{element}"),
        };
        match action {
            ORAction::Add(_) => {
                set.add(element, format!("{namespace}-tag-{index}"));
            }
            ORAction::Remove(_) => {
                set.remove(&element);
            }
        }
    }
    set
}

#[derive(Clone, Debug)]
enum OREvent {
    Add { element: String, tag: String },
    Remove { element: String, tag: String },
}

fn or_events(specs: &[(u8, bool)]) -> Vec<OREvent> {
    let mut events = Vec::new();
    for (index, (element, remove)) in specs.iter().enumerate() {
        let element = format!("element-{element}");
        let tag = format!("tag-{index}");
        events.push(OREvent::Add {
            element: element.clone(),
            tag: tag.clone(),
        });
        if *remove {
            events.push(OREvent::Remove { element, tag });
        }
    }
    events
}

fn apply_or_events<'a>(events: impl IntoIterator<Item = &'a OREvent>) -> ORSet {
    let mut set = ORSet::new();
    for event in events {
        match event {
            OREvent::Add { element, tag } => {
                set.apply_add(element.clone(), tag.clone());
            }
            OREvent::Remove { element, tag } => {
                set.apply_remove(element.clone(), [tag.clone()]);
            }
        }
    }
    set
}

#[derive(Clone, Debug)]
struct RgaSpec {
    parent_selector: u8,
    value: Vec<u8>,
    delete: bool,
}

fn rga_specs() -> impl Strategy<Value = Vec<RgaSpec>> {
    prop::collection::vec(
        (any::<u8>(), small_bytes(), any::<bool>()).prop_map(|(parent_selector, value, delete)| {
            RgaSpec {
                parent_selector,
                value,
                delete,
            }
        }),
        0..24,
    )
}

#[derive(Clone, Debug)]
enum RgaEvent {
    Insert {
        id: String,
        parent: Option<String>,
        value: Vec<u8>,
    },
    Delete(String),
}

fn rga_events(specs: &[RgaSpec], namespace: &str) -> Vec<RgaEvent> {
    let mut events = Vec::new();
    let mut ids: Vec<String> = Vec::new();
    for (index, spec) in specs.iter().enumerate() {
        let id = format!("{namespace}-id-{index}");
        let parent = if index == 0 || usize::from(spec.parent_selector) % (index + 1) == index {
            None
        } else {
            Some(ids[usize::from(spec.parent_selector) % index].clone())
        };
        events.push(RgaEvent::Insert {
            id: id.clone(),
            parent,
            value: spec.value.clone(),
        });
        if spec.delete {
            events.push(RgaEvent::Delete(id.clone()));
        }
        ids.push(id);
    }
    events
}

fn apply_rga_events<'a>(events: impl IntoIterator<Item = &'a RgaEvent>) -> Rga {
    let mut rga = Rga::new();
    for event in events {
        match event {
            RgaEvent::Insert { id, parent, value } => {
                rga.apply_insert(id.clone(), parent.clone(), value.clone());
            }
            RgaEvent::Delete(id) => {
                rga.apply_delete(id.clone());
            }
        }
    }
    rga
}

fn build_rga(specs: &[RgaSpec], namespace: &str) -> Rga {
    let events = rga_events(specs, namespace);
    apply_rga_events(&events)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(PROPERTY_CASES))]

    #[test]
    fn lww_map_obeys_merge_laws(
        a_actions in lww_actions(),
        b_actions in lww_actions(),
        c_actions in lww_actions(),
    ) {
        let a = apply_lww(&a_actions);
        let b = apply_lww(&b_actions);
        let c = apply_lww(&c_actions);

        prop_assert_eq!(a.merged_with(&b), b.merged_with(&a));

        let left = a.merged_with(&b).merged_with(&c);
        let right = a.merged_with(&b.merged_with(&c));
        prop_assert_eq!(left, right);

        let mut idempotent = a.clone();
        prop_assert!(!idempotent.merge(&a));
        prop_assert_eq!(&idempotent, &a);

        let json = a.to_json()?;
        prop_assert_eq!(LwwMap::from_json(&json)?, a);
    }

    #[test]
    fn lww_map_converges_for_reversed_delivery(actions in lww_actions()) {
        let forward = apply_lww(&actions);
        let reverse = apply_lww(actions.iter().rev());
        prop_assert_eq!(forward, reverse);
    }

    #[test]
    fn orset_obeys_merge_laws(
        a_actions in or_actions(),
        b_actions in or_actions(),
        c_actions in or_actions(),
    ) {
        let a = build_orset(&a_actions, "a");
        let b = build_orset(&b_actions, "b");
        let c = build_orset(&c_actions, "c");

        prop_assert_eq!(a.merged_with(&b), b.merged_with(&a));

        let left = a.merged_with(&b).merged_with(&c);
        let right = a.merged_with(&b.merged_with(&c));
        prop_assert_eq!(left, right);

        let mut idempotent = a.clone();
        prop_assert!(!idempotent.merge(&a));
        prop_assert_eq!(&idempotent, &a);

        let json = a.to_json()?;
        prop_assert_eq!(ORSet::from_json(&json)?, a);
    }

    #[test]
    fn orset_converges_for_reversed_delivery(
        specs in prop::collection::vec((0u8..5, any::<bool>()), 0..24),
    ) {
        let events = or_events(&specs);
        let forward = apply_or_events(&events);
        let reverse = apply_or_events(events.iter().rev());
        prop_assert_eq!(forward, reverse);
    }

    #[test]
    fn rga_obeys_merge_laws(
        a_specs in rga_specs(),
        b_specs in rga_specs(),
        c_specs in rga_specs(),
    ) {
        let a = build_rga(&a_specs, "a");
        let b = build_rga(&b_specs, "b");
        let c = build_rga(&c_specs, "c");

        prop_assert_eq!(a.merged_with(&b), b.merged_with(&a));

        let left = a.merged_with(&b).merged_with(&c);
        let right = a.merged_with(&b.merged_with(&c));
        prop_assert_eq!(left, right);

        let mut idempotent = a.clone();
        prop_assert!(!idempotent.merge(&a));
        prop_assert_eq!(&idempotent, &a);

        let ids = a.ordered_ids();
        let unique_ids: BTreeSet<_> = ids.iter().collect();
        prop_assert_eq!(ids.len(), unique_ids.len());

        let json = a.to_json()?;
        prop_assert_eq!(Rga::from_json(&json)?, a);
    }

    #[test]
    fn rga_converges_for_reversed_delivery(specs in rga_specs()) {
        let events = rga_events(&specs, "history");
        let forward = apply_rga_events(&events);
        let reverse = apply_rga_events(events.iter().rev());
        prop_assert_eq!(forward, reverse);
    }
}
