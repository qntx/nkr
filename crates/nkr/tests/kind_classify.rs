//! `vectors/kind-classify.json` runner.

#![allow(
    unused_crate_dependencies,
    reason = "each test binary uses a subset of the workspace dependencies"
)]
#![allow(
    clippy::tests_outside_test_module,
    reason = "each file is a single vector runner or property test"
)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "vector fixtures may panic on failure"
)]
#![allow(
    clippy::shadow_unrelated,
    reason = "the loop variables intentionally shadow per-case bindings"
)]

mod common;

use nkr::kind::{Kind, KindClass};
use serde::Deserialize;

#[allow(
    clippy::struct_excessive_bools,
    reason = "the vector schema is four flags"
)]
#[derive(Deserialize)]
struct Case {
    kind: u16,
    regular: bool,
    replaceable: bool,
    ephemeral: bool,
    addressable: bool,
    class: String,
}

#[test]
fn kind_classify() {
    let cases = common::cases::<Case>("kind-classify.json");
    assert_eq!(cases.len(), 18, "vector count drift");
    for case in &cases {
        let kind = Kind(case.kind);
        assert_eq!(
            kind.is_regular(),
            case.regular,
            "kind {} regular",
            case.kind
        );
        assert_eq!(
            kind.is_replaceable(),
            case.replaceable,
            "kind {} replaceable",
            case.kind
        );
        assert_eq!(
            kind.is_ephemeral(),
            case.ephemeral,
            "kind {} ephemeral",
            case.kind
        );
        assert_eq!(
            kind.is_addressable(),
            case.addressable,
            "kind {} addressable",
            case.kind
        );
        let class = match case.class.as_str() {
            "regular" => KindClass::Regular,
            "replaceable" => KindClass::Replaceable,
            "ephemeral" => KindClass::Ephemeral,
            "addressable" => KindClass::Addressable,
            other => panic!("kind {}: unknown class {other}", case.kind),
        };
        assert_eq!(kind.class(), class, "kind {} class", case.kind);
    }
}
