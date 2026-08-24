//! Generated wire types for `proto/headgate.proto` — the contract that makes two
//! languages and three stores one system (wire schema). Field numbers are permanent; removal
//! means `reserved`, never reuse.
//!
//! DO NOT EDIT `headgate.v1.rs` by hand: run `scripts/gen-proto.sh` after changing the
//! .proto and commit the output. Downstream builds never need protoc.

pub mod v1 {
    include!("headgate.v1.rs");
}
