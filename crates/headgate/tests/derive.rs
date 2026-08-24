//! #[derive(Task)] — kind/version/aliases plus the default JSON codec, end to end.

use headgate::Task;

#[derive(Task, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
#[task(kind = "notify:welcome", version = 2, aliases("email:welcome"))]
struct WelcomeNotification {
    to: String,
    locale: String,
}

#[derive(Task, serde::Serialize, serde::Deserialize)]
#[task(kind = "minimal")]
struct Minimal;

#[test]
fn derive_generates_identity_and_json_codec() {
    assert_eq!(WelcomeNotification::TYPE, "notify:welcome");
    assert_eq!(WelcomeNotification::VERSION, 2);
    // typed dispatch the rename path: enqueue uses TYPE, dispatch also answers the old kind.
    assert_eq!(WelcomeNotification::ALIASES, &["email:welcome"]);

    let t = WelcomeNotification {
        to: "a@b.c".into(),
        locale: "en".into(),
    };
    let bytes = t.encode().unwrap();
    assert_eq!(WelcomeNotification::decode(&bytes).unwrap(), t);

    // payload versioning the default upcast: current version decodes, unknown goes to undecodable.
    assert!(WelcomeNotification::upcast(2, &bytes).is_ok());
    assert!(matches!(
        WelcomeNotification::upcast(1, &bytes),
        Err(headgate::CodecError::UnknownVersion(1))
    ));

    assert_eq!(Minimal::VERSION, 1);
    assert!(Minimal::ALIASES.is_empty());
}

/// typed dispatch the kind-format rule is checked at REGISTRATION, for TYPE and for every alias —
/// an alias is a dispatch key jobs get enqueued under during a rename, so exempting it
/// would let the rename introduce exactly the kind a fresh registration is refused.
#[test]
fn registration_enforces_the_kind_format_rule() {
    use headgate::{JobCtx, Registry};

    struct Good;
    impl headgate::Task for Good {
        const TYPE: &'static str = "w"; // length ONE — legal here, illegal in River
        fn encode(&self) -> Result<Vec<u8>, headgate::CodecError> {
            Ok(vec![])
        }
        fn decode(_: &[u8]) -> Result<Self, headgate::CodecError> {
            Ok(Good)
        }
    }
    struct BadType;
    impl headgate::Task for BadType {
        const TYPE: &'static str = "bad kind";
        fn encode(&self) -> Result<Vec<u8>, headgate::CodecError> {
            Ok(vec![])
        }
        fn decode(_: &[u8]) -> Result<Self, headgate::CodecError> {
            Ok(BadType)
        }
    }
    struct BadAlias;
    impl headgate::Task for BadAlias {
        const TYPE: &'static str = "fine:kind";
        const ALIASES: &'static [&'static str] = &["old kind"];
        fn encode(&self) -> Result<Vec<u8>, headgate::CodecError> {
            Ok(vec![])
        }
        fn decode(_: &[u8]) -> Result<Self, headgate::CodecError> {
            Ok(BadAlias)
        }
    }

    let mut r = Registry::new();
    assert!(
        r.register::<Good, _, _>(|_: JobCtx, _: Good| async { Ok(()) })
            .is_ok()
    );
    let e = r
        .register::<BadType, _, _>(|_: JobCtx, _: BadType| async { Ok(()) })
        .unwrap_err();
    assert!(e.starts_with("invalid kind `bad kind`:"), "got {e}");
    let e = r
        .register::<BadAlias, _, _>(|_: JobCtx, _: BadAlias| async { Ok(()) })
        .unwrap_err();
    assert!(e.starts_with("invalid kind `old kind`:"), "got {e}");
    // and the rejected registration left nothing half-inserted. Round 32h: `!any()` is
    // also true of a `kinds()` that yields NOTHING, so the accepted registration is
    // asserted first — otherwise this passes against a registry that lost every kind.
    assert!(
        r.kinds().any(|k| k == "w"),
        "the ACCEPTED registration must be present"
    );
    assert!(!r.kinds().any(|k| k == "fine:kind"));
}
