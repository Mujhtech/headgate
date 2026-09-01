use criterion::{Criterion, black_box, criterion_group, criterion_main};
use headgate_core::{Envelope, Outcome, State, TransitionCtx, transition, validate_enqueue};

fn runtime_hot_paths(c: &mut Criterion) {
    let envelope = Envelope {
        id: "bench-job".into(),
        kind: "bench:job".into(),
        queue: "default".into(),
        payload: vec![42; 1024],
        ..Default::default()
    };
    c.bench_function("validate_enqueue_1k_payload", |b| {
        b.iter(|| validate_enqueue(black_box(std::slice::from_ref(&envelope))))
    });

    let context = TransitionCtx {
        attempt: 1,
        max_attempts: 25,
        crash_attempt: 0,
        crash_limit: 3,
        retention_ms: 86_400_000,
    };
    c.bench_function("state_transition_retry", |b| {
        b.iter(|| {
            transition(
                black_box(State::Running),
                black_box(Outcome::Retry),
                &context,
            )
        })
    });
}

criterion_group!(benches, runtime_hot_paths);
criterion_main!(benches);
