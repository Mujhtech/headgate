# Comment style

Comments in headgate document contracts, invariants, and non-obvious failure modes. They
are part of the release surface: a reader should understand them without knowing the
project's review history or having another document open beside the code.

## Keep a comment when it explains

- why an apparently simpler implementation is incorrect;
- an atomicity, fencing, clock, privacy, or bounded-work requirement;
- a public API contract that its type signature cannot express;
- backend-specific behavior that must remain equivalent across implementations; or
- the reason a test fixture or assertion has an unusual shape.

Delete comments that merely restate the next line, label an obvious block, narrate a
past review round, or preserve a temporary implementation diary.

## Write self-contained explanations

Prefer the reason itself over a section reference:

```text
// Recheck the state after locking because a concurrent claimant may have committed
// between candidate selection and row locking.
```

Avoid comments such as `Round 32 fixed this` or `See §5.1`. A link to architecture or
conformance material is useful only after the local comment explains the constraint.
Stable named anchors and file paths are preferable to section numbers.

Generated files are never edited by hand. Improve comments in their schema or generator
input, then regenerate the outputs. Admission SQL and Lua comments are intentionally
detailed because they protect concurrency behavior that looks redundant in isolation.

