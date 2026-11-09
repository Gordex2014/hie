# hie_runtime

What the engine runs a transform with: `worker.py` (the worker protocol), `hie.py` (the helpers a transform imports) and `limits.py`, the reference launcher of the `limited-process@1` contract.

`limits.py` is vendored unchanged and pinned by contract id and SHA-256 in `limits.pin`; the engine refuses to start a worker when the file on disk does not match the pin. Because it is unchanged, its docstring's `docs/contract.md` means [`docs/limited-process.md`](../docs/limited-process.md) here.

To re-vendor a new revision of the launcher:

1. Replace `limits.py` with the new file, byte for byte (`.gitattributes` keeps its line endings as they are).
2. Update both lines of `limits.pin`: the contract id the launcher follows (its `CONTRACT`) and its SHA-256 (`sha256sum limits.py`). A new contract id also means reviewing the engine's host side, `src/worker.rs`, against the new contract, and `docs/limited-process.md` with it.
3. Run the tests: `cargo test` (the `the_pin_matches_the_vendored_launcher` unit test and `tests/worker.rs`) and `python3 -m unittest tests/test_worker.py`.
