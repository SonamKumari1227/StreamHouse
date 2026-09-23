# tests/unit/

Pure functions only. No containers, no network, no Spark session beyond a local one.

- DataFrame assertions use `chispa`, never manual `collect()` comparison.
- Every transform function in `transform/` should be callable without a running stack. If one is not,
  that is a design problem in the transform, not a testing problem here.
- Target: 80% coverage on `transform/`, enforced in CI.

**Status:** Phase 1 onward, as transform code appears.
