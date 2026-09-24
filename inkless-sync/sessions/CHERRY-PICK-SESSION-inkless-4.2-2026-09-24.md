# Cherry-pick Session: inkless-4.2

## Session Info
- **Date**: 2026-09-24
- **Source**: origin/main (d16ea421f5, #804)
- **Target**: inkless-4.2
- **Base**: 308d91a9f9 (#802)
- **Status**: Complete

---

## Commits to Cherry-pick

| # | Hash | PR | Subject | Status |
|---|------|-----|---------|--------|
| 1 | e9de1e2e9b | #805 | fix(inkless:controller): count diskless topics with remote storage effectively off [KC-540] | Applied with 4.2 adaptation |
| 2 | b15ea95d2d | #808 | fix(inkless:retention): skip consolidating topics in WAL retention [KC-552] | Applied |
| 3 | c9b88d7ad9 | #809 | fix(inkless:retention): reject new copy-disabled consolidating topics [KC-552] | Applied |
| 4 | 2e3f24e376 | #806 | fix(inkless:consolidation): freeze cross-tier start during prune | Applied |
| 5 | d16ea421f5 | #804 | fix(inkless:config): allow routine alters on diskless topics with remote.storage.enable=false | Applied with conflict |

---

## Conflict Resolution Log

### #804 - fix(inkless:config): allow routine alters on diskless topics with remote.storage.enable=false

**File**: `storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java`
- **Conflict**: The private validation method used `requestedConfigs` and `Map<?, ?>` on 4.2, while the cherry-pick changed the merged state parameter to `resultingConfigs` and narrowed the parsed config type to `Map<String, ?>`.
- **Resolution**: Kept the cherry-pick's `resultingConfigs` semantics and retained 4.2's `Map<?, ?>` parsed config type.
- **Reason**: #804 must validate the controller's merged topic state, while 4.2's `ConfigDef.parse` call and public validation API still produce wildcard maps.

---

## Compilation Errors

| # | File:Line | Error | Cherry-pick | Fix | Status |
|---|-----------|-------|-------------|-----|--------|
| 1 | `ControllerMetadataMetricsPublisherTest.java:255` | `MetadataDelta` requires `MetadataImage` and `SupportedConfigChecker` on 4.2 | #805 | Constructed the delta with `MetadataDelta.Builder().setImage(...).build()` | Fixed |

---

## Verification

### Build
```bash
make build
```
- [x] Build passes

### Tests
```bash
make test
```
- [x] Tests pass

### Checklist
- [x] All cherry-picks applied
- [x] All conflicts documented
- [x] Compilation verified after each cherry-pick
- [x] Full build passes
- [x] Full tests pass

---

## Summary

### Results
| Result | Count |
|--------|-------|
| Successfully cherry-picked | 4 |
| Cherry-picked with conflicts | 1 |
| Skipped (not applicable) | 0 |
| Failed | 0 |

### Key Conflict Patterns

| Pattern | Files | Resolution |
|---------|-------|------------|
| `MetadataDelta` constructor requires a config checker | `ControllerMetadataMetricsPublisherTest.java` | Use `MetadataDelta.Builder().setImage(...).build()` |
| Parsed config generic type differs | `LogConfig.java` | Keep 4.2's `Map<?, ?>` type while carrying the merged-state behavior |

### Deferred Commits

| PR | Reason | Blocked By |
|----|--------|------------|

---

## Notes

- The consistency check reports #623 as an old missing commit. It remains intentionally skipped.
- Applied commits on 4.2: `6ab1218297`, `8f235ae8e8`, `a6cf6ddc04`, `f5ceb36876`, and `85d8618a5a`.
- `make build` and `make test` pass with Java 17.
- Local jOOQ regeneration rewrites `@SuppressWarnings` on generated sources. Those generated rewrites were discarded so the cherry-picked sources stay intact.
