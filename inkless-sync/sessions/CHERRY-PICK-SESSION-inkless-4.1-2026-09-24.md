# Cherry-pick Session: inkless-4.1

## Session Info
- **Date**: 2026-09-24
- **Source**: origin/main (d16ea421f5, #804)
- **Target**: inkless-4.1
- **Base**: a09a645876 (#802)
- **Status**: Complete

---

## Commits to Cherry-pick

| # | Hash | PR | Subject | Status |
|---|------|-----|---------|--------|
| 1 | e9de1e2e9b | #805 | fix(inkless:controller): count diskless topics with remote storage effectively off [KC-540] | Applied |
| 2 | b15ea95d2d | #808 | fix(inkless:retention): skip consolidating topics in WAL retention [KC-552] | Applied |
| 3 | c9b88d7ad9 | #809 | fix(inkless:retention): reject new copy-disabled consolidating topics [KC-552] | Applied |
| 4 | 2e3f24e376 | #806 | fix(inkless:consolidation): freeze cross-tier start during prune | Applied with 4.1 adaptation |
| 5 | d16ea421f5 | #804 | fix(inkless:config): allow routine alters on diskless topics with remote.storage.enable=false | Applied with conflict |

---

## Conflict Resolution Log

### #804 - fix(inkless:config): allow routine alters on diskless topics with remote.storage.enable=false

**File**: `storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java`
- **Conflict**: The private validation method used `requestedConfigs` and `Map<?, ?>` on 4.1, while the cherry-pick changed the merged state parameter to `resultingConfigs` and narrowed the parsed config type to `Map<String, ?>`.
- **Resolution**: Kept the cherry-pick's `resultingConfigs` semantics and retained 4.1's `Map<?, ?>` parsed config type.
- **Reason**: #804 must validate the controller's merged topic state, while 4.1's older `ConfigDef.parse` call and public validation API still produce wildcard maps.

---

## Compilation Errors

| # | File:Line | Error | Cherry-pick | Fix | Status |
|---|-----------|-------|-------------|-----|--------|
| 1 | Build script | Java 25 produced `Unsupported class file major version 69` | #805 | Ran all verification with Java 17, which 4.1 supports | Fixed |
| 2 | `RemoteLogManager.java:1353` | `RetriableRemoteStorageException` does not exist on Kafka 4.1 | #806 | Used 4.1's `RemoteStorageException` and adapted the assertion; the task scheduler retries both exceptions | Fixed |
| 3 | `LogConfig.java:948` | 4.1's `Map<?, ?>` could not be passed to the main-branch `Map<String, ?>` signature | #804 | Retained `Map<?, ?>` for the parsed config parameter | Fixed |

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
| Newer upstream exception absent on 4.1 | `RemoteLogManager.java`, `RemoteLogManagerTest.java` | Use `RemoteStorageException`; 4.1 reschedules the task after that exception |
| Parsed config generic type differs | `LogConfig.java` | Keep 4.1's `Map<?, ?>` type while carrying the merged-state behavior |

### Deferred Commits

| PR | Reason | Blocked By |
|----|--------|------------|

---

## Notes

- The consistency check also reports #548 and #502 as old missing commits. They remain intentionally skipped.
- Applied commits on 4.1: `28adbc4f82`, `0589163c45`, `716fd236f4`, `4143d241b5`, and `3bee2d20df`.
- `make build` and `make test` pass with Java 17.
