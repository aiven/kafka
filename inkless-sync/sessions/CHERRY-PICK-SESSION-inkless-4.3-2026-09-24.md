# Cherry-pick Session: inkless-4.3

## Session Info
- **Date**: 2026-09-24
- **Source**: origin/main (d16ea421f5, #804)
- **Target**: inkless-4.3
- **Base**: bf8c302cb6 (#789)
- **Status**: Complete

---

## Commits to Cherry-pick

| # | Hash | PR | Subject | Status |
|---|------|-----|---------|--------|
| 1 | 1c42660361 | #797 | docs(inkless:release): changelog and cherrypick session for 0.48 | Applied |
| 2 | 150842be9b | #796 | ci(inkless:release): move the release workflows to Node 24 action majors | Applied |
| 3 | 6f3f2361f0 | #800 | docs(inkless): drop cleanup.policy=delete from the unsupported list | Applied |
| 4 | fe9deee877 | #801 | docs(inkless): document PostgreSQL planning pressure | Applied |
| 5 | a7c46e2c4e | #803 | docs(inkless): describe TS unification benefits | Applied |
| 6 | 9b4fd9243c | #802 | fix(inkless:consolidation): start consolidation when remote storage is enabled later [KC-540] | Applied with 4.3 adaptation |
| 7 | e9de1e2e9b | #805 | fix(inkless:controller): count diskless topics with remote storage effectively off [KC-540] | Applied with 4.3 adaptation |
| 8 | b15ea95d2d | #808 | fix(inkless:retention): skip consolidating topics in WAL retention [KC-552] | Applied |
| 9 | c9b88d7ad9 | #809 | fix(inkless:retention): reject new copy-disabled consolidating topics [KC-552] | Applied with 4.3 adaptation |
| 10 | 2e3f24e376 | #806 | fix(inkless:consolidation): freeze cross-tier start during prune | Applied |
| 11 | d16ea421f5 | #804 | fix(inkless:config): allow routine alters on diskless topics with remote.storage.enable=false | Applied with 4.3 adaptation |

---

## Conflict Resolution Log

No merge conflicts. `#802` and `#809` auto-merged `QuorumController`, `ReplicationControlManager`, and `ReplicationControlManagerInklessTest`. `#806` auto-merged `BrokerServer.scala`.

---

## Compilation Errors

| # | File:Line | Error | Cherry-pick | Fix | Status |
|---|-----------|-------|-------------|-----|--------|
| 1 | `ReplicationControlManagerInklessTest.java:3261` | `incrementalAlterConfigs` still takes `forwarded` on 4.3 | #802 | Passed `false` for `forwarded`, matching the existing call in the same test | Fixed |
| 2 | `ControllerMetadataMetricsPublisherTest.java:255` | `MetadataDelta` requires `MetadataImage` and `SupportedConfigChecker` on 4.3 | #805 | Constructed the delta with `MetadataDelta.Builder().setImage(...).build()` | Fixed |
| 3 | `ReplicationControlManagerInklessTest.java:2897` | `createTopics` still takes `forwarded` on 4.3 | #809 | Passed `false` for `forwarded`, matching the existing calls in the same test | Fixed |
| 4 | `DisklessRemoteStorageDeleteConfigTest.scala:61` | `incrementalAlterConfigs` and `legacyAlterConfigs` still take `forwarded` on 4.3 | #804 | Passed `false` for `forwarded` on both calls | Fixed |

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
| Successfully cherry-picked | 7 |
| Cherry-picked with conflicts | 0 |
| Cherry-picked with 4.3 API adaptations | 4 |
| Skipped (not applicable) | 0 |
| Failed | 0 |

### Key Conflict Patterns

| Pattern | Files | Resolution |
|---------|-------|------------|
| `incrementalAlterConfigs`, `legacyAlterConfigs`, and `createTopics` still take `forwarded` | `ReplicationControlManagerInklessTest.java`, `DisklessRemoteStorageDeleteConfigTest.scala` | Pass `false` for `forwarded` |
| `MetadataDelta` constructor requires a config checker | `ControllerMetadataMetricsPublisherTest.java` | Use `MetadataDelta.Builder().setImage(...).build()` |

### Deferred Commits

| PR | Reason | Blocked By |
|----|--------|------------|

---

## Notes

- The consistency check reported 11 actionable commits and no old missing commits.
- Applied commits on 4.3: `03880b143f`, `e9e79fd05e`, `2029bd48f4`, `73af09ce17`, `eece6c0a84`, `21dfc930d3`, `0ad59887ee`, `439178dd4b`, `ef20ceb4dd`, `7933c09944`, and `ce65db5706`.
- `#804` applied without a `LogConfig` conflict. 4.3 already parses topic configs as `Map<String, ?>`.
- `make build` and `make test` pass with Java 17. The first core run reported startup timeouts in `InklessConfigsTest.regexExcludedTopicsAreExcludedFromForcePolicy` and `InklessManagedReplicasClusterTest.createDisklessTopicWithManagedReplicas`. Both pass when rerun alone.
- Local jOOQ regeneration drops `"this-escape"` from `@SuppressWarnings` on generated sources. Those generated rewrites were discarded so the cherry-picked sources stay intact.
