# Release Sync Session: inkless-4.2 to 4.2.2

## Session Info
- **Date**: 2026-10-07
- **Release Branch**: inkless-4.2
- **Working Branch**: inkless-4.2
- **Target Tag**: 4.2.2
- **Commits to merge**: 69
- **Status**: Complete

---

## Phase 1: Discovery

### Current State
- Current inkless version: `4.2.1-inkless`
- Current base version: `4.2.1`
- Target version: `4.2.2`

---

## Phase 2: Merge

### Merge Command
```bash
git merge 4.2.2
```

### Conflict Summary
| Category | Count | Files |
|----------|-------|-------|
| Version files | 4 | `gradle.properties`, `tests/kafkatest/__init__.py`, `tests/kafkatest/version.py`, `committer-tools/kafka-merge-pr.py` |
| Dependencies | 0 | — |
| Test files | 0 | — |
| Documentation | 0 | — |
| Other | 1 | `core/src/main/scala/kafka/server/KafkaApis.scala` |

---

## Phase 3: Conflict Resolution

### Version Files
| # | File | Resolution | Status |
|---|------|------------|--------|
| 1 | gradle.properties | `version=4.2.2-inkless` | Done |
| 2 | tests/kafkatest/__init__.py | `__version__ = '4.2.2.inkless'` | Done |
| 3 | tests/kafkatest/version.py | `DEV_VERSION = KafkaVersion("4.2.2-inkless")` | Done |
| 4 | docs/js/templateData.js | Not touched this sync (no conflict) | N/A |
| 5 | committer-tools/kafka-merge-pr.py | `DEFAULT_FIX_VERSION = "4.2.2-inkless"` | Done |

### Dependency Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| 1 | gradle/dependencies.gradle | No conflict; merged cleanly. Upstream bumped `jackson` (2.21.2→2.21.6), `jetty` (12.0.34→12.0.37), `jline` (3.30.4→3.30.15), `log4j2` (2.25.4→2.25.5). All Inkless-specific deps (`awsSdk`, `azureSdk`, `gcsSdk`, `jooq`, `flyway`, `fakeGcs`, `bucket4j`, `assertj`, `awaitility`) preserved. | Verified |

### POM Files (Keep Upstream Version)
| # | File | Notes | Status |
|---|------|-------|--------|
| 1 | streams/quickstart/pom.xml | Unaffected by this sync; still `4.2.2` style (no `-inkless` suffix) | Verified |
| 2 | streams/quickstart/java/pom.xml | Unaffected by this sync | Verified |

### Test Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| — | — | No test file conflicts in this sync | N/A |

### Other Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| 1 | core/src/main/scala/kafka/server/KafkaApis.scala | Share Fetch `recordBytesOutMetric`: merged upstream's safer null-check (handles deleted topic/invalid topic ID) with Inkless's `isDiskless` bytes-out metric, matching the existing pattern already used in the regular Fetch path (`recordBytesOutMetric` above it). | Done |
| 2 | .gitignore | No conflict; merged cleanly. Inkless entries (`_data/`, `.inkless-sync/`, `core/data/`) preserved alongside upstream removals. | Verified |

---

## Phase 4: Verification

### Build
```bash
./gradlew :core:build :storage:inkless:build :metadata:build -x test -x generateJooqClasses
```
- [x] Build passes

### Tests
```bash
./gradlew :storage:inkless:test :storage:inkless:integrationTest -x generateJooqClasses
./gradlew :metadata:test --tests "org.apache.kafka.controller.*"
./gradlew :core:test --tests "*Inkless*" --tests "*Diskless*" --tests "io.aiven.inkless.*"
```
- [x] Tests pass

Notes on test runs:
- `:storage:inkless:test`/`:storage:inkless:integrationTest`: passed.
- `:metadata:test --tests "org.apache.kafka.controller.*"`: initial run under the session's default `fi_FI.UTF-8` locale failed 5 `EventPerformanceMonitorTest` decimal-formatting tests (comma vs. period decimal separator). This is a pre-existing environment issue unrelated to the merge; rerunning with `LC_ALL=C LANG=C` passed all 669 tests cleanly.
- `:core:test --tests "*Inkless*" --tests "*Diskless*" --tests "io.aiven.inkless.*"`: a first run (12 parallel forks) reported 7 failures, all `software.amazon.awssdk...SdkClientException: Connection refused` against the MinIO Testcontainer, caused by resource contention after an earlier out-of-memory event in this session. Rerunning the same failing classes with `--max-workers=2` passed with no failures. Not a merge regression.

### Checklist
- [x] Version updated to `4.2.2-inkless` in gradle.properties
- [x] Inkless module builds: `./gradlew :storage:inkless:build`
- [x] Key inkless files unchanged:
  - [x] `storage/inkless/src/main/java/io/aiven/inkless/produce/Writer.java`
  - [x] `docs/inkless/README.md`

---

## Summary

### Merge Commit
```
ecdbaaa756 Merge upstream 4.2.2 into inkless-4.2
```

### Files Modified
| Type | Count |
|------|-------|
| Version files | 4 |
| Dependencies | 0 (clean merge) |
| Test files | 0 |
| Other | 1 (`KafkaApis.scala`) |

### Blockers (if any)
| Issue | Description | Action Needed |
|-------|-------------|---------------|
| None | — | — |

---

## Notes
- All five conflicts were straightforward: four were the standard version-string bumps, and the fifth (`KafkaApis.scala`) was a semantic merge combining an upstream robustness fix (null topic-name guard for Share Fetch bytes-out metrics) with the Inkless-specific `isDiskless` metric flag, following the pattern already established in the classic Fetch path's `recordBytesOutMetric`.
- Two test suites showed failures on first run that turned out to be environmental, not caused by the sync: a non-`C` locale breaking decimal formatting assertions, and Testcontainers/MinIO connection refusals from running too many parallel JVM forks after a session out-of-memory event. Both suites passed cleanly once rerun with `LC_ALL=C` and reduced worker parallelism, respectively. Future syncs on this host should run tests with `LC_ALL=C LANG=C` and conservative `--max-workers` to avoid false failures.
