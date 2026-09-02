# Release Sync Session: inkless-4.3 to 4.3.1

## Session Info
- **Date**: 2026-09-02
- **Release Branch**: inkless-4.3
- **Working Branch**: inkless-4.3-sync-4.3.1
- **Target Tag**: 4.3.1
- **Commits to merge**: 140
- **Status**: Complete

---

## Phase 1: Discovery

### Current State
```bash
./inkless-sync/release-sync.sh inkless-4.3 --list-tags
```

- Current inkless version: 4.3.0-inkless-SNAPSHOT
- Current base version: 4.3.0
- Target version: 4.3.1

---

## Phase 2: Merge

### Merge Command
```bash
./inkless-sync/release-sync.sh inkless-4.3 --to-tag 4.3.1 --branch inkless-4.3-sync-4.3.1 --yes
```

### Conflict Summary
| Category | Count | Files |
|----------|-------|-------|
| Version files | 5 | gradle.properties, tests/kafkatest/__init__.py, tests/kafkatest/version.py, committer-tools/kafka-merge-pr.py, .github/workflows/ci.yml |
| Dependencies | 0 | (gradle/dependencies.gradle auto-merged) |
| Test files | 0 | (none beyond version file above) |
| Documentation | 0 | |
| Other (interleaved controller logic) | 2 | metadata/.../ConfigurationControlManager.java, metadata/.../QuorumController.java |
| POM files | 3 | streams/quickstart/{pom.xml,java/pom.xml,java/.../archetype-resources/pom.xml} |

---

## Phase 3: Conflict Resolution

### Version Files
| # | File | Resolution | Status |
|---|------|------------|--------|
| 1 | gradle.properties | `version=4.3.1-inkless` (kept upstream-update NOTE comment) | Done |
| 2 | tests/kafkatest/__init__.py | `__version__ = '4.3.1+inkless'` (initially set to `'4.3.1.inkless'`, which failed CI: `packaging.version.InvalidVersion`; fixed to use the PEP 440 local version label `+inkless`) | Done |
| 3 | tests/kafkatest/version.py | `DEV_VERSION = KafkaVersion("4.3.1-inkless-SNAPSHOT")` | Done |
| 4 | docs/js/templateData.js | No conflict; not touched by this sync | N/A |
| 5 | committer-tools/kafka-merge-pr.py | `DEFAULT_FIX_VERSION = "4.3.1-inkless"` | Done |
| 6 | .github/workflows/ci.yml | Kept `main`/`inkless-*` triggers, replaced stale `4.0` with `4.3` (matches branch) | Done |

### Dependency Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| - | gradle/dependencies.gradle | Auto-merged cleanly, no manual changes needed | N/A |

### POM Files (Keep Upstream Version)
| # | File | Notes | Status |
|---|------|-------|--------|
| 1 | streams/quickstart/pom.xml | `<version>4.3.1</version>`, no -inkless suffix | Done |
| 2 | streams/quickstart/java/pom.xml | `<version>4.3.1</version>` | Done |
| 3 | streams/quickstart/java/src/main/resources/archetype-resources/pom.xml | `<kafka.version>4.3.1</kafka.version>` | Done |

### Interleaved controller logic (Inkless + upstream feature merge)
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| 1 | metadata/.../ConfigurationControlManager.java | Upstream added a `forwarded` boolean param (cordoned.log.dirs forwarding validation: `isCordonedLogDirsInvalid`, renamed `isCordonedLogDirsDisallowed`→`isCordonedLogDirsDisabled`) threaded through `incrementalAlterConfigs(s)`/`legacyAlterConfigs(s)`/`*Resource` and `validateAlterConfig`. Inkless already threads a `Function<ConfigResource, ApiError> postConfigValidation` through the same call chain for classic-to-diskless switch validation. Merged both params through every overload so callers can supply `forwarded` and/or `postConfigValidation` independently. Kept all diskless-specific guards (`isDisallowedDisklessEnableOnExistingTopic`, diskless.enable delete guards) alongside the new cordoned-log-dirs checks. | Done |
| 2 | metadata/.../QuorumController.java | Updated the two call sites (`incrementalAlterConfigs`, `legacyAlterConfigs`) to pass both the existing `forwarded` request flag and the inkless `postConfigValidation` lambda to the newly-merged 4-arg `ConfigurationControlManager` overloads, using the inkless `effectiveChanges`/`effectiveConfigs` (switch-normalized) maps as before. | Done |

### Other Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| - | .gitignore | Auto-merged cleanly; both inkless and upstream entries present | N/A |

---

## Phase 4: Verification

### Build
```bash
make build
```
- [x] Build passes

### Tests
```bash
make test
```
- [x] Tests pass (locale-neutral run; 5 unrelated locale-dependent test failures under fi_FI.UTF-8 are pre-existing and unrelated to this sync)

### Checklist
- [x] Version updated to `4.3.1-inkless` in gradle.properties
- [ ] Inkless module builds: `./gradlew :storage:inkless:build`
- [ ] Key inkless files unchanged:
  - [ ] `storage/inkless/src/main/java/io/aiven/inkless/InklessWriter.java`
  - [ ] `docs/inkless/README.md`

---

## Summary

### Merge Commit
```
5991a96c88 Merge upstream 4.3.1 into inkless-4.3-sync-4.3.1
```

### Files Modified
| Type | Count |
|------|-------|
| Version files | 6 |
| Dependencies | 0 |
| Test files | 0 |
| Interleaved controller logic | 2 |
| POM files | 3 |

### Blockers (if any)
| Issue | Description | Action Needed |
|-------|-------------|---------------|
| | | |

---

## Notes
The two non-standard conflicts (`ConfigurationControlManager.java`, `QuorumController.java`)
were not covered by the standard resolution patterns documented in RELEASE-SYNC-GUIDE.md.
Upstream 4.3.1 added a new `cordoned.log.dirs` forwarding-validation feature (`forwarded`
boolean threaded through the incremental/legacy alter-configs call chain), which collided
with the Inkless classic-to-diskless switch validation (`postConfigValidation` Function
threaded through the same call chain). Resolved by adding both parameters to every
affected method overload rather than picking one side, since both features are needed
concurrently. Recommend flagging these two files for extra scrutiny in future release
syncs, since they carry heavy interleaved Inkless logic and are prone to structural
conflicts (not just literal value conflicts) whenever upstream touches config validation.
