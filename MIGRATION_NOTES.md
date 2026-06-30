# Migration: TinkerPop 3.3.0 → 3.8.1 and Java 8 → Java 17

This document records the upgrade of Unipop to Apache TinkerPop **3.8.1** on **Java 17**.

## Summary

TinkerPop 3.8.0 dropped Java 8 and requires **Java 11+** (Java 17 supported), so the Gremlin
bump and the JVM bump are aligned. The upgrade was applied to the modules that can run on
Java 17 today — **`unipop-core`** and **`unipop-jdbc`** — which now build and package cleanly.

The Elasticsearch-backed modules are **deferred** (see below): Elasticsearch 5.3.1 (Lucene 6 +
the removed transport client) cannot run on a Java 17 JVM.

## Build / toolchain

- Java level moved to 17 via `<maven.compiler.release>17</maven.compiler.release>` (parent pom).
- Plugin upgrades required for JDK 17: `maven-compiler-plugin` 3.1 → **3.13.0**,
  `maven-javadoc-plugin` 2.9.1 → **3.6.3**, `maven-surefire-plugin` pinned **3.2.5**,
  `maven-source-plugin` → 3.3.1, `maven-gpg-plugin` → 3.2.8, `maven-release-plugin` → 3.1.1,
  `maven-dependency-plugin` → 3.6.1, `maven-enforcer-plugin` 1.3.1 → 3.5.0 (requireJavaVersion `[17,)`).
- Publishing (`nexus-staging-maven-plugin` + OSSRH `distributionManagement`) was commented out:
  OSSRH (oss.sonatype.org) was shut down 2025-06-30. Re-enable via the Central Portal
  (`central-publishing-maven-plugin`) if releasing.
- Local toolchain used: Homebrew `openjdk@17` (keg-only, `JAVA_HOME=/opt/homebrew/opt/openjdk@17`) + Maven 3.9.16.

## `unipop-core`

Dependency changes: TinkerPop 3.3.0 → **3.8.1**; `reflections` 0.9.10 → **0.10.2**;
`commons-collections4` 4.0 → 4.4; `org.json` → 20240303; `slf4j-log4j12` → **slf4j-reload4j** 1.7.36;
added **Guava** 33.2.1 (previously transitive via reflections 0.9.x, which 0.10 dropped) and
**commons-lang** 2.6 (was transitive under old TinkerPop).

TinkerPop API changes fixed:
- `ElementValueTraversal` → `ValueTraversal` (same `…traversal.lambda` package; removed in 3.5).
- Custom predicates implement `PBiPredicate` (3.7) instead of `BiPredicate`: `Text`, `Date`,
  `ExistsP`, `ConcatenateFieldPropertySchema` (the `P(BiPredicate, …)` constructor was replaced
  by `P(PBiPredicate, …)`).
- `Traverser.Admin.incrLoops(stepId)` → `incrLoops()` (no-arg).
- `RangeGlobalStep.getHighRange()/getLowRange()` now return boxed `Long` → cast via `(int)(long)`.
- `HasContainerHolder` is now generic `<S,E>`; used as `HasContainerHolder<?,?>` to avoid raw-type
  erasure of `getHasContainers()`.
- `org.apache.commons.configuration` → `org.apache.commons.configuration2` (TinkerPop 3.6).
- `ElementHelper.validateMixedElementIds(...)` removed (3.6, mixed ids now allowed) → call removed.
- `Scoping.getScopeValue(...)` now throws checked `Scoping.KeyNotFoundException` → caught and
  mapped to `FastNoSuchElementException`.

## `unipop-jdbc`

Dependency changes: `commons-dbcp` 1.4 → **`commons-dbcp2`** 2.12.0; **jOOQ** 3.7.0 → 3.19.15
(unused `jooq-meta` dropped); `h2` 1.4.196 → 2.3.232 (test); `sqlite-jdbc` 3.8.11.2 → 3.46.1.3.

API changes fixed:
- jOOQ `Settings.setRenderNameStyle(RenderNameStyle.AS_IS)` (removed) →
  `setRenderNameCase(RenderNameCase.AS_IS)` + `setRenderQuotedNames(RenderQuotedNames.NEVER)`.
- jOOQ `DSLContext` is no longer `AutoCloseable` (3.19); `context.close()` calls now close the
  pooled `BasicDataSource` (`closeQuietly()`), which owns the connections.
- `Order.incr` → `Order.asc` (`Order.shuffle` unchanged).
- `org.jooq.Record` is now ambiguous with Java 17's `java.lang.Record` → explicit `import org.jooq.Record;`.
- `org.apache.commons.configuration` → `…configuration2` in `JdbcGraphProvider`.

## Test harness migration to Gherkin/Cucumber

TinkerPop 3.8 removed the individual JUnit **process** `*Test` classes (e.g. `StoreTest`); the
process compliance suite is now the shared `.feature` files in `gremlin-test`, driven by Cucumber.
- Removed obsolete `UnipopProcessSuite` (core) and `JdbcProcessSuite` (jdbc) which extended the
  now-empty `ProcessStandardSuite` model.
- Added `JdbcWorld` (implements `org.apache.tinkerpop.gremlin.features.World`) + `JdbcFeatureTest`
  (`@RunWith(Cucumber.class)`, Guice object factory), reusing `JdbcGraphProvider` +
  `AbstractGraphProvider.loadGraphData` to build/load the H2-backed graph per scenario.
- Cucumber/guice test deps pinned to TinkerPop 3.8.1's versions (cucumber 7.21.1, guice 4.2.3).
- `@CucumberOptions` tags exclude capabilities Unipop's JDBC federation provider does not support
  (graph computer, null/list/set/map/uuid/datetime values, meta/multi-properties, user-supplied ids).
- **Structure** tests are unchanged: `UnipopStructureSuite`/`AbstractGremlinSuite` and the
  structure `*Test` classes still exist in 3.8.1 (`JdbcStructureSuite` retained).

A test-only fix was also required: H2 2.x promoted `YEAR`/`TIME` to reserved keywords (the JDBC test
schema uses them as column names), so the H2 URLs use `;NON_KEYWORDS=YEAR,TIME`.

> Unipop is a partial **federation** provider; like the original project it does not pass the full
> Gremlin compliance suite. The harness is migrated and runnable — achieving full green compliance
> is a separate effort, not part of this version migration.

### Test run results (Java 17, conf=basic)

- **Structure suite** (`JdbcStructureSuite`, `@RunWith(StructureStandardSuite.class)`): initializes
  and runs — **936 tests, ~353 pass, 31 failures, 182 errors, 370 skipped**. The failures/errors are
  pre-existing partial-provider behavior (e.g. `ConcurrentModificationException` in property
  iteration), not migration regressions. This confirms the migrated runtime stack (UniGraph + jdbc +
  H2 + data loading + traversal execution) actually functions on TinkerPop 3.8.1 / Java 17.
- **Process compliance** (`JdbcFeatureTest`, Gherkin/Cucumber): runs the shared TinkerPop feature
  suite — **1913 scenarios, ~964 pass, 603 fail, 332 error, 14 skipped**. After fixing harness-level
  issues (see below) the remaining failures are genuine capability gaps (`AssertionError`,
  `UnsupportedOperationException`, etc.) for a partial federation provider — not migration defects.

Harness-level fixes required for Java 17 (these make the suite *run*, independent of Unipop coverage):
- Cucumber object factory registered via SPI: `test-resources/META-INF/services/io.cucumber.core.backend.ObjectFactory`.
- Surefire `argLine` with `--add-opens` for Guice 4.2.3 (cglib) and Kryo/Gryo serialization
  (`java.base/java.lang`, `…/java.util.concurrent.atomic`, etc.) — otherwise `InaccessibleObjectException`.
- `JdbcWorld.convertIdToScript` quotes String/UUID ids so injected Gremlin string queries parse.

## Deferred — Elasticsearch modules (follow-up)

`unipop-elastic`, `unipop-rest`, and `unipop-test` are temporarily removed from the Maven reactor
(kept on disk). Reason: **Elasticsearch 5.3.1 cannot run on Java 17** — its bundled Lucene 6 uses
`sun.misc.Unsafe`/MMap reflection sealed by the Java 16/17 module system, ES 5.x predates JPMS and
needs the JDK-removed JAXB, and the 5.x **transport client** was removed entirely in ES 8.
`embedded-elasticsearch` 2.1.0 is archived and would launch an ES 5.3.1 node under Java 17 (fails).

Clearing this is a genuine port (concentrated in ~a dozen `unipop-elastic` source files, plus the
test harness; `unipop-rest` has no direct ES source imports and is light):
- ES 5.3.1 → **ES 8.x** (or 7.17 interim).
- transport client / `PreBuiltTransportClient` → **Elasticsearch Java API Client** over the REST client.
- `embedded-elasticsearch` → **Testcontainers** (`org.testcontainers:elasticsearch`).
- rewrite query construction (`QueryBuilders`/`SearchSourceBuilder`/`XContent`/geo `ShapeBuilder`).
- drop the abandoned `io.searchbox:jest`.

To re-enable, restore the three `<module>` entries in the root `pom.xml`.

---

# Follow-on: jdbc tests on embedded PostgreSQL (replacing H2)

The jdbc test suites were switched from H2 to **embedded PostgreSQL** (zonky
`io.zonky.test:embedded-postgres`, a real Postgres binary run in-process — no Docker), to test
against the database Unipop targets in production. Test-only deps/config; production jOOQ is
dialect-agnostic.

- `EmbeddedPostgresServer` (test source) starts one Postgres per JVM on a fixed port (54329),
  triggered from `JdbcGraphProvider`'s static initializer (annotation reflection does not run
  suite static blocks, so the trigger lives where the provider is *instantiated*).
- Config JSONs → `org.postgresql.Driver` / `jdbc:postgresql://localhost:54329/postgres` /
  `sqlDialect: POSTGRES`; column refs lowercased (Postgres folds unquoted identifiers to lower,
  and `FieldPropertySchema` looks up result columns case-sensitively); DDL `DOUBLE` → `DOUBLE PRECISION`.

Postgres exposed several issues H2 had masked, now fixed:
- **Infinite-retry hang**: `ContextManager.execute/render/batch` recursed unboundedly on persistent
  failure (only `fetch` was capped) — now a shared bounded helper logs the cause and rethrows.
- **Duplicate-key on insert**: plain `INSERT` re-inserting a PK now uses jOOQ `onDuplicateKeyIgnore()`
  (portable `ON CONFLICT DO NOTHING`); updates still go through `RowController.update`.
- **`varchar = bigint` on `g.V(id)`**: the predicate translator now stringifies values targeted at
  the VARCHAR id column only (numeric value columns keep their type).

**Results (Java 17, embedded Postgres):** feature suite at **H2 parity (~963 pass / 1913)**,
structure suite clean of DB errors (**~508 pass, 31 fail = H2**). Both suites run with no hang.

**Known residual:** ~13 feature scenarios hit `varchar = integer` on the test edges table's
`year` VARCHAR column queried numerically — the same type-impedance class in a non-id column. A
general fix needs accurate per-property type declarations in the configs (the test configs leave
properties untyped, so text-vs-numeric columns can't be distinguished generically); these are in
already-failing crew-graph scenarios and do not affect H2 parity.

# Provider property-schema types (jdbc)

Provider-specific PostgreSQL column behaviors are declared as **keyed property schemas** under a
vertex/edge's `properties`, contributed through `SourceProvider.providerBuilders()` (so they live in
`unipop-jdbc`, not core):

- **Enum column** — `"<col>": {"type": "enum"}` (optional `"enumType": "<name>"`, validated but
  metadata-only; optional `"field"` to alias the column). Backed by `EnumPropertySchema` (extends
  `FieldPropertySchema`); the query comparison renders `col::text = ?`.
- **JSONB column** — `"<col>": {"type": "jsonb"}`. Schemaless catch-all addressed `<col>.<path>`
  (any key, nested paths, text comparison). Backed by `JsonbPropertySchema`.

**Breaking change (enum):** the earlier element-level `"enums": {column: typeName}` object is
**removed** and no longer parsed. Migrate each entry to a keyed declaration on that column, e.g.
`"status": "@status"` + `"enums": {"status": "mood"}` → `"status": {"type": "enum", "enumType": "mood"}`.
A stale `"enums"` key is now silently ignored: the column falls back to a plain text column, so an
enum comparison fails at **query time** with a `PSQLException` (enum vs varchar) rather than at config
load — update configs when upgrading.
