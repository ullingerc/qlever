# Filter fact recognition (experimental)

Status: experimental branch, always on (there is no runtime switch). The
branch is built on the stack of upstream PRs (`filter-facts-canonicalizer`,
`-planner`, `-definitions`, `-constants`, `-multi-row-values`,
`-unit-variable`, `-lang`, `-lang-planner`, #3904 and #3908); this document
describes the stack together with the features of this branch that are not in
the stack yet (swapped comparisons, arithmetic folding). The earlier design
with implied filter-substitute entries and a covered-filter bit (instead of
synthesized FILTERs) is kept on the branch
`filter-fact-recognition-pre-reconcile`.

## Problem

Many of QLever's rewrites match a single syntactic shape. For example, this
FILTER is turned into a `SpatialJoin`:

```sparql
FILTER(geof:distance(?a, ?b, unit:M) <= 500)
```

but these semantically equivalent shapes are not:

```sparql
BIND(geof:distance(?a, ?b, unit:M) AS ?d) FILTER(?d <= 500)
VALUES ?r { 500 } FILTER(geof:distance(?a, ?b, unit:M) <= ?r)
```

Filter fact recognition is a small, generic layer between the FILTERs of a
group graph pattern and the rewrites that use them. A rewrite (a *consumer*)
reads typed *facts* instead of matching expression trees. A new shape is
supported by extending the canonicalizer or adding a recognizer.

## Design

```
group graph pattern ──► Definitions ──┐
                                      ▼
FILTER expression ─────────────► canonicalize ──► Conjuncts ──► recognizers ──► facts ──► rewriteFiltersWithFacts
```

The code lives in `src/engine/filterFacts/` and in `rewriteFiltersWithFacts`
(`src/engine/QueryRewriteUtils.h`).

1. **Definitions** (`Canonicalizer.h`, `collectDefinitions`). For one group
   graph pattern, a single scan over its direct children records:
   - `constants_`: the values of a variable from an inline `VALUES` clause of
     the group, if its column contains no `UNDEF`. Several rows give several
     values. Also the value of `BIND(c AS ?v)` for a constant `c`
     (`SparqlExpressionPimpl::getConstantValue`, numbers come as an `Id`),
     like a `VALUES` clause with a single row.
   - `expressions_`: the expression `e` of any other `BIND(e AS ?v)`, if `e`
     is deterministic, contains neither `NOW()` nor `EXISTS`, and all its
     variables are always bound by the patterns *before* the `BIND` (only
     triples, `VALUES` columns without `UNDEF` and `BIND`s of constants
     count). It is removed again
     if a later pattern of the group may bind `?v` (where `e` is undefined,
     that pattern binds `?v` freely).
2. **Canonicalizer** (`canonicalize`). Each FILTER becomes a list of
   `Conjunct`s (split at `&&`), each one of:
   - `Constraint{Distance{a, b}, op, rhs, exact}`: `distance(a, b) < rhs` or
     `<= rhs` in meters. Also with swapped operands (`500 >= d`), with the
     number computed by `+ - * /`, and with the distance, the number and the
     unit from the definitions. With several values (from
     a multi-row `VALUES` clause), `rhs` is the largest one, and `exact` is
     false unless all values are equal.
   - `LangEquals{var, rhs, exact}`: `LANG(?x) = c` for some `c` in `rhs` (from
     `=`, `IN`, and `||` of these, also through `BIND` and `VALUES`).
   - `Opaque`: everything else. Nothing is substituted inside of it.
3. **Recognizers** (`Recognizers.h`):
   - `recognizeDistanceBound`: `DistanceBound{a, b, maxMeters, exact}` (for
     `<`, the largest `double` below the number).
   - `recognizeLangIn`: `LangIn{var, langs, exact}` for plain string literals.
4. **Consumer** (`rewriteFiltersWithFacts`, called at the start of
   `QueryPlanner::optimize` for each group):
   - A `LangIn` fact is applied via `GraphPattern::addLanguageFilter`, which
     replaces `?s <p> ?x` by `?s @en@<p> ?x` or adds a `ql:langtag` triple. If
     this succeeds and the fact is exact, its conjunct is removed.
   - A FILTER with a `DistanceBound` (or a removed conjunct) is split into its
     conjuncts (`SparqlExpressionPimpl::splitConjunction`). A conjunct that is
     a `DistanceBound` only with the definitions is replaced by the synthesized
     FILTER `geof:metricDistance(a, b) <= maxMeters` (`distanceBoundToFilter`).
     The existing filter-substitute rewrite (`rewriteFilterToSpatialJoin`,
     which also uses `canonicalize` and `recognizeDistanceBound`) turns it into
     a `SpatialJoin`.

   The parser no longer applies LANG filters itself (its hook and
   `getLanguageFilterExpression` are removed), so all LANG shapes go through
   this consumer.
5. **Residual filter.** If a fact is not exact, its conjunct stays as well.
   For example, `VALUES ?r { 500 1000 } FILTER(dist <= ?r)` gives a
   `SpatialJoin` with a radius of 1000 m and the original FILTER on top.

## Correctness conditions

| Condition | How it is enforced | Tests |
|---|---|---|
| A `BIND` is substituted only if all its inputs are always bound before it, it is deterministic, and no later pattern may bind its target. | `collectDefinitions` scans the group in order. A later operation whose variables are not tracked (e.g. `SERVICE`) removes all definitions before it. | `FilterFactsCanonicalizer.CollectDefinitions`, `QueryPlannerFilterFacts.NoRewrite`, `QueryPlannerFilterFacts.BindTargetBoundLater` |
| Only inline `VALUES`, never a trailing `VALUES` after `WHERE {}`, and never a column with `UNDEF`. | Only the direct children of the group are scanned (the parser moves the `WHERE` clause of a trailing `VALUES` into a nested group). | `QueryPlannerFilterFacts.NoRewrite` |
| No substitution inside `BOUND(...)` or `EXISTS`. | All shapes that the canonicalizer does not model are `Opaque`. | `FilterFactsCanonicalizer.Opaque` |
| Typed constants. | Numbers must be `xsd:int` or `xsd:double` (the string `"500"` is rejected). `LangIn` accepts only literals without language tag or datatype (other than `xsd:string`). | `FilterFactsCanonicalizer.*`, `FilterFactsRecognizers.*` |
| Same results. | The original conjunct stays unless the fact is exact. | `QueryPlannerFilterFacts.ResultsAreEqual`, `QueryPlannerFilterFacts.LanguageFilterResultsAreEqual` (compared with the FILTER `(f) \|\| false`, which is never rewritten) |

## How to add a fact

1. Add the canonical form as an alternative of `Conjunct` in `Canonicalizer.h`
   (like `LangEquals`) and produce it in `toConjunct`.
2. Add the fact type and its recognizer to `Recognizers.h`.
3. Consume the fact in `rewriteFiltersWithFacts`. If the rewrite can fail
   (like `addLanguageFilter`), the conjunct must stay then, and it must also
   stay if the fact is not exact.
4. Add unit tests to `test/FilterFactsTest.cpp` and planner and
   result-equality tests to `test/QueryPlannerFilterFactsTest.cpp`.

## Known limitations and next steps

- **Cross-scope filters.** Definitions and facts never cross group, subquery,
  `UNION`, `OPTIONAL` or `MINUS` boundaries.
- **Conservative always-bound analysis.** Only triples, `VALUES` columns
  without `UNDEF` and `BIND`s of constants count as always binding.
- **Limited folding.** Only `+ - * /` on numbers, with at least one side
  single-valued, in `double`. Division by zero is not folded. To be replaced by
  the evaluator of #3897.
- **Synthesized FILTER without `SpatialJoin`.** If the `SpatialJoin` is not
  chosen, the synthesized FILTER is evaluated instead of the original one (for
  a `BIND` of the distance, the distance is then computed twice).
- **Geometry operands from `VALUES` are not resolved (dropped, 2026-11-06).**
  A variable operand with a single value is already joined with its `VALUES`
  clause by the `SpatialJoin`; replacing it by the value would only change the
  child of the `SpatialJoin`.
- **The `LANG` consumer mutates the parsed group.** `addLanguageFilter`
  rewrites the triples of the group when the planner starts planning it. A
  removed conjunct is not seen again, but a kept (inexact) one would be
  applied again if the same `ParsedQuery` was planned twice.
