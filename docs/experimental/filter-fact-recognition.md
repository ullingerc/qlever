# Filter fact recognition (experimental)

Status: experimental, behind the runtime parameter
`enable-filter-fact-recognition` (default `false`). With the switch off, query
plans are exactly the same as without this layer.

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
supported by extending the canonicalizer or adding a recognizer. Neither the
planner nor the consumer has to change.

## Design

```
group graph pattern ──► Definitions ──┐
                                      ▼
FILTER expression ─────────────► canonicalize ──► Conjuncts ──► recognizers ──► Facts ──► consumers
```

All code lives in `src/engine/filterFacts/`.

1. **Definitions** (`Canonicalizer.h`, `collectDefinitions`). For one group
   graph pattern, a single scan over its direct children records:
   - `constants_`: the possible values of a variable from an inline `VALUES`
     clause of the group, if its column contains no `UNDEF`. Several rows give
     several values.
   - `expressions_`: the defining expression `e` of `BIND(e AS ?v)`. It is
     recorded only if `e` is deterministic, contains neither `NOW()` nor
     `EXISTS`, and all input variables of `e` are always bound by the patterns
     *before* the `BIND` in the same group. "Always bound" is a minimal,
     conservative analysis: only the variables of triples and of `VALUES`
     columns without `UNDEF` count.
2. **Canonicalizer** (`canonicalize`). Each FILTER becomes a list of
   `Conjunct`s, each either a `Constraint{lhs, op, rhs}` or an `Opaque`
   expression:
   - `&&` is split into conjuncts.
   - Definitions are substituted: a variable term is replaced by its `BIND`
     expression, and a variable constant by its `VALUES` values. A geometry
     argument of `geof:distance` is replaced by its value if it has exactly one,
     and that value can be a geometry (a literal or a `GeoPoint`).
   - Constants are folded (`+ - * /` on numeric constants).
   - A comparison is flipped so the constant is on the right
     (`500 >= d` → `d <= 500`).
   - `IN` and `||` of equalities become one `=` constraint with several
     constants.
   - Distances are normalized to metres using the unit argument.

   `Constraint{lhs, op, rhs}` means "`lhs op c` holds for some `c` in `rhs`".
   The `lhs` is a `Term`: a `Variable`, a `Distance{a, b}` in metres, a
   `Lang{var}`, or an `Opaque` expression. Everything the canonicalizer does
   not model stays `Opaque`, and nothing is substituted inside an `Opaque`
   expression.
3. **Recognizers** (`Recognizers.h`). Each recognizer is a small function from
   a `Conjunct` to `std::optional<SomeFact>`. `Fact` is a `std::variant` of all
   fact types. Currently:
   - `DistanceBound{a, b, maxMeters}` from `distance(a, b) <= c` or `< c` with
     numeric `c`. With several constants, the maximum is used.
   - `LangIn{var, langs}` from `LANG(?x) = "l"` (also via `IN` and `||`) with
     plain string literals `"l"`.

   `recognizeFacts(group)` runs the whole pipeline for a group.
4. **Consumers.** Two exist, both called by the `QueryPlanner` at the start of
   planning a group graph pattern (`GraphPatternPlanner` constructor):
   - `DistanceBound` → a `SpatialJoin` filter substitute
     (`rewriteDistanceBoundToSpatialJoin` in `src/engine/QueryRewriteUtils.h`,
     called from `QueryPlanner::seedFactSubstitutes`).
   - `LangIn` → `GraphPattern::addLanguageFilter`, which replaces `?s <p> ?x`
     by `?s @en@<p> ?x` or adds a `ql:langtag` triple.

   With the switch on, these consumers replace the old syntactic rewrites for
   the same shapes. The parser no longer applies its LANG rewrite, and
   `rewriteFilterToSpatialJoin` skips the distance shape.
5. **Residual filter.** The original FILTER always stays in the plan, on top
   of the rewritten operator. A recognizer therefore only has to be **sound**
   (FILTER ⇒ fact), and it may over-approximate. For example,
   `VALUES ?r { 500 1000 } FILTER(dist <= ?r)` gives a `SpatialJoin` with a
   radius of 1000 m, and the residual FILTER does the rest.

   In the planner, every `DistanceBound` gets an entry with `isImplied_ = true`
   in the list of filters and substitutes. Such an entry only offers its
   `SpatialJoin` and is never applied as a regular filter. The original FILTER
   keeps its own entry. The implied entries come first in the list, because the
   bits in `SubtreePlan::_idsOfIncludedFilters` are list positions that are
   carried across the planning rounds of a group.

## Correctness conditions

| Condition | How it is enforced | Tests |
|---|---|---|
| A `BIND` is substituted only if all its inputs are always bound before it, and it is deterministic. | `collectDefinitions` scans the group in order and tracks the always-bound variables. The expression must satisfy `isDeterministic()` and contain no `NOW()` (which counts as deterministic in QLever) and no `EXISTS`. | `FilterFactsDefinitions.CollectDefinitions`, `QueryPlannerFilterFacts.NoRewrite` |
| Only inline `VALUES`, never a trailing `VALUES` after `WHERE {}`, and never a column with `UNDEF`. | Only the direct children of the group are scanned. Columns with `UNDEF` are skipped. A trailing `VALUES` is not among them: without `GROUP BY`, the parser moves the `WHERE` clause into a nested group and joins the `VALUES` with it in a new root group (see `ParsedQuery.cpp`), so the FILTERs of the `WHERE` clause are in a different scope. With `GROUP BY`, it is stored in `ParsedQuery::postQueryValuesClause_`. | `FilterFactsDefinitions.CollectDefinitions`, `QueryPlannerFilterFacts.NoRewrite` |
| No substitution inside `BOUND(...)` or `EXISTS`. | The canonicalizer only descends into the shapes it models. All other expressions are `Opaque`. | `FilterFactsCanonicalizer.NoSubstitutionInsideBoundOrExists` |
| Typed constants. | Recognizers check the type of every constant. `getNumericValue` accepts only `xsd:int` and `xsd:double`, so the string `"500"` is rejected. `LangIn` accepts only literals without language tag or datatype (other than `xsd:string`). | `FilterFactsRecognizers.*` |
| No crossing of scopes. | Definitions come only from the direct children of the group, and facts only from the group's own FILTERs. | `FilterFactsDefinitions.CollectDefinitions`, `QueryPlannerFilterFacts.NoRewrite` |
| Same results. | The residual filter is always applied. | `QueryPlannerFilterFacts.ResultsAreEqual`, `QueryPlannerFilterFacts.LanguageFilterResultsAreEqual` |

## How to add a recognizer

Worked example: `FILTER(?x = <c>)` (also through `VALUES` or `BIND`) should
yield a fact `VarEquals{?x, <c>}` that a consumer can use, for example to
replace `?x` by a constant in the triples.

1. **Fact type.** Add the struct to `Recognizers.h` and to the variant:

   ```cpp
   // The variable `var_` is bound to `value_`.
   struct VarEquals {
     Variable var_;
     TripleComponent value_;
     QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(VarEquals, var_, value_)
   };
   using Fact = std::variant<DistanceBound, LangIn, VarEquals>;
   ```

2. **Recognizer.** Add a function to `Recognizers.cpp` and call it from
   `recognize`:

   ```cpp
   std::optional<VarEquals> recognizeVarEquals(const Conjunct& conjunct) {
     const auto* constraint = std::get_if<Constraint>(&conjunct);
     if (!constraint || constraint->op_ != Comparison::EQ ||
         constraint->rhs_.size() != 1) {
       return std::nullopt;
     }
     const auto* var = std::get_if<Variable>(&constraint->lhs_);
     if (!var) {
       return std::nullopt;
     }
     return VarEquals{*var, constraint->rhs_.front()};
   }
   ```

   Because the canonicalizer already flips comparisons and substitutes
   `VALUES` and `BIND`, this one function also covers `FILTER(<c> = ?x)`,
   `VALUES ?v { <c> } FILTER(?x = ?v)` and `BIND(<c> AS ?v) FILTER(?x = ?v)`.
   Before you rely on an equality, check the type rules of SPARQL `=` (for
   example, `1 = 1.0` is true).

3. **Unit tests.** Add positive and negative cases to `test/FilterFactsTest.cpp`
   (see the existing `FilterFactsRecognizers` tests).

If the shape you need isn't produced by the canonicalizer yet (for example a
new function on the left-hand side), add a `Term` alternative in
`Canonicalizer.h` and handle it in `Canonicalizer::toTerm`. Every alternative
must have `operator==`, so `||` of equalities can be merged.

## How to add a consumer

A consumer reads facts and changes the plan. It must keep the original FILTER.

- **Rewrite of the parsed group** (like `LangIn`). In the constructor of
  `QueryPlanner::GraphPatternPlanner` (`QueryPlanner.cpp`), handle the new
  fact type in the visitor over the result of `recognizeFacts`. Do not remove
  the filter from `rootPattern->_filters`.
- **Filter substitute** (like `DistanceBound`). Collect the facts in the
  `GraphPatternPlanner` constructor, and create one implied entry per fact in
  `QueryPlanner::seedFactSubstitutes`. The entry's `filter_` must contain
  exactly the variables of the substitute plan, because the planner uses them
  to connect components and to decide when to apply the substitute. Use a
  synthesized filter that is equivalent to the fact, see
  `distanceBoundToFilter` in `src/engine/QueryRewriteUtils.h`. Add the new
  facts to `wrapFiltersWithoutSubstitutes` as well, so that the positions of
  the entries stay the same.

Add planner tests with the switch on and off, and a result-equality test that
runs the query on a small index with the switch on and off
(`test/QueryPlannerFilterFactsTest.cpp`).

## Known limitations and next steps

- **Cross-scope filters.** Substitution and facts never cross group, subquery,
  `UNION` branch, `OPTIONAL` or `MINUS` boundaries. A FILTER in an outer group
  that refers to a `BIND` in a nested group is not recognized. Moving filters
  (or facts) across scopes is the natural next step. It needs a proper
  always-bound / possibly-bound analysis across scopes.
- **Conservative always-bound analysis.** Only triples and `VALUES` columns
  without `UNDEF` count as always binding. Variables bound in nested groups,
  `UNION`s (bound in all branches), subqueries, `SERVICE` or by property paths
  through `seedFromPropertyPath` are not used.
- **No variable equalities.** `FILTER(?x = ?y)` and `BIND(?x AS ?y)` are not
  recorded as definitions.
- **Limited folding.** Only `+ - * /` on numeric constants, with at least one
  side single-valued. Division by zero is not folded. Integer arithmetic is
  done in `double`.
- **Floating-point radius.** The distance in metres is computed from the unit
  argument in floating point, as in the existing rewrite. A geometry at almost
  exactly the maximum distance may be classified differently by the
  `SpatialJoin` than by the residual FILTER.
- **Plan changes with the switch on.** The plain shape
  `FILTER(geof:distance(...) <= c)` now also keeps its residual FILTER, and
  `LANG` filters are no longer removed. This costs one extra `Filter`
  operation per rewritten FILTER.
- **Facts per group, not per plan.** A fact is used as soon as its group is
  planned. Facts derived from a FILTER whose variables are never bound in the
  group still produce a substitute that is simply never completed. It is never
  chosen, see the corresponding case in `QueryPlannerFilterFacts.ResultsAreEqual`.
- **The `LANG` consumer mutates the parsed group.** `addLanguageFilter`
  rewrites the triples of the group when the planner starts planning it. This
  is safe because every parsed query is planned once (the argument of an
  `EXISTS` is copied before it is planned). Planning the same `ParsedQuery`
  twice with the switch on would apply the language tag twice.
