<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# SAI full-text search

This fork adds full-text search to Storage Attached Indexes (SAI). The work is under
active development on this branch and lands in phases. This document describes what is
implemented as of the current commit and grows with each phase. Anything not listed
under a heading below is not available yet.

## Lucene upgrade

The bundled Lucene libraries (`lucene-core` and `lucene-analysis-common`) are upgraded
from 9.7.0 to 9.12.3. These two artifacts are the only Lucene artifacts shipped, which
bounds the set of available analyzers, tokenizers and filters.

## Analyzer configuration

The analyzer core parses and validates the value space of the `index_analyzer` and
`query_analyzer` index options. The value is either a built-in analyzer name (matched
case insensitively) or a JSON object describing a custom analyzer. You can validate a
config and preview its tokenization with the `sai_analyze` function described below,
and create analyzed indexes as described under "Analyzed indexes".

### Built-in analyzer names

Core analyzers:

    standard, simple, whitespace, keyword, stop, lowercase

Language analyzers:

    arabic, armenian, basque, bengali, brazilian, bulgarian, catalan, cjk, czech,
    danish, dutch, english, estonian, finnish, french, galician, german, greek,
    hindi, hungarian, indonesian, irish, italian, latvian, lithuanian, nepali,
    norwegian, persian, portuguese, romanian, russian, serbian, sorani, spanish,
    swedish, tamil, telugu, thai, turkish

That is 45 names in total. Analyzers needing Lucene artifacts this fork does not ship
(for example japanese, korean or polish) are rejected with a message listing the valid
names.

### Custom analyzer JSON

A custom analyzer is a JSON object with the optional keys `tokenizer`, `charFilters`
and `filters`. Each component has a required string `name` and an optional `args`
object whose values are strings, numbers or booleans. Component names are resolved
only through Lucene's factory registries, never by loading classes, and arguments that
would load classes or resource files are rejected. When `tokenizer` is absent the
`keyword` tokenizer is used, so a filters-only config is valid.

Example:

    {
      "tokenizer": { "name": "ngram", "args": { "minGramSize": "3", "maxGramSize": "3" } },
      "filters": [ { "name": "lowercase" } ]
    }

The raw option value is limited to 8192 characters and at most 32 components across
`charFilters` and `filters`.

## The sai_analyze function

`sai_analyze(value text, analyzer_config text)` returns `list<text>`. The second
argument accepts exactly the `index_analyzer` value space, with the same validation
and the same error messages. Each returned element is one token occurrence in emission
order, duplicates included, in the form `token@position`. Positions are zero based and
gaps left by filters that remove tokens are preserved.

    SELECT sai_analyze('The Quick Brown Fox', 'standard')

returns

    ['the@0', 'quick@1', 'brown@2', 'fox@3']

An analyzer that removes stopwords leaves a gap. With the `english` analyzer the
leading article is dropped and stemming applies:

    SELECT sai_analyze('The quick foxes', 'english')

returns

    ['quick@1', 'fox@2']

Either argument being null returns null. A config that fails validation fails the
query with the same message CREATE INDEX will use.

## Analyzed term guardrails

Analyzed values are bounded by guardrails. A value is either fully indexed or not
indexed at all, and the verdict is the same at memtable insert, flush and compaction.

Three bounds apply per indexed value:

| Guardrail | cassandra.yaml keys | Defaults |
|---|---|---|
| Per token size | `sai_string_term_size_warn_threshold` / `sai_string_term_size_fail_threshold` | 1KiB warn, 8KiB fail |
| Cumulative analyzed size | `sai_analyzed_size_warn_threshold` / `sai_analyzed_size_fail_threshold` | 1MiB warn, 8MiB fail |
| Token count | `sai_analyzed_tokens_warn_threshold` / `sai_analyzed_tokens_fail_threshold` | disabled (-1) |

The two size guardrails take size values such as `1MiB`. The token count guardrail
takes a plain count and ships disabled. All are live updatable through the
`org.apache.cassandra.db:type=Guardrails` MBean, like every other guardrail.

At the warn threshold a client mutation succeeds and the client receives a warning.
At the fail threshold a client mutation is rejected. On paths with no client, such as
compaction and index rebuild, a breach of a fail threshold means the whole value is
not indexed, a counter is incremented and a rate limited log line names the column and
key. The cumulative size bound exists because analyzers such as ngram can produce far
more indexed bytes than the input value contains.

## Analyzed indexes

CREATE INDEX accepts the `index_analyzer` and `query_analyzer` options on SAI indexes over
text columns, including non-frozen collections of text. An analyzed index tokenizes every
value it indexes and stores token positions, so it can answer the match and phrase
operators below. Values written to an analyzed index are indexed per token, with the
guardrails above applied per value.

    CREATE CUSTOM INDEX ON ks.articles(body) USING 'sai'
      WITH OPTIONS = { 'index_analyzer': 'english' };

`query_analyzer` defaults to `index_analyzer` when absent. Analysis options are rejected
on primary key columns and on non-text columns.

## The `MATCH` operator

`column MATCH 'value'` matches rows whose analyzed column contains every token the query
analyzer emits for the value. Multiple tokens are combined with AND semantics:

    SELECT * FROM ks.articles WHERE body MATCH 'quick fox';

matches rows whose body contains a token `quick` and a token `fox`, in any order and any
distance apart. A value the query analyzer emits no tokens for, for example one consisting
only of stopwords, consistently matches nothing.

On a non-frozen collection of text, `MATCH` searches the words of every element: each
token may come from any element.

CONTAINS and CONTAINS KEY always compare whole elements and whole keys. An index with an
`index_analyzer` holds words, so it cannot answer them. When every index on the target is
analyzed, they are refused with or without ALLOW FILTERING. CONTAINS is refused with an
error that points to `MATCH` or `PHRASE`. CONTAINS KEY is refused with an error that points
to `MATCH KEY` or `PHRASE KEY`. When another index covers the same target,
for example a plain storage-attached index or a legacy secondary index, that index answers
CONTAINS and CONTAINS KEY as usual.

`MATCH` requires a storage-attached index with an `index_analyzer` on the column. `MATCH`,
`PHRASE`, `MATCH KEY` and `PHRASE KEY` are not allowed in UPDATE or DELETE WHERE clauses. LWT IF conditions never take `MATCH`,
`PHRASE`, `MATCH KEY` or `PHRASE KEY`: such a condition is a syntax error, and conditions
always compare raw column values.

`MATCH` is an unreserved keyword, so `match` can be used as a column, table or user name.
There is no `:` operator. `column : 'value'` is a syntax error, write it as `column MATCH 'value'`.

## The `PHRASE` operator

`column PHRASE 'value'` matches rows whose analyzed column contains the value's tokens at
strictly adjacent positions:

    SELECT * FROM ks.articles WHERE body PHRASE 'quick fox';

Adjacency is gap preserving, zero based, with Lucene `PhraseQuery` slop 0 semantics. If
the analyzer removes stopwords but preserves position gaps, `'quick fox'` does not match
a stored `'quick the fox'` (indexed as `quick@0, fox@2`), while the query `'quick the
fox'` does. Users who want stopwords to be invisible configure an analyzer that compacts
position increments. A phrase whose analysis emits no tokens matches nothing.

On non-frozen collections each element has its own position space, so a phrase never
matches across element boundaries.

Phrase candidates are intersected inside each index segment on the stored positions, and
every returned row is re-checked by re-analyzing the stored value, on the replica and,
for reads that reconcile multiple replicas, again on the coordinator.

## `MATCH KEY` and `PHRASE KEY` on map keys

`MATCH KEY` and `PHRASE KEY` search the keys of a non-frozen map through an analyzed index
on `KEYS(column)`:

    CREATE INDEX ON ks.events (KEYS(attrs)) USING 'sai' WITH OPTIONS = {'index_analyzer': 'standard'};
    SELECT * FROM ks.events WHERE attrs MATCH KEY 'error code';
    SELECT * FROM ks.events WHERE attrs PHRASE KEY 'error code';

`MATCH KEY` has the semantics of `MATCH` over the keys: each token may come from any key.
`PHRASE KEY` has the semantics of `PHRASE` within one key: a phrase never matches across
two keys. `attrs MATCH 'x'` and `attrs PHRASE 'x'` search the map's values, through an
analyzed index on `VALUES(attrs)`. A map can have both indexes, each with its own analyzer.

`MATCH KEY` and `PHRASE KEY` need a non-frozen map column, and are refused on any other
column. Without an analyzed index on `KEYS(column)` they are refused with an error naming
that index, also when the map has an analyzed index on its values. `CONTAINS KEY` compares
whole keys, through a plain index on `KEYS(column)`.

## `=` on analyzed columns

The `equals_behaviour_when_analyzed` index option decides what `=` means on a column with
an `index_analyzer`:

| Option value | Behaviour of `=` on the analyzed column |
|---|---|
| `UNSUPPORTED` (default) | The query is rejected with an error suggesting the `MATCH` operator |
| `MATCH` | `=` behaves exactly like the `MATCH` operator, and the client receives a warning |

LWT IF conditions are unaffected: they always compare raw bytes, so under `MATCH` a
SELECT with `=` and an IF condition with `=` can disagree by design.

## The `OR` operator

WHERE clauses accept `OR` between predicates, with `AND` binding tighter and parentheses
overriding precedence:

    SELECT * FROM ks.articles WHERE category = 'news' OR (score > 100 AND body MATCH 'fox');

Semantics are those of strict boolean evaluation over the reconciled row: a row is
returned when the merged, newest version of its data satisfies the expression. Rows
matching several disjuncts are returned once. Analyzed `MATCH` and `PHRASE` predicates keep
their own semantics inside a disjunction, so the tokens of one `MATCH` value stay AND
combined within that predicate.

Restrictions in this cut, each refused with a clear error:

* `OR` may only touch regular and static columns. Partition key columns (including
  `token()`), and clustering columns are not supported inside `OR`.
* `IN`, `LIKE`, `IS NOT NULL`, `expr()` custom index expressions and ANN cannot appear
  inside `OR`, and a query combining `OR` with ANN ordering or a custom index expression
  is refused as a whole.
* A partition key `IN` restriction cannot be combined with `OR` anywhere in the query,
  since a query containing `OR` always runs as one contiguous range read.
* `OR` is not supported in UPDATE or DELETE statements, in LWT IF conditions (not
  expressible), in materialized view definitions, or on virtual tables.

ALLOW FILTERING rule: a disjunction runs on the SAI index path, with one index union per
`OR`, when every predicate of every branch has a storage-attached index (the only index
implementation that understands disjunctions). Otherwise the whole query requires
ALLOW FILTERING and is evaluated by filtering. There is no cost model for unions in this
cut, so very unselective disjunctions can materialize large key sets; the standard SAI
guardrails still apply per predicate.

When a read waits for two or more replicas, on the index path and on the filtering path
alike, the replicas together return every row that could match once their copies are merged,
and the coordinator filters the merged rows. On the filtering path a replica sends every row that
matches one chosen condition of each AND group, preferring a condition that is not on a
static column. Every node must use the same rule for choosing that condition. A release
that changes the rule says so in its upgrade notes. For an `OR` query that restricts static
columns, when a replica's static values for a partition are older than another replica's,
the coordinator reads that whole partition from it. This keeps rows whose matching values
were written to different replicas, at the cost of more rows sent to the coordinator. A
disjunction over wide partitions can therefore reach the replica filtering protection limit
`replica_filtering_protection.cached_rows_fail_threshold` and fail instead of returning
incomplete results.

A read that waits for one replica filters strictly on it, as at `ONE`. That is `ONE`,
`LOCAL_ONE` and `NODE_LOCAL`, any level on a keyspace with a replication factor of 1, and
`LOCAL_QUORUM` or `LOCAL_SERIAL` in a data center that holds one replica of the keyspace. Such a
read sends no extra rows, and it accepts an `IN` restriction next to `OR`, as `ONE` does. The
intersect filtering guardrail still applies to it as to any read above `ONE`.

A query without `OR` keeps the Apache Cassandra behaviour: with ALLOW FILTERING at a
consistency level above `ONE`, it can miss a row whose matching values were written to
different replicas.

Static columns may appear inside `OR`. One boundary case to know: a partition whose only
content is a matching static row (no regular rows at all) produces no result row for a
disjunction that also restricts regular columns, on the index path and the filtering path
alike. The synthetic row Cassandra emits for row-less partitions is reserved for queries
without regular column restrictions, exactly as for conjunctions.

## Cluster upgrade rule

The `MATCH`, `PHRASE`, `MATCH KEY` and `PHRASE KEY` operators, `=` under `MATCH`
behaviour, and `OR` in WHERE clauses are refused with an InvalidRequest error until every
node in the cluster runs this build.
Nodes advertise a fork messaging version and the coordinator checks that all live peers
speak it before accepting one of these queries. During a rolling upgrade the queries fail
fast with a message naming a node that has not been upgraded, instead of returning wrong
results from a node that cannot evaluate them. As a backstop, a filter containing `OR`
refuses to serialize towards a peer on the vanilla messaging version rather than ever
flattening to a conjunction.

A node only advertises the fork version when its `storage_compatibility_mode` is
not `CASSANDRA_4` (that is, `UPGRADING` or `NONE`). Clusters still running with
`CASSANDRA_4` compatibility, the 5.0 default in
`cassandra.yaml`, keep the analyzed operators gated off until the mode is lifted,
exactly like other 5.0-level features.

## Tracing

With tracing enabled these queries emit events at each decision point:

* how the query analyzer tokenized the queried value, tokens with positions
* the shape of the index query tree, operators and expression counts per node, for
  queries containing `OR`
* per segment phrase intersection statistics, candidates in and matches out, for both
  memtable and sstable segments
* post-filter counts, rows matched of rows checked, on replicas and on the coordinator
  during replica filtering protection

## Not yet available

Coming in later commits on this branch:

* BM25 relevance scoring and `ORDER BY`
* metrics for dropped values, oversize tokens and analyzer config errors
