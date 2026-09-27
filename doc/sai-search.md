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
config and preview its tokenization today with the `sai_analyze` function described
below. Creating an index with these options is not wired up yet, it arrives with the
index write and read path phases.

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

## Not yet available

Coming in later commits on this branch:

* the `:` match operator and the `PHRASE` operator
* `OR` in queries against analyzed indexes
* BM25 relevance scoring and `ORDER BY`
* `index_analyzer` and `query_analyzer` accepted by CREATE INDEX
* metrics for dropped values, oversize tokens and analyzer config errors
