/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.functions;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SaiFunctionsTest extends CQLTester
{
    @Test
    public void analyzeWithBuiltInName() throws Throwable
    {
        createSingleRowTable();
        assertRows(execute("SELECT sai_analyze('The Quick Fox', 'standard') FROM %s"),
                   row(list("the@0", "quick@1", "fox@2")));
    }

    @Test
    public void analyzeWithJsonConfig() throws Throwable
    {
        createSingleRowTable();
        assertRows(execute("SELECT sai_analyze('quick the fox', '{\"tokenizer\": {\"name\": \"standard\"}, " +
                           "\"filters\": [{\"name\": \"lowercase\"}, {\"name\": \"stop\"}]}') FROM %s"),
                   row(list("quick@0", "fox@2")));
    }

    @Test
    public void analyzeKeepsDuplicates() throws Throwable
    {
        createSingleRowTable();
        assertRows(execute("SELECT sai_analyze('fox fox fox', 'standard') FROM %s"),
                   row(list("fox@0", "fox@1", "fox@2")));
    }

    @Test
    public void analyzeAllStopwordsReturnsEmptyList() throws Throwable
    {
        createSingleRowTable();
        assertRows(execute("SELECT sai_analyze('the and of', '{\"tokenizer\": {\"name\": \"standard\"}, " +
                           "\"filters\": [{\"name\": \"lowercase\"}, {\"name\": \"stop\"}]}') FROM %s"),
                   row(list()));
    }

    @Test
    public void analyzeEmptyString() throws Throwable
    {
        createSingleRowTable();
        assertRows(execute("SELECT sai_analyze('', 'keyword') FROM %s"), row(list("@0")));
        assertRows(execute("SELECT sai_analyze('', 'standard') FROM %s"), row(list()));
    }

    @Test
    public void analyzeNullArgumentsReturnNull() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, c text)");
        execute("INSERT INTO %s (k) VALUES (0)");
        assertRows(execute("SELECT sai_analyze(v, 'standard') FROM %s"), row((Object) null));
        assertRows(execute("SELECT sai_analyze('quick fox', c) FROM %s"), row((Object) null));
    }

    @Test
    public void analyzeInvalidConfigFailsWithParserMessage() throws Throwable
    {
        createSingleRowTable();
        assertThatThrownBy(() -> execute("SELECT sai_analyze('quick fox', 'bogus') FROM %s"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Unknown analyzer 'bogus'. Valid built-in analyzers: ")
        .hasMessageEndingWith("A custom analyzer must be a JSON object.");

        assertThatThrownBy(() -> execute("SELECT sai_analyze('quick fox', '{\"tokenizer\": {\"name\": \"bogus\"}}') FROM %s"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Unknown tokenizer 'bogus'. Available: ");
    }

    @Test
    public void analyzeClassInjectionFails() throws Throwable
    {
        createSingleRowTable();
        assertThatThrownBy(() -> execute("SELECT sai_analyze('quick fox', '{\"filters\": [{\"name\": \"synonymGraph\", " +
                                         "\"args\": {\"synonyms\": \"syn.txt\", \"analyzer\": \"evil.Clazz\"}}]}') FROM %s"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith("Analyzer options must not load classes (requested '");
    }

    private void createSingleRowTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");
        execute("INSERT INTO %s (k) VALUES (0)");
    }
}
