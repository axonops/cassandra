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
package org.apache.cassandra.index.sai.cql;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AnalyzedTermLimits;
import org.apache.cassandra.service.ClientState;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * The W1 suite: the single {@link org.apache.cassandra.index.sai.analyzer.AnalyzedTermLimits}
 * instance produces the identical verdict at every write path, a value is either fully indexed or
 * not at all, and the only legal divergence is an operator changing a guardrail threshold between
 * write and compaction, which drops the value loudly.
 */
public class AnalyzedTermLimitsIntegrationTest extends SAITester
{
    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void clientMutationBreachingFailThresholdIsRejected()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        String indexName = createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        StorageAttachedIndex index = analyzedIndex(indexName);

        // guardrails only apply to ordinary users, so validate the mutation the way the write
        // path does for client mutations, with a logged in non superuser state
        IAuthenticator previousAuthenticator = DatabaseDescriptor.getAuthenticator();
        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        long previousWarn = Guardrails.instance.getSaiAnalyzedTokensWarnThreshold();
        long previousFail = Guardrails.instance.getSaiAnalyzedTokensFailThreshold();
        Guardrails.instance.setSaiAnalyzedTokensThreshold(1, 2);
        try
        {
            ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 9042));
            clientState.login(new AuthenticatedUser("test_user")
            {
                @Override
                public boolean canLogin()
                {
                    return true;
                }

                @Override
                public boolean isSuper()
                {
                    return false;
                }
            });

            PartitionUpdate update = new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), 0, 1)
                                     .add("body", "one two three")
                                     .buildUpdate();

            assertThatThrownBy(() -> index.validate(update, clientState)).isInstanceOf(InvalidRequestException.class);
        }
        finally
        {
            Guardrails.instance.setSaiAnalyzedTokensThreshold(previousWarn, previousFail);
            DatabaseDescriptor.setAuthenticator(previousAuthenticator);
        }
    }

    @Test
    public void tightenedThresholdDropsValueAtCompactionOnly()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        String indexName = createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        StorageAttachedIndex index = analyzedIndex(indexName);

        execute("INSERT INTO %s (id, body) VALUES (1, 'one two three four five six')");
        flush();

        // the value passed the thresholds at write time, flush never drops
        assertEquals(6, index.cellCount());
        assertEquals(0, index.analyzedTermLimits().droppedValueCount());

        long previousWarn = Guardrails.instance.getSaiAnalyzedTokensWarnThreshold();
        long previousFail = Guardrails.instance.getSaiAnalyzedTokensFailThreshold();
        Guardrails.instance.setSaiAnalyzedTokensThreshold(2, 3);
        try
        {
            // the documented legal divergence: an operator tightened a guardrail between write and
            // compaction, so compaction drops the whole value, loudly
            compact();
            waitForCompactionsFinished();
            assertEquals(0, analyzedIndex(indexName).cellCount());
            assertEquals(1, index.analyzedTermLimits().droppedValueCount());

            // the row itself is untouched, only the index entries are gone
            assertEquals(1, execute("SELECT * FROM %s WHERE id = 1").size());
        }
        finally
        {
            Guardrails.instance.setSaiAnalyzedTokensThreshold(previousWarn, previousFail);
        }
    }

    @Test
    public void oversizeTokenDropsValueAndCounts()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        // the keyword analyzer emits the whole value as one token, the standard analyzer would
        // split anything longer than its token length cap
        String indexName = createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'keyword' }");
        StorageAttachedIndex index = analyzedIndex(indexName);

        // a single token over the default 8KiB sai_string_term_size fail threshold
        StringBuilder token = new StringBuilder();
        for (int i = 0; i < 9 * 1024; i++)
            token.append('a');
        execute("INSERT INTO %s (id, body) VALUES (1, ?)", token.toString());

        assertEquals(1, index.analyzedTermLimits().droppedValueCount());
        assertEquals(1, index.analyzedTermLimits().oversizeTokenCount());

        // the whole value indexed nothing, so the flushed index is empty
        flush();
        assertEquals(0, index.cellCount());
    }

    @Test
    public void identicalVerdictAtMemtableAndCompaction()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        String indexName = createIndex("CREATE INDEX ON %s(body) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        StorageAttachedIndex index = analyzedIndex(indexName);

        long previousWarn = Guardrails.instance.getSaiAnalyzedTokensWarnThreshold();
        long previousFail = Guardrails.instance.getSaiAnalyzedTokensFailThreshold();
        Guardrails.instance.setSaiAnalyzedTokensThreshold(1, 2);
        try
        {
            // with fixed thresholds, memtable add and compaction evaluate the identical bounds on
            // the identical token list, so both drop the value and both count it
            execute("INSERT INTO %s (id, body) VALUES (1, 'one two three')");
            assertEquals(1, index.analyzedTermLimits().droppedValueCount());

            flush();
            assertEquals(1, index.analyzedTermLimits().droppedValueCount());

            compact();
            waitForCompactionsFinished();
            assertEquals(2, index.analyzedTermLimits().droppedValueCount());
            assertEquals(0, analyzedIndex(indexName).cellCount());
        }
        finally
        {
            Guardrails.instance.setSaiAnalyzedTokensThreshold(previousWarn, previousFail);
        }
    }

    @Test
    public void rowElementBoundRejectsClientMutation()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val set<text>)");
        String indexName = createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        assertThatThrownBy(() -> execute("INSERT INTO %s (id, val) VALUES (1, ?)",
                                         elements(AnalyzedTermLimits.MAX_ELEMENTS_PER_ROW + 1)))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining(Integer.toString(AnalyzedTermLimits.MAX_ELEMENTS_PER_ROW));

        // the whole mutation was rejected before anything was written or indexed
        assertEquals(0, execute("SELECT * FROM %s WHERE id = 1").size());
        assertEquals(0, analyzedIndex(indexName).analyzedTermLimits().droppedValueCount());
    }

    @Test
    public void rowElementBoundDropsExcessValuesAtMemtableAndCompaction()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val set<text>)");
        String indexName = createIndex("CREATE INDEX ON %s(val) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");
        StorageAttachedIndex index = analyzedIndex(indexName);

        // apply directly to reach the non client paths with a row a client mutation could not write
        int excess = 5;
        new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), 0, 1)
        .add("val", elements(AnalyzedTermLimits.MAX_ELEMENTS_PER_ROW + excess))
        .build()
        .applyUnsafe();

        // the memtable indexes the first MAX_ELEMENTS_PER_ROW values and drops the rest, counted
        assertEquals(excess, index.analyzedTermLimits().droppedValueCount());
        assertEquals(1, execute("SELECT * FROM %s WHERE val MATCH 'e0'").size());

        flush();
        assertEquals(excess, index.analyzedTermLimits().droppedValueCount());

        compact();
        waitForCompactionsFinished();
        assertEquals(2L * excess, index.analyzedTermLimits().droppedValueCount());
        assertEquals(1, execute("SELECT * FROM %s WHERE val MATCH 'e0'").size());
    }

    private Set<String> elements(int count)
    {
        Set<String> elements = new TreeSet<>();
        for (int i = 0; i < count; i++)
            elements.add("e" + i);
        return elements;
    }

    private StorageAttachedIndex analyzedIndex(String indexName)
    {
        return (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
    }
}
