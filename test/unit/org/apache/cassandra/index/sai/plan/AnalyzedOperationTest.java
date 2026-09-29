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
package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * How several analyzed relations on one column become index expressions when one of them has no
 * index, for example because the index was dropped after the query was prepared.
 */
public class AnalyzedOperationTest extends SAITester
{
    @Test
    public void analyzedRelationsNeverFoldWithoutAnIndex()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, m map<text, text>, t text)");
        createIndex("CREATE INDEX ON %s(VALUES(m)) USING 'sai' WITH OPTIONS = { 'index_analyzer' : 'standard' }");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ReadCommand command = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        QueryController controller = new QueryController(cfs, command, null, new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)));
        ColumnMetadata m = cfs.metadata().getColumn(ColumnIdentifier.getInterned("m", false));
        ColumnMetadata t = cfs.metadata().getColumn(ColumnIdentifier.getInterned("t", false));

        // MATCH on the values has an index, MATCH KEY on the keys has none. The relation without an index
        // gets an expression of its own and leaves the bound of the indexed one alone.
        RowFilter onMap = RowFilter.create(false);
        onMap.add(m, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose("alpha"));
        onMap.add(m, Operator.ANALYZER_MATCHES_KEY, UTF8Type.instance.decompose("beta"));
        List<Expression> mapExpressions = Operation.buildIndexExpressions(controller, new ArrayList<>(onMap.getExpressions())).expressionsFor(m);
        assertEquals(mapExpressions.toString(), 2, mapExpressions.size());
        assertFalse(mapExpressions.get(0).isNotIndexed());
        assertEquals("alpha", UTF8Type.instance.compose(mapExpressions.get(0).lower().value.raw));
        assertTrue(mapExpressions.get(1).isNotIndexed());
        assertEquals("beta", UTF8Type.instance.compose(mapExpressions.get(1).lower().value.raw));

        // Two relations with no index keep one expression each, neither is lost
        RowFilter onText = RowFilter.create(false);
        onText.add(t, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose("x"));
        onText.add(t, Operator.ANALYZER_MATCHES, UTF8Type.instance.decompose("y"));
        List<Expression> textExpressions = Operation.buildIndexExpressions(controller, new ArrayList<>(onText.getExpressions())).expressionsFor(t);
        assertEquals(textExpressions.toString(), 2, textExpressions.size());
        assertTrue(textExpressions.get(0).isNotIndexed());
        assertEquals("x", UTF8Type.instance.compose(textExpressions.get(0).lower().value.raw));
        assertTrue(textExpressions.get(1).isNotIndexed());
        assertEquals("y", UTF8Type.instance.compose(textExpressions.get(1).lower().value.raw));
    }
}
