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
package org.apache.cassandra.cql3;

import org.junit.Test;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.WhereClause.AndElement;
import org.apache.cassandra.cql3.WhereClause.ExpressionElement;
import org.apache.cassandra.cql3.WhereClause.OrElement;
import org.apache.cassandra.cql3.WhereClause.RelationElement;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WhereClauseTest
{
    @Test
    public void testConjunctionKeepsFlatShape() throws Exception
    {
        WhereClause clause = WhereClause.parse("a = 1 AND b = 2");

        assertFalse(clause.containsDisjunction());
        assertEquals(2, clause.relations().size());
        assertEquals(2, clause.root().children().size());
        assertTrue(clause.expressions().isEmpty());
        assertEquals("a = 1 AND b = 2", clause.toCQLString());
    }

    @Test
    public void testSingleRelation() throws Exception
    {
        WhereClause clause = WhereClause.parse("a = 1");

        assertFalse(clause.containsDisjunction());
        assertEquals(1, clause.relations().size());
        assertEquals(1, clause.root().children().size());
        assertTrue(clause.root().children().get(0) instanceof RelationElement);
    }

    @Test
    public void testDisjunction() throws Exception
    {
        WhereClause clause = WhereClause.parse("a = 1 OR b = 2");

        assertTrue(clause.containsDisjunction());
        assertEquals(1, clause.root().children().size());
        OrElement or = (OrElement) clause.root().children().get(0);
        assertEquals(2, or.children().size());
    }

    @Test
    public void testAndBindsTighterThanOr() throws Exception
    {
        WhereClause clause = WhereClause.parse("a = 1 OR b = 2 AND c = 3");

        OrElement or = (OrElement) clause.root().children().get(0);
        assertEquals(2, or.children().size());
        assertTrue(or.children().get(0) instanceof RelationElement);
        AndElement and = (AndElement) or.children().get(1);
        assertEquals(2, and.children().size());

        assertEquals(clause, WhereClause.parse("a = 1 OR (b = 2 AND c = 3)"));
    }

    @Test
    public void testParenthesesOverridePrecedence() throws Exception
    {
        WhereClause clause = WhereClause.parse("(a = 1 OR b = 2) AND c = 3");

        assertEquals(2, clause.root().children().size());
        assertTrue(clause.root().children().get(0) instanceof OrElement);
        assertTrue(clause.root().children().get(1) instanceof RelationElement);
    }

    @Test
    public void testSameOperatorNestingFlattens() throws Exception
    {
        assertEquals(WhereClause.parse("a = 1 AND b = 2 AND c = 3"),
                     WhereClause.parse("a = 1 AND (b = 2 AND c = 3)"));
        assertEquals(WhereClause.parse("a = 1 OR b = 2 OR c = 3"),
                     WhereClause.parse("a = 1 OR (b = 2 OR c = 3)"));
        assertEquals(3, ((OrElement) WhereClause.parse("a = 1 OR (b = 2 OR c = 3)").root()
                                                .children().get(0)).children().size());
    }

    @Test
    public void testParenthesizedSingleRelation() throws Exception
    {
        assertEquals(WhereClause.parse("a > 1"), WhereClause.parse("(a > 1)"));
        assertEquals(WhereClause.parse("a > 1"), WhereClause.parse("((a > 1))"));
    }

    @Test
    public void testTupleRelationsStillParse() throws Exception
    {
        WhereClause clause = WhereClause.parse("(a, b) > (1, 2)");
        assertEquals(1, clause.relations().size());
        assertTrue(clause.relations().get(0).isMultiColumn());

        assertEquals(clause, WhereClause.parse("((a, b) > (1, 2))"));

        WhereClause in = WhereClause.parse("(a, b) IN ((1, 2), (3, 4))");
        assertEquals(1, in.relations().size());
    }

    @Test
    public void testTokenRelations() throws Exception
    {
        assertTrue(WhereClause.parse("token(a) > 5").containsTokenRelations());
        assertFalse(WhereClause.parse("a = 1 OR b = 2").containsTokenRelations());
        assertTrue(WhereClause.parse("a = 1 OR token(b) > 5").containsTokenRelations());
    }

    @Test
    public void testFlatViewsRejectDisjunctions() throws Exception
    {
        WhereClause clause = WhereClause.parse("a = 1 OR b = 2");

        try
        {
            clause.relations();
            fail("The relations view of a disjunctive clause should throw");
        }
        catch (AssertionError e)
        {
            assertTrue(e.getMessage().contains("not defined"));
        }

        try
        {
            clause.expressions();
            fail("The expressions view of a disjunctive clause should throw");
        }
        catch (AssertionError e)
        {
            assertTrue(e.getMessage().contains("not defined"));
        }
    }

    @Test
    public void testToCQLStringRoundTripsThroughParse() throws Exception
    {
        String[] clauses =
        {
            "a = 1",
            "a = 1 AND b = 2",
            "a = 1 OR b = 2",
            "a = 1 OR b = 2 AND c = 3",
            "(a = 1 AND b = 2) OR c = 3",
            "(a = 1 OR b = 2) AND c = 3",
            "a = 1 OR b = 2 OR c = 3",
            "(a, b) > (1, 2)",
            "a > 1 AND a < 5"
        };
        for (String cql : clauses)
        {
            WhereClause parsed = WhereClause.parse(cql);
            WhereClause reparsed = WhereClause.parse(parsed.toCQLString());
            assertEquals(parsed.toCQLString(), parsed, reparsed);
            assertEquals(parsed.toCQLString(), reparsed.toCQLString());
        }
    }

    @Test
    public void testRenameIdentifier() throws Exception
    {
        WhereClause clause = WhereClause.parse("a = 1 OR (a = 2 AND b = 3)");
        WhereClause renamed = clause.renameIdentifier(new ColumnIdentifier("a", false),
                                                      new ColumnIdentifier("x", false));

        assertEquals(WhereClause.parse("x = 1 OR (x = 2 AND b = 3)"), renamed);
        assertEquals(clause, WhereClause.parse("a = 1 OR (a = 2 AND b = 3)"));
    }

    @Test
    public void testProgrammaticBuilderProducesConjunction()
    {
        WhereClause.Builder builder = new WhereClause.Builder();
        WhereClause clause = builder.build();
        assertFalse(clause.containsDisjunction());
        assertTrue(clause.relations().isEmpty());
        assertTrue(clause.root().children().isEmpty());

        assertEquals(WhereClause.empty(), clause);
    }

    @Test
    public void testMalformedClausesFailCleanly()
    {
        for (String cql : new String[]{ "a >", "a = 1 OR", "OR a = 1", "a = 1 AND", "(a = 1", "()", "a = 1 OR (b = 2" })
        {
            try
            {
                WhereClause.parse(cql);
                fail(cql + " should not parse");
            }
            catch (RecognitionException | SyntaxException e)
            {
                // expected, and specifically not an assertion or a null pointer failure
            }
        }
    }

    @Test
    public void testConjunctiveFormWrapsDisjunctiveRoot() throws Exception
    {
        WhereClause clause = WhereClause.parse("a = 1 OR b = 2");
        assertTrue(clause.root() instanceof AndElement);
        ExpressionElement child = clause.root().children().get(0);
        assertTrue(child instanceof OrElement);
    }
}
