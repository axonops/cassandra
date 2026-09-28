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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.AddIdentityStatement;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.DropIdentityStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateFunctionStatement;
import org.apache.cassandra.cql3.statements.schema.CreateKeyspaceStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.service.ClientState;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * PHRASE and MATCH are unreserved keywords. Statements that take an unquoted name accept phrase and
 * match as that name, and the PHRASE and MATCH operators parse in WHERE clauses only.
 */
public class PhraseKeywordTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static CQLStatement.Raw parse(String query)
    {
        return QueryProcessor.parseStatement(query);
    }

    private static void assertSyntaxError(String query)
    {
        assertThatThrownBy(() -> parse(query)).isInstanceOf(SyntaxException.class);
    }

    @Test
    public void testPhraseAsUserName()
    {
        for (String name : new String[]{ "phrase", "PHRASE", "Phrase" })
        {
            assertTrue(parse("CREATE USER " + name) instanceof CreateRoleStatement);
            assertTrue(parse("CREATE USER IF NOT EXISTS " + name + " WITH PASSWORD 'secret' NOSUPERUSER") instanceof CreateRoleStatement);
            assertTrue(parse("ALTER USER " + name + " WITH PASSWORD 'secret'") instanceof AlterRoleStatement);
            assertTrue(parse("DROP USER " + name) instanceof DropRoleStatement);
            assertTrue(parse("DROP USER IF EXISTS " + name) instanceof DropRoleStatement);
        }
    }

    @Test
    public void testPhraseAsRoleName()
    {
        assertTrue(parse("CREATE ROLE phrase") instanceof CreateRoleStatement);
        assertTrue(parse("ALTER ROLE phrase WITH LOGIN = true") instanceof AlterRoleStatement);
        assertTrue(parse("DROP ROLE phrase") instanceof DropRoleStatement);
    }

    @Test
    public void testPhraseAsIdentity()
    {
        assertTrue(parse("ADD IDENTITY phrase TO ROLE reader") instanceof AddIdentityStatement);
        assertTrue(parse("ADD IDENTITY IF NOT EXISTS reader TO ROLE phrase") instanceof AddIdentityStatement);
        assertTrue(parse("ADD IDENTITY phrase TO ROLE phrase") instanceof AddIdentityStatement);
        assertTrue(parse("DROP IDENTITY phrase") instanceof DropIdentityStatement);
        assertTrue(parse("DROP IDENTITY IF EXISTS phrase") instanceof DropIdentityStatement);
    }

    @Test
    public void testPhraseAsFunctionLanguage()
    {
        String create = "CREATE FUNCTION ks.f (a int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE %s AS 'return a;'";
        for (String language : new String[]{ "phrase", "PHRASE", "lua" })
        {
            CreateFunctionStatement statement = ((CreateFunctionStatement.Raw) parse(String.format(create, language))).prepare(ClientState.forInternalCalls());
            assertThatThrownBy(() -> statement.apply(Keyspaces.none()))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageStartingWith("Currently only Java UDFs are available in Cassandra.");
        }
    }

    @Test
    public void testPhraseAsSchemaNames()
    {
        assertTrue(parse("CREATE KEYSPACE phrase WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}") instanceof CreateKeyspaceStatement.Raw);
        assertTrue(parse("CREATE TABLE phrase.phrase (phrase int PRIMARY KEY, v text)") instanceof CreateTableStatement.Raw);
        assertTrue(parse("CREATE TYPE phrase.phrase (phrase int)") instanceof CreateTypeStatement.Raw);
        assertTrue(parse("SELECT phrase FROM phrase.phrase WHERE phrase = 1") instanceof SelectStatement.RawStatement);
    }

    @Test
    public void testPhraseOperator() throws Exception
    {
        WhereClause clause = WhereClause.parse("body PHRASE 'quick fox'");
        assertEquals(1, clause.relations().size());
        assertEquals(Operator.PHRASE, clause.relations().get(0).operator());

        clause = WhereClause.parse("phrase PHRASE 'quick fox' AND phrase = 'x'");
        assertEquals(2, clause.relations().size());
        assertEquals(Operator.PHRASE, clause.relations().get(0).operator());
        assertEquals(Operator.EQ, clause.relations().get(1).operator());
    }

    @Test
    public void testMatchAsUserName()
    {
        for (String name : new String[]{ "match", "MATCH", "Match" })
        {
            assertTrue(parse("CREATE USER " + name) instanceof CreateRoleStatement);
            assertTrue(parse("CREATE USER IF NOT EXISTS " + name + " WITH PASSWORD 'secret' NOSUPERUSER") instanceof CreateRoleStatement);
            assertTrue(parse("ALTER USER " + name + " WITH PASSWORD 'secret'") instanceof AlterRoleStatement);
            assertTrue(parse("DROP USER " + name) instanceof DropRoleStatement);
            assertTrue(parse("DROP USER IF EXISTS " + name) instanceof DropRoleStatement);
        }
    }

    @Test
    public void testMatchAsRoleName()
    {
        assertTrue(parse("CREATE ROLE match") instanceof CreateRoleStatement);
        assertTrue(parse("ALTER ROLE match WITH LOGIN = true") instanceof AlterRoleStatement);
        assertTrue(parse("DROP ROLE match") instanceof DropRoleStatement);
    }

    @Test
    public void testMatchAsIdentity()
    {
        assertTrue(parse("ADD IDENTITY match TO ROLE reader") instanceof AddIdentityStatement);
        assertTrue(parse("ADD IDENTITY IF NOT EXISTS reader TO ROLE match") instanceof AddIdentityStatement);
        assertTrue(parse("ADD IDENTITY match TO ROLE match") instanceof AddIdentityStatement);
        assertTrue(parse("DROP IDENTITY match") instanceof DropIdentityStatement);
        assertTrue(parse("DROP IDENTITY IF EXISTS match") instanceof DropIdentityStatement);
    }

    @Test
    public void testMatchAsFunctionLanguage()
    {
        String create = "CREATE FUNCTION ks.f (a int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE %s AS 'return a;'";
        for (String language : new String[]{ "match", "MATCH" })
        {
            CreateFunctionStatement statement = ((CreateFunctionStatement.Raw) parse(String.format(create, language))).prepare(ClientState.forInternalCalls());
            assertThatThrownBy(() -> statement.apply(Keyspaces.none()))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageStartingWith("Currently only Java UDFs are available in Cassandra.");
        }
    }

    @Test
    public void testMatchAsSchemaNames()
    {
        assertTrue(parse("CREATE KEYSPACE match WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}") instanceof CreateKeyspaceStatement.Raw);
        assertTrue(parse("CREATE TABLE match.match (match int PRIMARY KEY, v text)") instanceof CreateTableStatement.Raw);
        assertTrue(parse("CREATE TYPE match.match (match int)") instanceof CreateTypeStatement.Raw);
        assertTrue(parse("SELECT match FROM match.match WHERE match = 1") instanceof SelectStatement.RawStatement);
    }

    @Test
    public void testMatchOperator() throws Exception
    {
        assertEquals("MATCH", Operator.ANALYZER_MATCHES.toString());

        WhereClause clause = WhereClause.parse("body MATCH 'quick fox'");
        assertEquals(1, clause.relations().size());
        assertEquals(Operator.ANALYZER_MATCHES, clause.relations().get(0).operator());
        assertEquals("body MATCH 'quick fox'", clause.toCQLString());

        clause = WhereClause.parse("match MATCH 'quick fox' AND match = 'x'");
        assertEquals(2, clause.relations().size());
        assertEquals(Operator.ANALYZER_MATCHES, clause.relations().get(0).operator());
        assertEquals(Operator.EQ, clause.relations().get(1).operator());

        assertTrue(parse("SELECT * FROM ks.t WHERE match MATCH 'x'") instanceof SelectStatement.RawStatement);
    }

    @Test
    public void testColonIsNotAnOperator()
    {
        assertSyntaxError("SELECT * FROM ks.t WHERE body : 'x'");
        assertSyntaxError("SELECT * FROM ks.t WHERE body : ?");
        assertSyntaxError("SELECT * FROM ks.t WHERE token(k) : 'x'");
        assertSyntaxError("SELECT * FROM ks.t WHERE m['k'] : 'x'");
    }

    @Test
    public void testAnalyzedOperatorsAreNotConditions()
    {
        assertSyntaxError("UPDATE ks.t SET v = 'a' WHERE k = 0 IF body MATCH 'x'");
        assertSyntaxError("DELETE FROM ks.t WHERE k = 0 IF body MATCH 'x'");
        assertSyntaxError("UPDATE ks.t SET v = 'a' WHERE k = 0 IF body PHRASE 'x'");
        assertSyntaxError("UPDATE ks.t SET v = 'a' WHERE k = 0 IF body : 'x'");
    }

    @Test
    public void testColonKeepsItsOtherUses()
    {
        // named bind markers
        assertTrue(parse("SELECT * FROM ks.t WHERE a = :name") instanceof SelectStatement.RawStatement);
        assertTrue(parse("SELECT * FROM ks.t WHERE a = :match") instanceof SelectStatement.RawStatement);
        assertTrue(parse("SELECT * FROM ks.t WHERE a = :phrase") instanceof SelectStatement.RawStatement);
        assertTrue(parse("SELECT * FROM ks.t WHERE a IN :ks") instanceof SelectStatement.RawStatement);
        assertTrue(parse("SELECT * FROM ks.t WHERE (a, b) = (:x, :y)") instanceof SelectStatement.RawStatement);
        assertTrue(parse("SELECT * FROM ks.t WHERE (a, b) = :t") instanceof SelectStatement.RawStatement);
        assertTrue(parse("SELECT * FROM ks.t WHERE (a, b) IN :ts") instanceof SelectStatement.RawStatement);
        assertTrue(parse("SELECT * FROM ks.t LIMIT :l") instanceof SelectStatement.RawStatement);
        assertTrue(parse("UPDATE ks.t SET v = 1 WHERE k = 0 IF v = :x") instanceof ModificationStatement.Parsed);
        assertTrue(parse("UPDATE ks.t SET v = 1 WHERE k = 0 IF v IN :x") instanceof ModificationStatement.Parsed);
        assertTrue(parse("INSERT INTO ks.t (k, v) VALUES (0, 1) USING TTL :t") instanceof ModificationStatement.Parsed);
        assertTrue(parse("INSERT INTO ks.t JSON :j") instanceof ModificationStatement.Parsed);

        // map and user type literals
        assertTrue(parse("UPDATE ks.t SET m = {'k': 'v'} WHERE k = 0") instanceof ModificationStatement.Parsed);
        assertTrue(parse("UPDATE ks.t SET u = {f: 1} WHERE k = 0") instanceof ModificationStatement.Parsed);
        assertTrue(parse("SELECT {'k': v} FROM ks.t") instanceof SelectStatement.RawStatement);
    }
}
