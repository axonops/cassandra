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

package org.apache.cassandra.index.sai.analyzer;

import java.io.IOException;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.et.EstonianAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.ne.NepaliAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sr.SerbianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.ta.TamilAnalyzer;
import org.apache.lucene.analysis.te.TeluguAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;

/**
 * Registry of analyzer names accepted as the whole value of the {@code index_analyzer} and
 * {@code query_analyzer} index options. Names match case insensitively. Every call to
 * {@link #newAnalyzer()} returns a new Lucene {@link Analyzer}, so no state is ever shared
 * through this registry. Only analyzers shipped in lucene-core and lucene-analysis-common
 * are listed here.
 */
public enum BuiltInAnalyzer
{
    STANDARD(StandardAnalyzer::new),
    SIMPLE(SimpleAnalyzer::new),
    WHITESPACE(WhitespaceAnalyzer::new),
    KEYWORD(KeywordAnalyzer::new),
    STOP(() -> new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET)),
    LOWERCASE(BuiltInAnalyzer::lowercaseAnalyzer),
    ARABIC(ArabicAnalyzer::new),
    ARMENIAN(ArmenianAnalyzer::new),
    BASQUE(BasqueAnalyzer::new),
    BENGALI(BengaliAnalyzer::new),
    BRAZILIAN(BrazilianAnalyzer::new),
    BULGARIAN(BulgarianAnalyzer::new),
    CATALAN(CatalanAnalyzer::new),
    CJK(CJKAnalyzer::new),
    CZECH(CzechAnalyzer::new),
    DANISH(DanishAnalyzer::new),
    DUTCH(DutchAnalyzer::new),
    ENGLISH(EnglishAnalyzer::new),
    ESTONIAN(EstonianAnalyzer::new),
    FINNISH(FinnishAnalyzer::new),
    FRENCH(FrenchAnalyzer::new),
    GALICIAN(GalicianAnalyzer::new),
    GERMAN(GermanAnalyzer::new),
    GREEK(GreekAnalyzer::new),
    HINDI(HindiAnalyzer::new),
    HUNGARIAN(HungarianAnalyzer::new),
    INDONESIAN(IndonesianAnalyzer::new),
    IRISH(IrishAnalyzer::new),
    ITALIAN(ItalianAnalyzer::new),
    LATVIAN(LatvianAnalyzer::new),
    LITHUANIAN(LithuanianAnalyzer::new),
    NEPALI(NepaliAnalyzer::new),
    NORWEGIAN(NorwegianAnalyzer::new),
    PERSIAN(PersianAnalyzer::new),
    PORTUGUESE(PortugueseAnalyzer::new),
    ROMANIAN(RomanianAnalyzer::new),
    RUSSIAN(RussianAnalyzer::new),
    SERBIAN(SerbianAnalyzer::new),
    SORANI(SoraniAnalyzer::new),
    SPANISH(SpanishAnalyzer::new),
    SWEDISH(SwedishAnalyzer::new),
    TAMIL(TamilAnalyzer::new),
    TELUGU(TeluguAnalyzer::new),
    THAI(ThaiAnalyzer::new),
    TURKISH(TurkishAnalyzer::new);

    private final Supplier<Analyzer> factory;

    BuiltInAnalyzer(Supplier<Analyzer> factory)
    {
        this.factory = factory;
    }

    public Analyzer newAnalyzer()
    {
        return factory.get();
    }

    /**
     * @return the built-in analyzer with the given name, matched case insensitively, or null when
     * no built-in analyzer has that name
     */
    public static BuiltInAnalyzer fromName(String name)
    {
        for (BuiltInAnalyzer analyzer : values())
        {
            if (analyzer.name().equalsIgnoreCase(name))
                return analyzer;
        }
        return null;
    }

    static String validNames()
    {
        return Stream.of(values())
                     .map(analyzer -> analyzer.name().toLowerCase(Locale.ROOT))
                     .collect(Collectors.joining(", "));
    }

    /**
     * No Lucene analyzer class emits the whole value as a single lowercased token, so the common
     * expectation of a case insensitive keyword analyzer is assembled from parts.
     */
    private static Analyzer lowercaseAnalyzer()
    {
        try
        {
            return CustomAnalyzer.builder()
                                 .withTokenizer("keyword")
                                 .addTokenFilter("lowercase")
                                 .build();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
