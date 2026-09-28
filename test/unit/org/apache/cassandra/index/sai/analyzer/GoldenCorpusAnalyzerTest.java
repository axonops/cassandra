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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.util.Version;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Pins the exact tokenization of the shipped Lucene for every built in analyzer and for two
 * custom configs, as full token at position lists. Analyzed indexes freeze tokens on disk at
 * write time, so any Lucene change that alters tokenization makes queries silently miss rows
 * until the index is rebuilt. These golden corpora turn that silent drift into a test failure.
 * Every language corpus contains at least one stopword of that language, so the position gap it
 * leaves pins the stop set, and at least one word whose emitted form differs from its surface
 * form, so the stemmer or normalizer is pinned too.
 */
public class GoldenCorpusAnalyzerTest
{
    private static final Map<BuiltInAnalyzer, String> INPUTS = new EnumMap<>(BuiltInAnalyzer.class);
    private static final Map<BuiltInAnalyzer, List<String>> EXPECTED = new EnumMap<>(BuiltInAnalyzer.class);

    static
    {
        // Core analyzers. Each input pins the distinguishing behavior: unicode word segmentation
        // with case folding for standard (note "3.14" and "user@host" survive as split tokens),
        // letters only splitting for simple, no transformation at all beyond whitespace splitting
        // for whitespace, the whole value as one untouched token for keyword, english stopword
        // gaps for stop, and the whole value as one lowercased token for lowercase.
        golden(BuiltInAnalyzer.STANDARD, "The Quick, Brown Fox 3.14 user@host",
               "the@0", "quick@1", "brown@2", "fox@3", "3.14@4", "user@5", "host@6");
        golden(BuiltInAnalyzer.SIMPLE, "Foo123Bar Baz Qux9",
               "foo@0", "bar@1", "baz@2", "qux@3");
        golden(BuiltInAnalyzer.WHITESPACE, "Hello,World FOO bar",
               "Hello,World@0", "FOO@1", "bar@2");
        golden(BuiltInAnalyzer.KEYWORD, "The Whole Value 123",
               "The Whole Value 123@0");
        golden(BuiltInAnalyzer.STOP, "The Quick and the Fox",
               "quick@1", "fox@4");
        golden(BuiltInAnalyzer.LOWERCASE, "Hello World 123",
               "hello world 123@0");

        // Language analyzers, in enum order.
        golden(BuiltInAnalyzer.ARABIC, "الكتاب في المكتبة الكبيرة",
               "كتاب@0", "مكتب@2", "كبير@3");
        golden(BuiltInAnalyzer.ARMENIAN, "գիրքը և գրքերը սեղանին են",
               "գիրքը@0", "գրքերը@2", "սեղ@3");
        golden(BuiltInAnalyzer.BASQUE, "etxeak eta etxea handiak dira",
               "etxe@0", "etxea@2", "handi@3");
        golden(BuiltInAnalyzer.BENGALI, "বইগুলি ও বইটি টেবিলে আছে",
               "বই@0", "বই@2", "টেব@3");
        golden(BuiltInAnalyzer.BRAZILIAN, "os livros e o livro na mesa",
               "livr@1", "livr@4", "mes@6");
        golden(BuiltInAnalyzer.BULGARIAN, "книгите и книгата са на масата",
               "книг@0", "книг@2", "мас@5");
        golden(BuiltInAnalyzer.CATALAN, "els llibres i el llibre a la taula",
               "llibr@1", "llib@4", "taul@7");
        // cjk emits overlapping bigrams for han text and its stop set removes english stopwords,
        // hence the leading gap
        golden(BuiltInAnalyzer.CJK, "the 我购买了书籍",
               "我购@1", "购买@2", "买了@3", "了书@4", "书籍@5");
        golden(BuiltInAnalyzer.CZECH, "knihy a kniha jsou na stole",
               "knih@0", "knih@2", "stol@5");
        golden(BuiltInAnalyzer.DANISH, "bøgerne og bogen ligger på bordet",
               "bøg@0", "bog@2", "lig@3", "bord@5");
        golden(BuiltInAnalyzer.DUTCH, "de boeken en het boek op de tafel",
               "boek@1", "boek@4", "tafel@7");
        golden(BuiltInAnalyzer.ENGLISH, "The running foxes jumped",
               "run@1", "fox@2", "jump@3");
        // the estonian stop set does not contain the copula "on", so it survives
        golden(BuiltInAnalyzer.ESTONIAN, "raamatud ja raamat on laual",
               "raama@0", "raama@2", "on@3", "laual@4");
        golden(BuiltInAnalyzer.FINNISH, "kirjat ja kirja ovat pöydällä",
               "kirj@0", "kirj@2", "pöydä@4");
        golden(BuiltInAnalyzer.FRENCH, "les chevaux et le cheval dans la grange",
               "cheval@1", "cheval@4", "grang@7");
        golden(BuiltInAnalyzer.GALICIAN, "os libros e o libro na mesa",
               "libro@1", "libro@4", "mes@6");
        // german folds eszett to double s
        golden(BuiltInAnalyzer.GERMAN, "die Häuser und das Haus sind groß",
               "haus@1", "haus@4", "gross@6");
        golden(BuiltInAnalyzer.GREEK, "τα βιβλία και το βιβλίο στο τραπέζι",
               "βιβλ@1", "βιβλι@4", "τραπεζ@6");
        golden(BuiltInAnalyzer.HINDI, "किताबें और किताब मेज पर हैं",
               "किताब@0", "किताब@2", "मेज@3");
        golden(BuiltInAnalyzer.HUNGARIAN, "a könyvek és a könyv az asztalon",
               "könyv@1", "könyv@4", "asztal@6");
        golden(BuiltInAnalyzer.INDONESIAN, "saya membaca buku dan majalah itu",
               "baca@1", "buku@2", "maja@4");
        // irish strips the eclipsis mutation, mbord becomes bord
        golden(BuiltInAnalyzer.IRISH, "na leabhair agus an leabhar ar an mbord",
               "leabhair@1", "leabhar@4", "bord@7");
        golden(BuiltInAnalyzer.ITALIAN, "i libri e il libro sul tavolo",
               "libri@1", "libro@4", "tavol@6");
        golden(BuiltInAnalyzer.LATVIAN, "grāmatas un grāmata ir uz galda",
               "grāmat@0", "grāmat@2", "gald@5");
        golden(BuiltInAnalyzer.LITHUANIAN, "knygos ir knyga yra ant stalo",
               "knyg@0", "knyg@2", "stal@5");
        golden(BuiltInAnalyzer.NEPALI, "किताबहरू र किताब टेबुलमा छन्",
               "किताब@0", "किताब@2", "टेबुल@3");
        golden(BuiltInAnalyzer.NORWEGIAN, "bøkene og boken ligger på bordet",
               "bøk@0", "bok@2", "ligg@3", "bord@5");
        // persian normalizes the farsi keheh to the arabic kaf, so the emitted bytes differ from
        // the surface form even without a stemmer
        golden(BuiltInAnalyzer.PERSIAN, "کتابها و کتاب روی میز هستند",
               "كتابها@0", "كتاب@2", "ميز@4");
        golden(BuiltInAnalyzer.PORTUGUESE, "os livros e o livro na mesa",
               "livr@1", "livr@4", "mesa@6");
        // the shipped romanian stop set spells its words with cedilla letters, so the comma below
        // form of "si" survives while "sunt" and "pe" leave the gap
        golden(BuiltInAnalyzer.ROMANIAN, "cărțile și cartea sunt pe masă",
               "cărț@0", "și@1", "cart@2", "mas@5");
        golden(BuiltInAnalyzer.RUSSIAN, "книги и книга лежат на столе",
               "книг@0", "книг@2", "лежат@3", "стол@5");
        // the serbian stop set does not contain "na"
        golden(BuiltInAnalyzer.SERBIAN, "knjige i knjiga su na stolu",
               "knjig@0", "knjig@2", "na@4", "stol@5");
        golden(BuiltInAnalyzer.SORANI, "کتێبەکان و کتێبەکە لەسەر مێزەکە",
               "کتێب@0", "کتێب@2", "مێزە@4");
        golden(BuiltInAnalyzer.SPANISH, "los libros y el libro en la mesa",
               "libr@1", "libr@4", "mesa@7");
        golden(BuiltInAnalyzer.SWEDISH, "böckerna och boken ligger på bordet",
               "böck@0", "bok@2", "ligg@3", "bordet@5");
        golden(BuiltInAnalyzer.TAMIL, "புத்தகங்கள் மற்றும் புத்தகம் மேசையில்",
               "புத்தகம்@0", "புத்தகம்@2", "மேசை@3");
        golden(BuiltInAnalyzer.TELUGU, "పుస్తకాలు మరియు పుస్తకం బల్లపై",
               "పుస్తకా@0", "పుస్తకం@2", "బల్ల@3");
        // thai has no stemmer, the corpus pins the dictionary word segmentation and the stop set
        golden(BuiltInAnalyzer.THAI, "หนังสือและแมวอยู่ที่บ้าน",
               "หนังสือ@0", "แมว@2", "บ้าน@5");
        golden(BuiltInAnalyzer.TURKISH, "kitaplar ve kitap masada duruyor",
               "kitap@0", "kitap@2", "masa@3", "duruyor@4");
    }

    /**
     * When this fails after a Lucene bump, every golden corpus below must be re-verified and
     * either re-accepted (updating expectations means existing analyzed indexes need rebuilding,
     * per doc/sai-search.md) or the bump reverted.
     */
    @Test
    public void luceneVersionIsPinned()
    {
        assertEquals("9.12.3", Version.LATEST.toString());
    }

    @Test
    public void everyBuiltInAnalyzerOutputIsPinned()
    {
        for (BuiltInAnalyzer builtIn : BuiltInAnalyzer.values())
        {
            String input = INPUTS.get(builtIn);
            assertNotNull("missing golden corpus for " + builtIn.name(), input);
            assertEquals(builtIn.name(), EXPECTED.get(builtIn), analyze(builtIn.name().toLowerCase(Locale.ROOT), input));
        }
    }

    /**
     * The ngram example from doc/sai-search.md. The tokenizer emits every trigram at its own
     * position and the lowercase filter applies to each.
     */
    @Test
    public void ngramCustomConfigOutputIsPinned()
    {
        String config = "{\"tokenizer\": {\"name\": \"ngram\", \"args\": {\"minGramSize\": \"3\", \"maxGramSize\": \"3\"}}, " +
                        "\"filters\": [{\"name\": \"lowercase\"}]}";
        assertEquals(List.of("fox@0", "ox2@1", "x21@2"), analyze(config, "Fox21"));
    }

    /**
     * A filters only config falls back to the keyword tokenizer, so the whole value comes out as
     * one lowercased token.
     */
    @Test
    public void filtersOnlyCustomConfigOutputIsPinned()
    {
        String config = "{\"filters\": [{\"name\": \"lowercase\"}]}";
        assertEquals(List.of("the whole value 123@0"), analyze(config, "The Whole Value 123"));
    }

    private static void golden(BuiltInAnalyzer analyzer, String input, String... expected)
    {
        INPUTS.put(analyzer, input);
        EXPECTED.put(analyzer, List.of(expected));
    }

    private static List<String> analyze(String config, String text)
    {
        AnalyzerConfig analyzerConfig = AnalyzerConfig.parse(AnalyzerConfig.INDEX_ANALYZER_OPTION, config);
        try (LuceneTextAnalyzer analyzer = new LuceneTextAnalyzer(analyzerConfig))
        {
            List<String> tokens = new ArrayList<>();
            for (AnalyzedToken token : analyzer.analyze(text))
                tokens.add(UTF8Type.instance.compose(token.bytes()) + '@' + token.position());
            return tokens;
        }
    }
}
