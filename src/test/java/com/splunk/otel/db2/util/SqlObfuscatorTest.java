package com.splunk.otel.db2.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class SqlObfuscatorTest {

    // --- Single-quoted strings ---

    @Test
    void singleQuotedString() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = 'Alice'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void escapedQuoteInsideString() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = 'O''Brien'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    // --- Prefixed string literals ---

    @Test
    void hexLiteralUppercase() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE BIN = X'deadbeef'"))
            .isEqualTo("SELECT * FROM T WHERE BIN = ?");
    }

    @Test
    void hexLiteralLowercase() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE BIN = x'FF00'"))
            .isEqualTo("SELECT * FROM T WHERE BIN = ?");
    }

    @Test
    void graphicStringLiteral() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE G_COL = G'hello'"))
            .isEqualTo("SELECT * FROM T WHERE G_COL = ?");
    }

    @Test
    void nationalStringLiteral() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = N'Alice'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void unicodeHexLiteral() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE C = UX'0041'"))
            .isEqualTo("SELECT * FROM T WHERE C = ?");
    }

    @Test
    void unicodeEscapeLiteral() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE C = U&'\\0041'"))
            .isEqualTo("SELECT * FROM T WHERE C = ?");
    }

    @Test
    void prefixedLiteralLowercaseUx() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE C = ux'0041'"))
            .isEqualTo("SELECT * FROM T WHERE C = ?");
    }

    // --- Numeric literals ---

    @Test
    void numericIntegerLiteral() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE AGE > 30"))
            .isEqualTo("SELECT * FROM T WHERE AGE > ?");
    }

    @Test
    void numericDecimalLiteral() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE SALARY > 12345.67"))
            .isEqualTo("SELECT * FROM T WHERE SALARY > ?");
    }

    @Test
    void scientificNotationInteger() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 15E1"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void scientificNotationDecimal() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 6.022E23"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void scientificNotationNegativeExponent() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE THRESH = 2.0E-3"))
            .isEqualTo("SELECT * FROM T WHERE THRESH = ?");
    }

    @Test
    void scientificNotationPositiveExponent() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 1.5E+10"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    // --- Identifier boundary: digits inside identifiers must NOT be replaced ---

    @Test
    void tableNameWithNumberNotObfuscated() {
        assertThat(SqlObfuscator.obfuscate("SELECT T1.A FROM T1 JOIN T2 ON T1.ID = T2.ID"))
            .isEqualTo("SELECT T1.A FROM T1 JOIN T2 ON T1.ID = T2.ID");
    }

    @Test
    void underscoreDigitIdentifierNotObfuscated() {
        assertThat(SqlObfuscator.obfuscate("SELECT COL_1 FROM T"))
            .isEqualTo("SELECT COL_1 FROM T");
    }

    @Test
    void underscoreDigitIdentifierWithValueObfuscated() {
        assertThat(SqlObfuscator.obfuscate("UPDATE T SET VAR_10 = 42"))
            .isEqualTo("UPDATE T SET VAR_10 = ?");
    }

    @Test
    void tableQualifiedColumnWithUnderscoreNotObfuscated() {
        assertThat(SqlObfuscator.obfuscate("SELECT T.COL_1 FROM T"))
            .isEqualTo("SELECT T.COL_1 FROM T");
    }

    // --- Double-quoted delimited identifiers — must be preserved ---

    @Test
    void doubleQuotedIdentifierPreserved() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM \"MySchema\".\"MyTable\""))
            .isEqualTo("SELECT * FROM \"MySchema\".\"MyTable\"");
    }

    @Test
    void doubleQuotedIdentifierWithEscapedQuote() {
        assertThat(SqlObfuscator.obfuscate("SELECT \"O\"\"Brien\" FROM T"))
            .isEqualTo("SELECT \"O\"\"Brien\" FROM T");
    }

    // --- Comments ---

    @Test
    void lineComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T -- filter\nWHERE NAME = 'Bob'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void lineCommentCarriageReturn() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T -- filter\rWHERE NAME = 'Bob'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void lineCommentAdjacentTokensGetSpace() {
        assertThat(SqlObfuscator.obfuscate("SELECT col--comment\nFROM T"))
            .isEqualTo("SELECT col FROM T");
    }

    @Test
    void blockComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT /* top 10 */ * FROM T WHERE ID = 1"))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
    }

    @Test
    void nestedBlockComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT /* outer /* inner API_KEY=secret */ end */ * FROM T"))
            .isEqualTo("SELECT * FROM T");
    }

    @Test
    void blockCommentAdjacentTokensGetSpace() {
        assertThat(SqlObfuscator.obfuscate("SELECT col/*comment*/FROM T"))
            .isEqualTo("SELECT col FROM T");
    }

    // --- Whitespace normalisation ---

    @Test
    void multipleSpacesCollapsed() {
        assertThat(SqlObfuscator.obfuscate("SELECT  *   FROM   T"))
            .isEqualTo("SELECT * FROM T");
    }

    @Test
    void tabsAndNewlinesCollapsed() {
        assertThat(SqlObfuscator.obfuscate("SELECT\t*\nFROM\tT"))
            .isEqualTo("SELECT * FROM T");
    }

    // --- Unicode format character stripping ---

    @Test
    void zeroWidthSpaceStripped() {
        // U+200B zero-width space — stripped, real space preserved for token separation
        assertThat(SqlObfuscator.obfuscate("SELECT \u200B* FROM T WHERE ID = 1"))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
    }

    @Test
    void softHyphenStripped() {
        // U+00AD soft hyphen
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = 'Ali\u00ADce'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    // --- Fail-closed safety net ---

    @Test
    void failClosedConstantIsDefinedAndNonEmpty() {
        // OBFUSCATION_FAILED is the sentinel returned when a residual literal
        // survives the main pass (defensive backstop against future bugs).
        assertThat(SqlObfuscator.OBFUSCATION_FAILED).isNotEmpty();
    }

    @Test
    void obfuscatedOutputContainsNoSingleQuotes() {
        String result = SqlObfuscator.obfuscate("SELECT * FROM T WHERE A='x' AND B='y'");
        assertThat(result).doesNotContain("'");
    }

    // --- Mixed ---

    @Test
    void multipleStringAndNumericLiterals() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = 'Alice' AND AGE > 30"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ? AND AGE > ?");
    }

    @Test
    void insertStatement() {
        assertThat(SqlObfuscator.obfuscate("INSERT INTO EMPLOYEE (NAME, AGE) VALUES ('Carol', 29)"))
            .isEqualTo("INSERT INTO EMPLOYEE (NAME, AGE) VALUES (?, ?)");
    }

    @Test
    void updateStatement() {
        assertThat(SqlObfuscator.obfuscate("UPDATE EMPLOYEE SET SALARY = 75000.00 WHERE EMP_ID = 42"))
            .isEqualTo("UPDATE EMPLOYEE SET SALARY = ? WHERE EMP_ID = ?");
    }

    @Test
    void nullAndKeywordsUnchanged() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X IS NULL"))
            .isEqualTo("SELECT * FROM T WHERE X IS NULL");
    }

    // --- Db2-specific patterns ---

    @Test
    void db2FetchFirstRowsOnly() {
        assertThat(SqlObfuscator.obfuscate(
                "SELECT * FROM T ORDER BY EXEC_TIME DESC FETCH FIRST 10 ROWS ONLY"))
            .isEqualTo("SELECT * FROM T ORDER BY EXEC_TIME DESC FETCH FIRST ? ROWS ONLY");
    }

    @Test
    void db2InListObfuscated() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE ID IN (1, 2, 3)"))
            .isEqualTo("SELECT * FROM T WHERE ID IN (?, ?, ?)");
    }

    // --- Edge / crash safety ---

    @Test
    void unterminatedSingleQuotedString() {
        // unterminated string is consumed to end and replaced with ? — no data leak
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = 'unterminated"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void unterminatedBlockComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT /* unterminated"))
            .isEqualTo("SELECT");
    }

    @Test
    void unterminatedHexLiteral() {
        assertThat(SqlObfuscator.obfuscate("WHERE X = X'deadbeef"))
            .isEqualTo("WHERE X = ?");
    }

    @Test
    void nullInputReturnsNull() {
        assertThat(SqlObfuscator.obfuscate(null)).isNull();
    }

    @Test
    void emptyInputReturnsEmpty() {
        assertThat(SqlObfuscator.obfuscate("")).isEmpty();
    }

    @Test
    void whitespaceOnlyInput() {
        assertThat(SqlObfuscator.obfuscate("   ")).isEmpty();
    }

    // --- Property / fuzz test ---

    @Test
    void fuzzOutputNeverContainsRawLiteral() {
        // Invariant: obfuscated output must never contain a single-quote or
        // a standalone digit-run ≥ 4 (i.e. no PII numeric value survived)
        String[] templates = {
            "SELECT * FROM T WHERE NAME = '%s' AND AGE > %d",
            "INSERT INTO T VALUES ('%s', %d, '%s')",
            "UPDATE T SET X = '%s' WHERE ID = %d",
            "SELECT * FROM T WHERE BIN = X'%s' AND VAL = %d",
        };
        String[] strings = {"Alice", "O'Brien", "hello world", "test'value", "secret123"};
        Random rng = new Random(42);

        for (int iter = 0; iter < 500; iter++) {
            String template = templates[rng.nextInt(templates.length)];
            String s1 = strings[rng.nextInt(strings.length)];
            String s2 = strings[rng.nextInt(strings.length)];
            int n = rng.nextInt(100000);
            String sql = String.format(template, s1, n, s2);

            String result = SqlObfuscator.obfuscate(sql);

            assertThat(result)
                .as("obfuscated output for: %s", sql)
                .doesNotContain("'");

            // result is either fully redacted or contains no raw 5+-digit run
            if (!SqlObfuscator.OBFUSCATION_FAILED.equals(result)) {
                assertThat(result)
                    .as("digit-run leaked in: %s → %s", sql, result)
                    .doesNotMatch(".*(?<![A-Za-z0-9_$#@])\\d{5,}(?![A-Za-z0-9_$#@]).*");
            }
        }
    }

    // --- Parameterised: all Db2 prefixed literal variants ---

    @ParameterizedTest
    @ValueSource(strings = {"X'FF'", "x'FF'", "G'hello'", "g'hello'", "N'Alice'", "n'Alice'",
                            "UX'0041'", "ux'0041'", "U&'\\0041'", "u&'\\0041'"})
    void allPrefixedLiteralVariantsObfuscated(String literal) {
        String sql = "SELECT * FROM T WHERE C = " + literal;
        assertThat(SqlObfuscator.obfuscate(sql))
            .isEqualTo("SELECT * FROM T WHERE C = ?");
    }
}
