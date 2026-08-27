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
    void nationalStringLiteralWithEscapedQuote() {
        // N'O''Brien' contains a '' escape — must produce exactly one ?
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = N'O''Brien'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
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

    @Test
    void unterminatedDoubleQuotedIdentifierWithStringLiteralTailReturnsFailed() {
        // The tail after the opening " is verbatim output and then blanked by
        // containsResidualLiteral — sensitive values would never be flagged without
        // the unterminated-" check in hasMalformedQuoting.
        assertThat(SqlObfuscator.obfuscate("SELECT \"unterminated WHERE secret='password'"))
            .isEqualTo(SqlObfuscator.OBFUSCATION_FAILED);
    }

    @Test
    void unterminatedDoubleQuotedIdentifierWithNumericTailReturnsFailed() {
        assertThat(SqlObfuscator.obfuscate("SELECT \"unterminated WHERE account=123456"))
            .isEqualTo(SqlObfuscator.OBFUSCATION_FAILED);
    }

    @Test
    void doubleQuotedIdentifierInternalWhitespacePreserved() {
        // "Order  History" (two spaces) is a different object from "Order History" (one space)
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM \"Order  History\""))
            .isEqualTo("SELECT * FROM \"Order  History\"");
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

    // --- Unicode identifier boundary in residual check ---

    @Test
    void unicodeIdentifierWithDigitsSuffixNotFlaggedByResidualCheck() {
        // Δ2024 is a Unicode identifier — the residual check must use \p{L}/\p{N}
        // boundaries (matching Character.isLetterOrDigit) so it does not false-positive
        // and return OBFUSCATION_FAILED for this query.
        assertThat(SqlObfuscator.obfuscate("SELECT Δ2024 FROM T WHERE ID = 1"))
            .isEqualTo("SELECT Δ2024 FROM T WHERE ID = ?");
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

    @Test
    void formatCharInsideIdentifierPreserved() {
        // "A\u200BB" and "AB" can name different Db2 objects — format chars inside
        // "..." identifiers must not be stripped
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM \"A\u200BB\""))
            .isEqualTo("SELECT * FROM \"A\u200BB\"");
    }

    @Test
    void formatCharInsideBlockCommentConsumedWithComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT ID /* \u200B */ FROM T"))
            .isEqualTo("SELECT ID FROM T");
    }

    @Test
    void formatCharInsideSingleQuotedStringConsumedWithLiteral() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = 'Ali\u200Bce'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void formatCharBetweenDashesDoesNotBreakLineComment() {
        // -\u200B- must still be recognised as -- line comment, not two minus signs
        assertThat(SqlObfuscator.obfuscate("SELECT ID -\u200B- API_KEY=secret\nFROM T"))
            .isEqualTo("SELECT ID FROM T");
    }

    @Test
    void formatCharInsideIdentifierWithDigitNotObfuscated() {
        // COL\u200B2024 — format char between identifier and digit; digit is part of
        // the identifier, so 2024 must NOT be treated as a standalone numeric literal
        assertThat(SqlObfuscator.obfuscate("SELECT COL\u200B2024 FROM T"))
            .isEqualTo("SELECT COL2024 FROM T");
    }

    @Test
    void formatCharBetweenNumericTokensMergesIntoOneLiteral() {
        // 12\u200B34 — format char in the middle of a numeric literal; the whole token
        // must produce a single ? not two
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 12\u200B34"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void formatCharBetweenMantissaAndExponentE() {
        // 1\u200BE123 — format char before E; must produce single ? not ?E123
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 1\u200BE123"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void formatCharBetweenExponentEAndDigits() {
        // 1E\u200B123 — format char after E; must produce single ? not ?E123
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 1E\u200B123"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void formatCharBetweenExponentDigits() {
        // 1E1\u200B23 — format char in exponent digit run; must produce single ?
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 1E1\u200B23"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void formatCharBetweenEscapedQuotePair() {
        // 'O'\u200B'Brien' — format char between the two apostrophes of '' escape; must be single ?
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = 'O'\u200B'Brien'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void formatCharBetweenEscapedQuotePairInPrefixedLiteral() {
        // N'O'\u200B'Brien' — same as above but inside a prefixed literal
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = N'O'\u200B'Brien'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void formatCharInNestedBlockCommentDelimiter() {
        // /<\u200B>* inside a comment body — format-aware scanner must recognise nested /*
        // and consume the entire outer comment including API_KEY=secret
        assertThat(SqlObfuscator.obfuscate("SELECT ID /* outer /\u200B* inner */ API_KEY=secret */ FROM T"))
            .isEqualTo("SELECT ID FROM T");
    }

    @Test
    void formatCharBetweenPrefixAndQuote() {
        // N\u200B'Alice' — format char between prefix and opening quote; must be single ?
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = N\u200B'Alice'"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void formatCharBetweenTwoCharPrefixChars() {
        // G\u200BX'FF01' — format char inside two-char prefix GX; must be single ?
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE C = G\u200BX'FF01'"))
            .isEqualTo("SELECT * FROM T WHERE C = ?");
    }

    @Test
    void supplementaryPlaneFormatCharStripped() {
        // U+E0020 TAG SPACE (supplementary-plane Cf) is encoded as a surrogate pair in Java.
        // Character.getType(char) on either surrogate returns SURROGATE, not FORMAT — only
        // codepoint-aware iteration detects it. Verify it is stripped so 1<U+E0020>E123
        // is not left as ?E123 (which would re-trigger residual-literal detection as a
        // standalone digit-run).
        String tagSpace = new String(Character.toChars(0xE0020));
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 1" + tagSpace + "E123"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void supplementaryPlaneFormatCharBetweenQuoteAndLiteral() {
        // U+E0020 between opening quote and content must not prevent obfuscation
        String tagSpace = new String(Character.toChars(0xE0020));
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE K = '" + tagSpace + "secret'"))
            .isEqualTo("SELECT * FROM T WHERE K = ?");
    }

    // --- Prefixed literal identifier boundary ---

    @Test
    void prefixLetterAtEndOfIdentifierNotTreatedAsLiteralPrefix() {
        // MAX'abc' — X is part of MAX, not a hex prefix; identifier must be preserved
        assertThat(SqlObfuscator.obfuscate("SELECT MAX'abc' FROM T"))
            .isEqualTo("SELECT MAX? FROM T");
    }

    @Test
    void prefixLetterMidIdentifierNotTreatedAsPrefix() {
        // colN'secret' — N is part of colN, not a national string prefix
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE colN'secret'"))
            .isEqualTo("SELECT * FROM T WHERE colN?");
    }

    // --- Fail-closed safety net ---

    @Test
    void containsResidualLiteralDetectsUnmatchedQuote() {
        // Direct test of the safety-net helper — residual ' triggers true
        assertThat(SqlObfuscator.containsResidualLiteral("SELECT * FROM T WHERE X = bad'input"))
            .isTrue();
    }

    @Test
    void containsResidualLiteralDetectsDigitRun() {
        assertThat(SqlObfuscator.containsResidualLiteral("SELECT * FROM T WHERE X = 12345"))
            .isTrue();
    }

    @Test
    void containsResidualLiteralCleanOutputReturnsFalse() {
        assertThat(SqlObfuscator.containsResidualLiteral("SELECT * FROM T WHERE X = ?"))
            .isFalse();
    }

    @Test
    void obfuscationFailedIsRegressionBackstop() {
        // OBFUSCATION_FAILED is unreachable in practice — the main pass masks every
        // reachable literal, so this path exists only as a regression backstop for future
        // bugs. Direct coverage is in containsResidualLiteral* tests above.
        // This test locks in the sentinel value so a rename or typo is caught.
        assertThat(SqlObfuscator.OBFUSCATION_FAILED).isEqualTo("<obfuscation-failed>");
    }

    @Test
    void doubleQuotedIdentifierWithDigitsDoesNotTriggerFailClosed() {
        // "2024" is a delimited identifier — must be preserved and must NOT
        // trigger the fail-closed digit-run check (identifier body is blanked
        // before the residual scan runs).
        assertThat(SqlObfuscator.obfuscate("SELECT \"2024\" FROM T"))
            .isEqualTo("SELECT \"2024\" FROM T");
    }

    @Test
    void doubleQuotedIdentifierWithApostropheDoesNotTriggerFailClosed() {
        // "it's" as a delimited identifier must not trigger the residual-quote check.
        assertThat(SqlObfuscator.obfuscate("SELECT \"it's\" FROM T"))
            .isEqualTo("SELECT \"it's\" FROM T");
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
            .isEqualTo(SqlObfuscator.OBFUSCATION_FAILED);
    }

    @Test
    void malformedSqlWithOddQuoteCountReturnsFailed() {
        // Three single-quotes outside delimited identifiers = unbalanced → fail-closed
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE A = 'x' AND B = 'y"))
            .isEqualTo(SqlObfuscator.OBFUSCATION_FAILED);
    }

    @Test
    void apostropheInsideLineCommentDoesNotTriggerFailClosed() {
        // -- user's query: the ' is inside a comment, not a string literal
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T -- user's query\nWHERE ID = 1"))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
    }

    @Test
    void apostropheInsideBlockCommentDoesNotTriggerFailClosed() {
        assertThat(SqlObfuscator.obfuscate("SELECT /* it's fine */ * FROM T WHERE ID = 1"))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
    }

    @Test
    void lineCommentTokenInsideSingleQuotedStringNotTreatedAsComment() {
        // 'abc--def' is a valid string literal containing --; must not be treated as a
        // line comment by hasUnbalancedQuotes, which would desync the quote count
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 'abc--def'"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void blockCommentTokenInsideSingleQuotedStringNotTreatedAsComment() {
        // 'abc/*def*/' is a valid string literal containing /* */
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 'abc/*def*/'"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void nestedBlockCommentInHasUnbalancedQuotesCheck() {
        // Nested /* /* */ */ — both depth levels must be consumed without exposing the
        // apostrophe inside the inner comment to the quote counter
        assertThat(SqlObfuscator.obfuscate("SELECT /* outer /* it's nested */ end */ * FROM T WHERE ID = 1"))
            .isEqualTo("SELECT * FROM T WHERE ID = ?");
    }

    @Test
    void doubleQuoteInsideSingleQuotedStringDoesNotFlipIdentifierState() {
        // A " inside a string literal must not cause stripFormatCodepoints to enter
        // inDoubleQuotedId mode — the quote is part of the literal content.
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 'he said \"hi\"'"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void formatCharInsideSingleQuotedStringContainingDoubleQuote() {
        // Combines both: format char inside a string that also contains a "
        // The " must not desync the identifier-tracking state in stripFormatCodepoints.
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X = 'he\u200B said \"hi\"'"))
            .isEqualTo("SELECT * FROM T WHERE X = ?");
    }

    @Test
    void unterminatedBlockComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT /* unterminated"))
            .isEqualTo("SELECT");
    }

    @Test
    void unterminatedHexLiteral() {
        assertThat(SqlObfuscator.obfuscate("WHERE X = X'deadbeef"))
            .isEqualTo(SqlObfuscator.OBFUSCATION_FAILED);
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

            // result is either fully redacted (OBFUSCATION_FAILED fired) or contains
            // no standalone digit-run >= 4 — matching the actual RESIDUAL_LITERAL threshold
            if (!SqlObfuscator.OBFUSCATION_FAILED.equals(result)) {
                assertThat(result)
                    .as("digit-run leaked in: %s → %s", sql, result)
                    .doesNotMatch(".*(?<![\\p{L}\\p{N}_$#@])\\d{4,}(?![\\p{L}\\p{N}_$#@]).*");
            }
        }
    }

    // --- Parameterised: all Db2 prefixed literal variants ---

    @ParameterizedTest
    @ValueSource(strings = {
        "X'FF'", "x'FF'",
        "G'hello'", "g'hello'",
        "N'Alice'", "n'Alice'",
        "UX'0041'", "ux'0041'",
        "U&'\\0041'", "u&'\\0041'",
        "GX'FF01'", "gx'FF01'",
        "BX'10110101'", "bx'10110101'"
    })
    void allPrefixedLiteralVariantsObfuscated(String literal) {
        String sql = "SELECT * FROM T WHERE C = " + literal;
        assertThat(SqlObfuscator.obfuscate(sql))
            .isEqualTo("SELECT * FROM T WHERE C = ?");
    }
}
