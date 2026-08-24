package com.splunk.otel.db2.util;

import org.junit.jupiter.api.Test;

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

    // --- Identifier characters: digits inside identifiers must NOT be replaced ---

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

    // --- Hex literals ---

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

    // --- Double-quoted delimited identifiers — must be preserved ---

    @Test
    void doubleQuotedIdentifierPreserved() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM \"MySchema\".\"MyTable\""))
            .isEqualTo("SELECT * FROM \"MySchema\".\"MyTable\"");
    }

    @Test
    void doubleQuotedIdentifierWithEscapedQuote() {
        // "" inside a delimited identifier is an escaped double-quote
        assertThat(SqlObfuscator.obfuscate("SELECT \"O\"\"Brien\" FROM T"))
            .isEqualTo("SELECT \"O\"\"Brien\" FROM T");
    }

    // --- Comments ---

    @Test
    void lineComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T -- filter\nWHERE NAME = 'Bob'"))
            .isEqualTo("SELECT * FROM T \nWHERE NAME = ?");  // trailing space before -- preserved; newline acts as separator
    }

    @Test
    void blockComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT /* top 10 */ * FROM T WHERE ID = 1"))
            .isEqualTo("SELECT   * FROM T WHERE ID = ?");
    }

    @Test
    void nestedBlockComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT /* outer /* inner API_KEY=secret */ end */ * FROM T"))
            .isEqualTo("SELECT   * FROM T");
    }

    @Test
    void blockCommentAdjacentTokensGetSpace() {
        // col/*comment*/FROM must not become colFROM
        assertThat(SqlObfuscator.obfuscate("SELECT col/*comment*/FROM T"))
            .isEqualTo("SELECT col FROM T");
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
        // must not throw; literals consumed to end of input
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = 'unterminated"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void unterminatedBlockComment() {
        // must not throw; comment consumed to end of input
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
}
