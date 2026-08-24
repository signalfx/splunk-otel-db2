package com.splunk.otel.db2.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SqlObfuscatorTest {

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
    void multipleStringAndNumericLiterals() {
        assertThat(SqlObfuscator.obfuscate(
                "SELECT * FROM T WHERE NAME = 'Alice' AND AGE > 30"))
            .isEqualTo("SELECT * FROM T WHERE NAME = ? AND AGE > ?");
    }

    @Test
    void hexLiteral() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE BIN = X'deadbeef'"))
            .isEqualTo("SELECT * FROM T WHERE BIN = ?");
    }

    @Test
    void hexLiteralLowercase() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE BIN = x'FF00'"))
            .isEqualTo("SELECT * FROM T WHERE BIN = ?");
    }

    @Test
    void doubleQuotedValue() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE NAME = \"Alice\""))
            .isEqualTo("SELECT * FROM T WHERE NAME = ?");
    }

    @Test
    void lineComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T -- filter by name\nWHERE NAME = 'Bob'"))
            .isEqualTo("SELECT * FROM T \nWHERE NAME = ?");
    }

    @Test
    void blockComment() {
        assertThat(SqlObfuscator.obfuscate("SELECT /* top 10 */ * FROM T WHERE ID = 1"))
            .isEqualTo("SELECT  * FROM T WHERE ID = ?");
    }

    @Test
    void insertStatement() {
        assertThat(SqlObfuscator.obfuscate(
                "INSERT INTO EMPLOYEE (NAME, AGE) VALUES ('Carol', 29)"))
            .isEqualTo("INSERT INTO EMPLOYEE (NAME, AGE) VALUES (?, ?)");
    }

    @Test
    void updateStatement() {
        assertThat(SqlObfuscator.obfuscate(
                "UPDATE EMPLOYEE SET SALARY = 75000.00 WHERE EMP_ID = 42"))
            .isEqualTo("UPDATE EMPLOYEE SET SALARY = ? WHERE EMP_ID = ?");
    }

    @Test
    void nullAndKeywordsUnchanged() {
        assertThat(SqlObfuscator.obfuscate("SELECT * FROM T WHERE X IS NULL"))
            .isEqualTo("SELECT * FROM T WHERE X IS NULL");
    }

    @Test
    void tableNameWithNumberNotObfuscated() {
        // T1 and T2 are identifiers — digits follow a letter so should not be replaced
        assertThat(SqlObfuscator.obfuscate("SELECT T1.A FROM T1 JOIN T2 ON T1.ID = T2.ID"))
            .isEqualTo("SELECT T1.A FROM T1 JOIN T2 ON T1.ID = T2.ID");
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
}
