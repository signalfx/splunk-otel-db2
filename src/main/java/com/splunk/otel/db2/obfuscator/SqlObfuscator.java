package com.splunk.otel.db2.obfuscator;

/**
 * Masks sensitive literals in SQL query text before emitting as OTLP logs.
 *
 * <p>All string literals, numeric literals, hex literals, and bind parameters are replaced with
 * {@code ?}. {@code IN (...)} and {@code VALUES (...)} clauses are collapsed to {@code (?)}.
 * Structure (keywords, identifiers, operators) is preserved unchanged.
 *
 * <p>This is a single-pass O(n) character scanner — no external dependencies.
 */
public final class SqlObfuscator {

    private static final SqlObfuscator INSTANCE = new SqlObfuscator();

    private SqlObfuscator() {}

    public static SqlObfuscator getInstance() {
        return INSTANCE;
    }

    /**
     * Returns the query with all literals replaced by {@code ?}.
     * Returns the original string if input is null or blank.
     */
    public String obfuscate(String sql) {
        if (sql == null || sql.isBlank()) return sql;

        StringBuilder out = new StringBuilder(sql.length());
        int i = 0;
        int len = sql.length();

        while (i < len) {
            char c = sql.charAt(i);

            // -- line comment: strip entirely to prevent leaking sensitive data in comments
            if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
                int end = sql.indexOf('\n', i);
                if (end == -1) end = len;
                i = end; // discard comment content
                continue;
            }

            // /* block comment */ (DB2 supports nested block comments): strip entirely
            if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                int end = indexOfBlockCommentEnd(sql, i + 2, 1);
                i = end; // discard comment content
                continue;
            }

            // single-quoted string literal  'value'  ('' escape)
            if (c == '\'') {
                i = skipString(sql, i, '\'');
                out.append('?');
                continue;
            }

            // double-quoted identifier — preserve as-is
            if (c == '"') {
                int end = indexOfClosingQuote(sql, i + 1, '"');
                out.append(sql, i, end);
                i = end;
                continue;
            }

            // ? parameter (DB2 positional)
            if (c == '?') {
                out.append('?');
                i++;
                continue;
            }

            // 0x hex literal
            if (c == '0' && i + 1 < len && (sql.charAt(i + 1) == 'x' || sql.charAt(i + 1) == 'X')) {
                i += 2;
                while (i < len && isHexDigit(sql.charAt(i))) i++;
                out.append('?');
                continue;
            }

            // x'hex' / X'hex'  or  b'bits' / B'bits'
            if ((c == 'x' || c == 'X' || c == 'b' || c == 'B')
                    && i + 1 < len && sql.charAt(i + 1) == '\'') {
                i = skipString(sql, i + 1, '\'');
                out.append('?');
                continue;
            }

            // numeric literal
            if (Character.isDigit(c) || (c == '.' && i + 1 < len && Character.isDigit(sql.charAt(i + 1)))) {
                while (i < len && (Character.isDigit(sql.charAt(i)) || sql.charAt(i) == '.')) i++;
                // optional exponent
                if (i < len && (sql.charAt(i) == 'e' || sql.charAt(i) == 'E')) {
                    i++;
                    if (i < len && (sql.charAt(i) == '+' || sql.charAt(i) == '-')) i++;
                    while (i < len && Character.isDigit(sql.charAt(i))) i++;
                }
                out.append('?');
                continue;
            }

            // everything else — keyword, identifier, operator, whitespace
            out.append(c);
            i++;
        }

        return collapseListClauses(out.toString());
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    /** Skip past the closing quote char, handling '' and \\ escapes. Returns index after closing quote. */
    private static int skipString(String sql, int start, char quote) {
        int i = start + 1; // skip opening quote
        int len = sql.length();
        while (i < len) {
            char c = sql.charAt(i);
            if (c == '\\') { i += 2; continue; }           // backslash escape
            if (c == quote) {
                if (i + 1 < len && sql.charAt(i + 1) == quote) { i += 2; continue; } // '' escape
                return i + 1;
            }
            i++;
        }
        return len;
    }

    /** Returns the index just past the closing {@code *}{@code /} of a block comment, handling nesting. */
    private static int indexOfBlockCommentEnd(String sql, int i, int depth) {
        int len = sql.length();
        while (i < len - 1) {
            if (sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { depth++; i += 2; continue; }
            if (sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
                depth--;
                i += 2;
                if (depth == 0) return i;
                continue;
            }
            i++;
        }
        return len;
    }

    private static int indexOfClosingQuote(String sql, int i, char quote) {
        int len = sql.length();
        while (i < len) {
            if (sql.charAt(i) == quote) return i + 1;
            i++;
        }
        return len;
    }

    private static boolean isHexDigit(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    /**
     * Collapses {@code IN (?, ?, ?)} → {@code IN (?)} and
     * {@code VALUES (?, ?)}, {@code (?, ?)} → {@code VALUES (?)}.
     * Uses a simple regex on the already-masked string so we never touch raw data.
     */
    private static String collapseListClauses(String sql) {
        // collapse multiple ?s inside parens: (?, ?, ?) → (?)
        return sql.replaceAll("\\(\\?(?:\\s*,\\s*\\?)+\\)", "(?)");
    }
}
