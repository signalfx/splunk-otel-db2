package com.splunk.otel.db2.util;

/**
 * Replaces SQL literal values with {@code ?} so query text can be safely
 * emitted as telemetry without leaking PII or sensitive data.
 *
 * Handled:
 *   single-quoted strings  'O''Brien'  →  ?
 *   double-quoted strings  "value"     →  ?   (Db2 non-identifier use)
 *   numeric literals       42, 3.14    →  ?
 *   hex literals           X'deadbeef' →  ?
 *   line comments          -- …        →  stripped
 *   block comments         /* … * /    →  stripped
 *
 * Identifiers in double-quotes that appear in object-name position are also
 * replaced with ?, which is safe — the sanitized SQL is for display/grouping,
 * not re-execution.
 */
public final class SqlObfuscator {

    private SqlObfuscator() {}

    public static String obfuscate(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        StringBuilder out = new StringBuilder(sql.length());
        int i = 0;
        int len = sql.length();

        while (i < len) {
            char c = sql.charAt(i);

            // Line comment: -- ... newline
            if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
                i += 2;
                while (i < len && sql.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }

            // Block comment: /* ... */
            if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                i += 2;
                while (i + 1 < len && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
                    i++;
                }
                i += 2; // consume */
                continue;
            }

            // Hex literal: X'...' or x'...'
            if ((c == 'X' || c == 'x') && i + 1 < len && sql.charAt(i + 1) == '\'') {
                i += 2;
                while (i < len && sql.charAt(i) != '\'') {
                    i++;
                }
                i++; // consume closing '
                out.append('?');
                continue;
            }

            // Single-quoted string with '' escape: 'O''Brien'
            if (c == '\'') {
                i++;
                while (i < len) {
                    if (sql.charAt(i) == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') {
                        i += 2; // escaped quote — skip both
                    } else if (sql.charAt(i) == '\'') {
                        i++; // closing quote
                        break;
                    } else {
                        i++;
                    }
                }
                out.append('?');
                continue;
            }

            // Double-quoted string/identifier
            if (c == '"') {
                i++;
                while (i < len && sql.charAt(i) != '"') {
                    i++;
                }
                i++; // consume closing "
                out.append('?');
                continue;
            }

            // Numeric literal (integer or decimal, not inside an identifier)
            if (Character.isDigit(c) && (i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1)))) {
                while (i < len && (Character.isDigit(sql.charAt(i)) || sql.charAt(i) == '.')) {
                    i++;
                }
                out.append('?');
                continue;
            }

            out.append(c);
            i++;
        }

        return out.toString().trim();
    }
}
