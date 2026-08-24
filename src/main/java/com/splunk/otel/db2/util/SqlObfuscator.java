package com.splunk.otel.db2.util;

/**
 * Replaces SQL literal values with {@code ?} so query text can be safely
 * emitted as telemetry without leaking PII or sensitive data.
 *
 * Handled:
 *   single-quoted strings  'O''Brien'    →  ?
 *   numeric literals       42, 3.14,
 *                          1.5E10, 2E-3  →  ?
 *   hex literals           X'deadbeef'   →  ?
 *   line comments          -- …          →  stripped (replaced with space)
 *   block comments         /* … * /      →  stripped, including nested (replaced with space)
 *
 * Preserved (not replaced):
 *   double-quoted identifiers  "MyTable"  — Db2 LUW uses double quotes for
 *   delimited identifiers, not string literals; replacing them would collapse
 *   queries against different objects into the same fingerprint.
 *
 * The sanitized SQL is intended for display and grouping, not re-execution.
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
                out.append(' ');
                continue;
            }

            // Block comment: /* ... */ with nesting support
            if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                i += 2;
                int depth = 1;
                while (i + 1 < len && depth > 0) {
                    if (sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
                        depth++;
                        i += 2;
                    } else if (sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
                        depth--;
                        i += 2;
                    } else {
                        i++;
                    }
                }
                if (depth > 0) {
                    i = len; // unterminated — consume to end
                }
                out.append(' ');
                continue;
            }

            // Hex literal: X'...' or x'...'
            if ((c == 'X' || c == 'x') && i + 1 < len && sql.charAt(i + 1) == '\'') {
                i += 2;
                while (i < len && sql.charAt(i) != '\'') {
                    i++;
                }
                if (i < len) i++; // consume closing '
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

            // Double-quoted delimited identifier — preserve as-is (Db2 LUW semantics)
            if (c == '"') {
                out.append(c);
                i++;
                while (i < len) {
                    char q = sql.charAt(i);
                    out.append(q);
                    i++;
                    if (q == '"') {
                        // handle "" escape inside identifier
                        if (i < len && sql.charAt(i) == '"') {
                            out.append('"');
                            i++;
                        } else {
                            break; // closing quote
                        }
                    }
                }
                continue;
            }

            // Numeric literal — only when preceded by a non-identifier character
            // Identifier characters in Db2: letters, digits, underscore, $, #, @
            if (Character.isDigit(c)) {
                char prev = (i == 0) ? ' ' : sql.charAt(i - 1);
                boolean insideIdentifier = Character.isLetterOrDigit(prev)
                        || prev == '_' || prev == '$' || prev == '#' || prev == '@';
                if (!insideIdentifier) {
                    // consume mantissa: digits and at most one dot
                    while (i < len && (Character.isDigit(sql.charAt(i)) || sql.charAt(i) == '.')) {
                        i++;
                    }
                    // consume optional exponent: [eE][+-]?[0-9]+
                    if (i < len && (sql.charAt(i) == 'e' || sql.charAt(i) == 'E')) {
                        int saved = i;
                        i++;
                        if (i < len && (sql.charAt(i) == '+' || sql.charAt(i) == '-')) {
                            i++;
                        }
                        if (i < len && Character.isDigit(sql.charAt(i))) {
                            while (i < len && Character.isDigit(sql.charAt(i))) {
                                i++;
                            }
                        } else {
                            i = saved; // not a valid exponent — backtrack
                        }
                    }
                    out.append('?');
                    continue;
                }
            }

            out.append(c);
            i++;
        }

        return out.toString().trim();
    }
}
