package com.splunk.otel.db2.util;

import java.util.regex.Pattern;

/**
 * Replaces SQL literal values with {@code ?} so query text can be safely
 * emitted as telemetry without leaking PII or sensitive data.
 *
 * Handled:
 *   single-quoted strings  'O''Brien'          →  ?
 *   prefixed string literals
 *     X/x'...' (hex)       X'deadbeef'         →  ?
 *     G/g'...' (graphic)   G'hello'            →  ?
 *     N/n'...' (national)  N'Alice'            →  ?
 *     UX/ux'...' (Unicode hex)  UX'0041'       →  ?
 *     U&/u&'...' (Unicode escape)  U&'\0041'   →  ?
 *   numeric literals       42, 3.14,
 *                          1.5E10, 2.0E-3      →  ?
 *   line comments          -- …                →  stripped → space  (\n or \r)
 *   block comments         /* … * /            →  stripped → space (nested supported)
 *   Unicode format chars   U+200B, U+00AD …    →  stripped before processing
 *
 * Preserved:
 *   double-quoted identifiers  "MyTable"  — Db2 LUW uses " for delimited
 *   identifiers, not string literals; replacing them collapses different objects.
 *
 * Fail-closed: if any literal remnant survives obfuscation (unmatched {@code '}
 * outside a delimited identifier, or standalone digit-run ≥ 4 outside an
 * identifier), returns {@link #OBFUSCATION_FAILED} rather than leaking data.
 *
 * The sanitized SQL is intended for display and grouping, not re-execution.
 */
public final class SqlObfuscator {

    public static final String OBFUSCATION_FAILED = "<obfuscation-failed>";

    // Matches residual literal fragments that should have been replaced.
    // Applied to a version of the result with all "..." identifiers blanked out,
    // so identifiers like "it's" or "2024" do not trigger false positives.
    private static final Pattern RESIDUAL_LITERAL = Pattern.compile(
        "'|(?<![A-Za-z0-9_$#@])\\d{4,}(?![A-Za-z0-9_$#@])");

    private static final Pattern WHITESPACE_RUN = Pattern.compile("\\s+");

    private SqlObfuscator() {}

    public static String obfuscate(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        String input = stripFormatChars(sql);
        if (input.isEmpty()) {
            return input;
        }

        StringBuilder out = new StringBuilder(input.length());
        int i = 0;
        int len = input.length();

        while (i < len) {
            char c = input.charAt(i);

            // Line comment: -- ... \n or \r
            if (c == '-' && i + 1 < len && input.charAt(i + 1) == '-') {
                i += 2;
                while (i < len && input.charAt(i) != '\n' && input.charAt(i) != '\r') {
                    i++;
                }
                out.append(' ');
                continue;
            }

            // Block comment: /* ... */ with nesting support
            if (c == '/' && i + 1 < len && input.charAt(i + 1) == '*') {
                i += 2;
                int depth = 1;
                while (i + 1 < len && depth > 0) {
                    if (input.charAt(i) == '/' && input.charAt(i + 1) == '*') {
                        depth++;
                        i += 2;
                    } else if (input.charAt(i) == '*' && input.charAt(i + 1) == '/') {
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

            // Prefixed string literals: X'...' G'...' N'...' UX'...' U&'...'
            int prefixSkip = prefixedLiteralSkip(input, i, len);
            if (prefixSkip > 0) {
                i += prefixSkip;
                while (i < len && input.charAt(i) != '\'') {
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
                    if (input.charAt(i) == '\'' && i + 1 < len && input.charAt(i + 1) == '\'') {
                        i += 2;
                    } else if (input.charAt(i) == '\'') {
                        i++;
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
                    char q = input.charAt(i);
                    out.append(q);
                    i++;
                    if (q == '"') {
                        if (i < len && input.charAt(i) == '"') {
                            out.append('"');
                            i++;
                        } else {
                            break;
                        }
                    }
                }
                continue;
            }

            // Numeric literal — only when preceded by a non-identifier character.
            // Db2 identifier chars: letters, digits, _, $, #, @
            if (Character.isDigit(c)) {
                char prev = (i == 0) ? ' ' : input.charAt(i - 1);
                boolean insideIdentifier = Character.isLetterOrDigit(prev)
                        || prev == '_' || prev == '$' || prev == '#' || prev == '@';
                if (!insideIdentifier) {
                    while (i < len && (Character.isDigit(input.charAt(i)) || input.charAt(i) == '.')) {
                        i++;
                    }
                    // optional exponent: [eE][+-]?[0-9]+
                    if (i < len && (input.charAt(i) == 'e' || input.charAt(i) == 'E')) {
                        int saved = i;
                        i++;
                        if (i < len && (input.charAt(i) == '+' || input.charAt(i) == '-')) {
                            i++;
                        }
                        if (i < len && Character.isDigit(input.charAt(i))) {
                            while (i < len && Character.isDigit(input.charAt(i))) {
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

        String result = WHITESPACE_RUN.matcher(out.toString()).replaceAll(" ").trim();

        // Fail-closed: if any literal remnant survived, redact entirely.
        if (containsResidualLiteral(result)) {
            return OBFUSCATION_FAILED;
        }

        return result;
    }

    /**
     * Checks for residual literal fragments in the obfuscated result.
     * Scans past double-quoted identifiers (which legitimately may contain
     * single quotes or digit-sequences) to avoid false positives.
     */
    private static boolean containsResidualLiteral(String result) {
        // Build a version with "..." identifier bodies blanked to avoid false positives
        // from identifiers like "it's" (contains ') or "2024" (contains digit-run).
        StringBuilder stripped = new StringBuilder(result.length());
        int i = 0;
        while (i < result.length()) {
            char c = result.charAt(i);
            if (c == '"') {
                stripped.append('I'); // open-quote placeholder
                i++;
                while (i < result.length()) {
                    char q = result.charAt(i++);
                    if (q == '"') {
                        if (i < result.length() && result.charAt(i) == '"') {
                            i++; // "" escape — skip both
                        } else {
                            break; // closing quote
                        }
                    }
                    // identifier body intentionally not appended — blanked to prevent
                    // false positives from identifiers like "2024" or "it's"
                }
                stripped.append('I'); // close-quote placeholder
            } else {
                stripped.append(c);
                i++;
            }
        }
        return RESIDUAL_LITERAL.matcher(stripped.toString()).find();
    }

    /**
     * Strips Unicode format characters (category Cf — zero-width spaces, soft
     * hyphens, etc.) that can produce different fingerprints for identical queries.
     * Only allocates a new string when format chars are actually present.
     */
    private static String stripFormatChars(String sql) {
        for (int i = 0; i < sql.length(); i++) {
            if (Character.getType(sql.charAt(i)) == Character.FORMAT) {
                StringBuilder sb = new StringBuilder(sql.length());
                sb.append(sql, 0, i);
                for (int j = i + 1; j < sql.length(); j++) {
                    char c = sql.charAt(j);
                    if (Character.getType(c) != Character.FORMAT) {
                        sb.append(c);
                    }
                }
                return sb.toString();
            }
        }
        return sql;
    }

    /**
     * Returns the number of characters to skip past the prefix + opening quote
     * for Db2 prefixed string literals. Returns 0 if not a prefixed literal.
     */
    private static int prefixedLiteralSkip(String sql, int i, int len) {
        char c = sql.charAt(i);
        // Single-char prefixes: X/x G/g N/n followed immediately by '
        if (i + 1 < len && sql.charAt(i + 1) == '\'') {
            if (c == 'X' || c == 'x' || c == 'G' || c == 'g' || c == 'N' || c == 'n') {
                return 2;
            }
        }
        // Two-char prefixes: UX/ux or U&/u& followed immediately by '
        if ((c == 'U' || c == 'u') && i + 2 < len && sql.charAt(i + 2) == '\'') {
            char next = sql.charAt(i + 1);
            if (next == 'X' || next == 'x' || next == '&') {
                return 3;
            }
        }
        return 0;
    }
}
