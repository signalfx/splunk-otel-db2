package com.splunk.otel.db2.util;

import java.util.regex.Pattern;

/**
 * Replaces SQL literal values with {@code ?} so query text can be safely
 * emitted as telemetry without leaking PII or sensitive data.
 *
 * Handled:
 *   single-quoted strings  'O''Brien'          →  ?
 *   prefixed string literals ('' escape supported in all)
 *     X/x'...' (hex)       X'deadbeef'         →  ?
 *     G/g'...' (graphic)   G'O''Brien'         →  ?
 *     N/n'...' (national)  N'Alice'            →  ?
 *     GX/gx'...' (hex-graphic)                 →  ?
 *     BX/bx'...' (binary)                      →  ?
 *     UX/ux'...' (Unicode hex)  UX'0041'       →  ?
 *     U&/u&'...' (Unicode escape)  U&'\0041'   →  ?
 *   numeric literals       42, 3.14,
 *                          1.5E10, 2.0E-3      →  ?
 *   line comments          -- …                →  stripped → space  (\n or \r)
 *   block comments         /* … * /            →  stripped → space (nested supported)
 *   Unicode format chars   U+200B, U+00AD …    →  every delimiter check and boundary
 *                          check is format-char-aware throughout; format chars inside
 *                          "..." identifiers are preserved unchanged.
 *
 * Preserved:
 *   double-quoted identifiers  "MyTable", "Order  History"  — Db2 LUW uses "
 *   for delimited identifiers; internal whitespace and format chars are preserved.
 *
 * Fail-closed: if any literal remnant survives obfuscation (unmatched {@code '}
 * outside a delimited identifier, or standalone digit-run ≥ 4 outside an
 * identifier), returns {@link #OBFUSCATION_FAILED} rather than leaking data.
 *
 * The sanitized SQL is intended for display and grouping, not re-execution.
 */
public final class SqlObfuscator {

    public static final String OBFUSCATION_FAILED = "<obfuscation-failed>";

    // Matches residual literal fragments. Uses Unicode letter/number categories
    // (\p{L}\p{N}) to match the same identifier boundary as Character.isLetterOrDigit,
    // so Unicode identifiers like Δ2024 are not falsely flagged.
    // Applied after "..." identifier bodies are blanked, so "it's" / "2024" are safe.
    private static final Pattern RESIDUAL_LITERAL = Pattern.compile(
        "'|(?<![\\p{L}\\p{N}_$#@])\\d{4,}(?![\\p{L}\\p{N}_$#@])");

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

            // Skip Unicode format chars (Cf) outside all other lexical contexts.
            // All delimiter and boundary checks below are independently format-char-aware
            // via nextNonFormat / prevNonFormat, so skipping here does not mis-classify
            // adjacent tokens.
            if (Character.getType(c) == Character.FORMAT) {
                i++;
                continue;
            }

            // Line comment: -- ... \n or \r  (format-char-aware dash pair)
            if (c == '-') {
                int nxt = nextNonFormat(sql, i + 1, len);
                if (nxt < len && sql.charAt(nxt) == '-') {
                    i = nxt + 1;
                    while (i < len && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
                        i++;
                    }
                    out.append(' ');
                    continue;
                }
            }

            // Block comment: /* ... */ with nesting support.
            // All /* and */ delimiters — including nested ones — are format-char-aware.
            if (c == '/') {
                int nxt = nextNonFormat(sql, i + 1, len);
                if (nxt < len && sql.charAt(nxt) == '*') {
                    i = nxt + 1;
                    int depth = 1;
                    while (i < len && depth > 0) {
                        char ch = sql.charAt(i);
                        if (Character.getType(ch) == Character.FORMAT) {
                            i++;
                            continue;
                        }
                        if (ch == '/') {
                            int n2 = nextNonFormat(sql, i + 1, len);
                            if (n2 < len && sql.charAt(n2) == '*') {
                                depth++;
                                i = n2 + 1;
                                continue;
                            }
                        } else if (ch == '*') {
                            int n2 = nextNonFormat(sql, i + 1, len);
                            if (n2 < len && sql.charAt(n2) == '/') {
                                depth--;
                                i = n2 + 1;
                                continue;
                            }
                        }
                        i++;
                    }
                    // if depth > 0 the comment was unterminated; i == len already
                    out.append(' ');
                    continue;
                }
            }

            // Prefixed string literals: X'...' G'...' N'...' GX'...' BX'...' UX'...' U&'...'
            // Prefix detection uses nextNonFormat so format chars between prefix chars
            // and the opening quote are transparent.
            int bodyStart = prefixedLiteralBodyStart(sql, i, len);
            if (bodyStart >= 0) {
                i = bodyStart;
                while (i < len) {
                    if (sql.charAt(i) == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') {
                        i += 2;
                    } else if (sql.charAt(i) == '\'') {
                        i++;
                        break;
                    } else {
                        i++;
                    }
                }
                out.append('?');
                continue;
            }

            // Single-quoted string with '' escape: 'O''Brien'
            if (c == '\'') {
                i++;
                while (i < len) {
                    if (sql.charAt(i) == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') {
                        i += 2;
                    } else if (sql.charAt(i) == '\'') {
                        i++;
                        break;
                    } else {
                        i++;
                    }
                }
                out.append('?');
                continue;
            }

            // Double-quoted delimited identifier — preserve as-is, including internal
            // whitespace and format chars ("A\u200BB" names a different object than "AB").
            if (c == '"') {
                out.append(c);
                i++;
                while (i < len) {
                    char q = sql.charAt(i);
                    out.append(q);
                    i++;
                    if (q == '"') {
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

            // Numeric literal — only when preceded by a non-identifier character.
            // prevNonFormat skips format chars so COL\u200B2024 correctly stays as identifier.
            if (Character.isDigit(c)) {
                char prev = prevNonFormat(sql, i - 1);
                boolean insideIdentifier = Character.isLetterOrDigit(prev)
                        || prev == '_' || prev == '$' || prev == '#' || prev == '@';
                if (!insideIdentifier) {
                    // consume mantissa: digits, dots, and format chars between them
                    while (i < len) {
                        char ch = sql.charAt(i);
                        if (Character.isDigit(ch) || ch == '.') {
                            i++;
                        } else if (Character.getType(ch) == Character.FORMAT) {
                            int nxt = nextNonFormat(sql, i, len);
                            if (nxt < len && (Character.isDigit(sql.charAt(nxt)) || sql.charAt(nxt) == '.')) {
                                i = nxt;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    // optional exponent: [eE] (format*)? [+-]? (format*)? [0-9]+
                    // All three segments are format-char-aware so 1E\u200B123 → single ?
                    if (i < len && (sql.charAt(i) == 'e' || sql.charAt(i) == 'E')) {
                        int saved = i;
                        i++; // consume e/E
                        i = nextNonFormat(sql, i, len); // skip format chars after e/E
                        if (i < len && (sql.charAt(i) == '+' || sql.charAt(i) == '-')) {
                            i++;
                            i = nextNonFormat(sql, i, len); // skip format chars after sign
                        }
                        if (i < len && Character.isDigit(sql.charAt(i))) {
                            while (i < len) {
                                char ch = sql.charAt(i);
                                if (Character.isDigit(ch)) {
                                    i++;
                                } else if (Character.getType(ch) == Character.FORMAT) {
                                    int nxt = nextNonFormat(sql, i, len);
                                    if (nxt < len && Character.isDigit(sql.charAt(nxt))) {
                                        i = nxt;
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
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

        // Normalise whitespace runs to a single space, preserving internal whitespace
        // inside double-quoted identifiers. Must run before containsResidualLiteral.
        String result = normalizeWhitespace(out.toString());

        // Fail-closed: if any literal remnant survived, redact entirely.
        if (containsResidualLiteral(result)) {
            return OBFUSCATION_FAILED;
        }

        return result;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Returns the index of the next non-format character at or after {@code i}. */
    private static int nextNonFormat(String sql, int i, int len) {
        while (i < len && Character.getType(sql.charAt(i)) == Character.FORMAT) {
            i++;
        }
        return i;
    }

    /**
     * Returns the first non-format character found scanning backward from {@code i}.
     * Returns a space (safe "not an identifier char" sentinel) if no such character exists.
     */
    private static char prevNonFormat(String sql, int i) {
        while (i >= 0 && Character.getType(sql.charAt(i)) == Character.FORMAT) {
            i--;
        }
        return (i >= 0) ? sql.charAt(i) : ' ';
    }

    /**
     * If position {@code i} starts a Db2 prefixed string literal, returns the index
     * of the character immediately after the opening {@code '} (i.e., the body start).
     * Returns -1 if not a prefixed literal.
     *
     * Two-char prefixes are checked before single-char prefixes so that {@code GX'...'}
     * is not mis-classified as a bare {@code G} prefix.  All adjacency checks use
     * {@link #nextNonFormat} so format characters between prefix chars and the opening
     * quote are transparent.
     *
     * Supported forms:
     *   Single-char:  X/x (hex), G/g (graphic), N/n (national)
     *   Two-char:     GX/gx (hex-graphic), BX/bx (binary), UX/ux (Unicode hex),
     *                 U&/u& (Unicode escape)
     */
    private static int prefixedLiteralBodyStart(String sql, int i, int len) {
        char c = sql.charAt(i);

        // Candidates for any prefix
        boolean couldBePrefix = c == 'X' || c == 'x'
                || c == 'G' || c == 'g'
                || c == 'N' || c == 'n'
                || c == 'U' || c == 'u'
                || c == 'B' || c == 'b';
        if (!couldBePrefix) return -1;

        int nxt1 = nextNonFormat(sql, i + 1, len);
        if (nxt1 >= len) return -1;
        char second = sql.charAt(nxt1);

        // Try two-char prefixes first (GX before G, BX before B, UX/U& before U)
        boolean validTwoChar = false;
        if ((c == 'U' || c == 'u') && (second == 'X' || second == 'x' || second == '&')) {
            validTwoChar = true;
        } else if ((c == 'G' || c == 'g') && (second == 'X' || second == 'x')) {
            validTwoChar = true;
        } else if ((c == 'B' || c == 'b') && (second == 'X' || second == 'x')) {
            validTwoChar = true;
        }

        if (validTwoChar) {
            int nxt2 = nextNonFormat(sql, nxt1 + 1, len);
            if (nxt2 < len && sql.charAt(nxt2) == '\'') {
                return nxt2 + 1; // body starts after opening '
            }
            // Second char looked like a two-char prefix letter but ' didn't follow —
            // not a literal at this position.
            return -1;
        }

        // Single-char prefixes
        if ((c == 'X' || c == 'x' || c == 'G' || c == 'g' || c == 'N' || c == 'n')
                && second == '\'') {
            return nxt1 + 1; // body starts after opening '
        }

        return -1;
    }

    /**
     * Collapses runs of whitespace to a single space, preserving whitespace
     * inside double-quoted delimited identifiers unchanged.
     */
    private static String normalizeWhitespace(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        boolean inIdentifier = false;
        boolean prevWasSpace = false;
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (!inIdentifier && c == '"') {
                inIdentifier = true;
                prevWasSpace = false;
                sb.append(c);
                i++;
            } else if (inIdentifier) {
                sb.append(c);
                i++;
                if (c == '"') {
                    if (i < s.length() && s.charAt(i) == '"') {
                        sb.append('"');
                        i++; // "" escape — stay inside identifier
                    } else {
                        inIdentifier = false; // closing quote
                    }
                }
            } else if (Character.isWhitespace(c)) {
                if (!prevWasSpace) {
                    sb.append(' ');
                    prevWasSpace = true;
                }
                i++;
            } else {
                sb.append(c);
                prevWasSpace = false;
                i++;
            }
        }
        // trim leading/trailing spaces
        int start = 0;
        int end = sb.length();
        while (start < end && sb.charAt(start) == ' ') start++;
        while (end > start && sb.charAt(end - 1) == ' ') end--;
        return sb.substring(start, end);
    }

    /**
     * Checks for residual literal fragments in the obfuscated result.
     * Scans past double-quoted identifiers (which legitimately may contain
     * single quotes or digit-sequences) to avoid false positives.
     */
    static boolean containsResidualLiteral(String result) {
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
}
