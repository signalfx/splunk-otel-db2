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

        // Pre-strip ALL Unicode format (Cf) codepoints before lexical analysis.
        // Character.getType(char) misses supplementary-plane FORMAT codepoints such as
        // U+E0020 (TAG SPACE) which are encoded as surrogate pairs in Java Strings —
        // iterating chars would see two non-FORMAT chars and leave the codepoint intact,
        // potentially bypassing delimiter detection. Iterating by codepoint is exact.
        // Double-quoted identifier bodies are not yet parsed here; format chars inside
        // them are re-introduced by the main loop via the raw sql reference — they are
        // preserved by design (a "A\u200BB" identifier is distinct from "AB").
        sql = stripFormatCodepoints(sql);

        // Fail-closed early: malformed SQL with an odd number of single-quotes (outside
        // double-quoted identifiers) cannot be safely obfuscated — a quote desync would
        // cause literal text to be emitted as identifier text, bypassing the residual
        // check. Return OBFUSCATION_FAILED rather than risk leaking data.
        if (hasUnbalancedQuotes(sql)) {
            return OBFUSCATION_FAILED;
        }

        StringBuilder out = new StringBuilder(sql.length());
        int i = 0;
        int len = sql.length();

        while (i < len) {
            char c = sql.charAt(i);

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
                i = consumeQuotedStringBody(sql, i, len);
                out.append('?');
                continue;
            }

            // Single-quoted string with '' escape: 'O''Brien'
            if (c == '\'') {
                i++;
                i = consumeQuotedStringBody(sql, i, len);
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
                    // optional exponent: (format*)? [eE] (format*)? [+-]? (format*)? [0-9]+
                    // Skip format chars before e/E too so 1\u200BE123 → single ?
                    int ePos = nextNonFormat(sql, i, len);
                    if (ePos < len && (sql.charAt(ePos) == 'e' || sql.charAt(ePos) == 'E')) {
                        i = ePos; // advance past any format chars before e/E
                    }
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

    /**
     * Consumes a single-quoted string body starting at {@code i} (after the opening quote).
     * Handles {@code ''} escape sequences with format-char transparency: the two apostrophes
     * in an escape may be separated by format characters.
     * Returns the index of the first character after the closing quote.
     */
    private static int consumeQuotedStringBody(String sql, int i, int len) {
        while (i < len) {
            char ch = sql.charAt(i);
            if (ch == '\'') {
                // Check if the next non-format char is also ' (escaped quote)
                int nxt = nextNonFormat(sql, i + 1, len);
                if (nxt < len && sql.charAt(nxt) == '\'') {
                    i = nxt + 1; // skip both quotes (and any format chars between them)
                } else {
                    i++; // closing quote
                    break;
                }
            } else {
                i++;
            }
        }
        return i;
    }

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
     *
     * Uses {@code codePointBefore} so that a supplementary-plane character stored as a
     * surrogate pair is evaluated as a single codepoint, matching the codepoint-aware
     * {@code stripFormatCodepoints} pass that runs first.
     */
    private static char prevNonFormat(String sql, int i) {
        while (i >= 0) {
            int cp = sql.codePointBefore(i + 1);
            if (Character.getType(cp) != Character.FORMAT) {
                return (char) cp; // for BMP chars this is the exact char; for supplementary, only low surrogate returned — safe as "not identifier-char"
            }
            i -= Character.charCount(cp);
        }
        return ' ';
    }

    /**
     * Returns true if {@code sql} (already format-stripped) contains an odd number of
     * single-quote characters outside of double-quoted identifiers and outside of comments.
     * An odd count means at least one literal is unterminated, which would desync the lexer
     * and risk emitting literal text as identifier output.
     *
     * State machine mirrors the main lexer: single-quoted strings, double-quoted identifiers,
     * line comments, and nested block comments are all handled, so {@code --} / {@code /*}
     * tokens inside a string literal (e.g. {@code 'abc--def'}) are not mis-classified as
     * comment delimiters.
     */
    private static boolean hasUnbalancedQuotes(String sql) {
        int quoteCount = 0;
        boolean inSingleQuoted = false;
        boolean inDoubleQuoted = false;
        int i = 0;
        int len = sql.length();
        while (i < len) {
            char c = sql.charAt(i);
            if (inSingleQuoted) {
                if (c == '\'') {
                    if (i + 1 < len && sql.charAt(i + 1) == '\'') {
                        i += 2; // '' escape — both quotes belong to this literal
                    } else {
                        quoteCount++; // closing quote
                        inSingleQuoted = false;
                        i++;
                    }
                } else {
                    i++;
                }
            } else if (inDoubleQuoted) {
                if (c == '"') {
                    if (i + 1 < len && sql.charAt(i + 1) == '"') {
                        i += 2; // "" escape
                    } else {
                        inDoubleQuoted = false;
                        i++;
                    }
                } else {
                    i++;
                }
            } else if (c == '\'') {
                quoteCount++; // opening quote
                inSingleQuoted = true;
                i++;
            } else if (c == '"') {
                inDoubleQuoted = true;
                i++;
            } else if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
                // line comment — skip to end of line
                i += 2;
                while (i < len && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') i++;
            } else if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                // nested block comment — depth-tracked, matching the main lexer
                i += 2;
                int depth = 1;
                while (i < len && depth > 0) {
                    if (sql.charAt(i) == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                        depth++;
                        i += 2;
                    } else if (sql.charAt(i) == '*' && i + 1 < len && sql.charAt(i + 1) == '/') {
                        depth--;
                        i += 2;
                    } else {
                        i++;
                    }
                }
            } else {
                i++;
            }
        }
        // Even count = every opening quote has a matching closing quote
        return (quoteCount % 2) != 0;
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

        // Identifier boundary check: if the prefix letter is part of an identifier
        // (e.g. MAX'...', colN'...') do not treat it as a literal prefix.
        // Uses prevNonFormat to skip any format chars before the current position.
        char prev = prevNonFormat(sql, i - 1);
        if (Character.isLetterOrDigit(prev) || prev == '_' || prev == '$' || prev == '#' || prev == '@') {
            return -1;
        }

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
     * Strips Unicode FORMAT (Cf) codepoints from SQL outside double-quoted identifiers,
     * iterating by codepoint to handle supplementary-plane FORMAT chars (e.g. U+E0020
     * TAG SPACE) that {@code Character.getType(char)} misses (it sees surrogate chars, not
     * the actual codepoint). FORMAT chars inside {@code "..."} identifiers are preserved
     * unchanged — a {@code "A\u200BB"} identifier is lexically distinct from {@code "AB"}.
     */
    static String stripFormatCodepoints(String s) {
        // Fast path: most SQL contains no FORMAT codepoints at all.
        boolean hasFormat = false;
        for (int i = 0, len = s.length(); i < len; ) {
            int cp = s.codePointAt(i);
            if (Character.getType(cp) == Character.FORMAT) {
                hasFormat = true;
                break;
            }
            i += Character.charCount(cp);
        }
        if (!hasFormat) return s;

        StringBuilder sb = new StringBuilder(s.length());
        boolean inDoubleQuotedId = false; // inside "..."
        boolean inSingleQuotedStr = false; // inside '...'
        for (int i = 0, len = s.length(); i < len; ) {
            int cp = s.codePointAt(i);
            int charCount = Character.charCount(cp);

            if (inDoubleQuotedId) {
                // Inside "..." — preserve everything verbatim (format chars included)
                sb.appendCodePoint(cp);
                if (cp == '"') {
                    // Check for "" escape: peek at next codepoint
                    int next = i + charCount;
                    if (next < len && s.codePointAt(next) == '"') {
                        sb.appendCodePoint('"');
                        i = next + Character.charCount('"');
                        continue;
                    }
                    inDoubleQuotedId = false; // closing quote
                }
            } else if (inSingleQuotedStr) {
                // Inside '...' — strip FORMAT codepoints, track '' escape
                if (cp == '\'') {
                    int next = i + charCount;
                    if (next < len && s.codePointAt(next) == '\'') {
                        // '' escape — emit both, skip both
                        sb.appendCodePoint(cp);
                        sb.appendCodePoint(cp);
                        i = next + charCount;
                        continue;
                    }
                    // Closing quote
                    sb.appendCodePoint(cp);
                    inSingleQuotedStr = false;
                } else if (Character.getType(cp) != Character.FORMAT) {
                    sb.appendCodePoint(cp);
                }
                // else: strip FORMAT codepoint inside string (main loop handles it later)
            } else if (cp == '"') {
                inDoubleQuotedId = true;
                sb.appendCodePoint(cp);
            } else if (cp == '\'') {
                inSingleQuotedStr = true;
                sb.appendCodePoint(cp);
            } else if (Character.getType(cp) != Character.FORMAT) {
                sb.appendCodePoint(cp);
                // else: strip FORMAT codepoint outside identifier/string
            }
            i += charCount;
        }
        return sb.toString();
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
