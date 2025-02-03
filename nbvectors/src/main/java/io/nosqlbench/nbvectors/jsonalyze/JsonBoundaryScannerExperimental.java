package io.nosqlbench.nbvectors.jsonalyze;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// A utility class for scanning a JSON file for object boundaries.
/// This class provides a method to scan a JSON file (assumed to be a JSON array of objects)
/// for the comma tokens that separate objects. Only commas that are not within JSON string
/// literals (i.e. those not escaped) and that occur at the top-level array (depth 1) are
/// considered.
/// When a candidate boundary is found, a context string is built from recently read characters plus
/// a
/// lookahead. This context is then validated against a user-provided regex [Pattern].
/// If the pattern matches, the user-supplied [LongConsumer] is invoked with the byte offset of the
/// boundary.
/// **Note:** This implementation assumes the JSON file is encoded in UTF-8.
public final class JsonBoundaryScannerExperimental {

  /// Maximum number of previously read characters to keep as context.
  private static final int CONTEXT_BUFFER_SIZE = 50;
  /// Number of bytes to look ahead when checking a candidate.
  private static final int LOOKAHEAD_SIZE = 20;

  /// Private constructor since this is a utility class.
  private JsonBoundaryScannerExperimental() {
  }

  /// Scans the JSON file at the given file path for object boundaries.
  /// An object boundary is assumed to be a comma (',') that separates JSON objects in a top-level
  /// JSON array. This method uses a buffered, pushback stream so that a small lookahead (up
  /// to [#LOOKAHEAD_SIZE] bytes) can be obtained without disturbing the main scan. The
  /// candidate comma is further validated by matching a context string (built from a
  /// ring-buffer of previously read characters plus the lookahead) against the provided regex
  /// [Pattern]. If a match is found the [LongConsumer] is invoked with the byte offset (in
  /// bytes) at which the comma was found.
  ///
  /// This method correctly handles valid whitespace and escaped characters inside JSON string literals.
  /// **Example boundary pattern:**
  /// ```java
  /// // Matches a comma that is preceded by a closing curly brace and followed
  /// // (ignoring whitespace) by an opening curly brace.
  /// Pattern boundaryPattern = Pattern.compile("(?<=\\})\\s*,\\s*(?=\\{)");
  ///```
  /// @param filePath the path to the JSON file to scan.
  /// @param offsetConsumer that will be called with the byte offset of each
  /// valid boundary.
  /// @param boundaryPattern a regex [Pattern] used to match the boundary (including any
  /// allowed whitespace).
  /// @throws IOException if an I/O error occurs while reading the file.
  ///
  public static void scanJsonFile(
      Path filePath,
      LongConsumer offsetConsumer,
      Pattern boundaryPattern
  ) throws IOException
  {
    // Wrap the input stream in a BufferedInputStream and then a PushbackInputStream so that we can read ahead.
    try (PushbackInputStream pbis = new PushbackInputStream(
        new BufferedInputStream(Files.newInputStream(
            filePath)), LOOKAHEAD_SIZE
    ))
    {

      long byteOffset = 0;
      int b;
      boolean inString = false;
      boolean escape = false;
      int depth = 0; // JSON structural depth.
      StringBuilder contextBuffer = new StringBuilder();

      while ((b = pbis.read()) != -1) {
        char ch = (char) b;
        byteOffset++;

        // Append the current character to the context buffer and ensure it does not exceed the max size.
        contextBuffer.append(ch);
        if (contextBuffer.length() > CONTEXT_BUFFER_SIZE) {
          contextBuffer.delete(0, contextBuffer.length() - CONTEXT_BUFFER_SIZE);
        }

        if (inString) {
          if (escape) {
            // Current character is escaped; do not process it specially.
            escape = false;
          } else if (ch == '\\') {
            // Begin escape sequence.
            escape = true;
          } else if (ch == '"') {
            // End of string literal.
            inString = false;
          }
          // While inside a string literal, ignore structural tokens.
        } else {
          // Outside of a string literal.
          if (ch == '"') {
            inString = true;
          } else if (ch == '{' || ch == '[') {
            depth++;
          } else if (ch == '}' || ch == ']') {
            depth--;
          } else if (ch == ',' && depth == 1) {
            // Candidate boundary: a comma at depth 1 (i.e. separating top-level objects).
            // Look ahead without permanently consuming the bytes.
            byte[] lookaheadBytes = new byte[LOOKAHEAD_SIZE];
            int readCount = pbis.read(lookaheadBytes);
            if (readCount > 0) {
              pbis.unread(lookaheadBytes, 0, readCount);
            }
            String lookaheadStr = new String(lookaheadBytes, 0, readCount, StandardCharsets.UTF_8);
            // Build the context string from the context buffer (which already includes the comma) and the lookahead.
            String context = contextBuffer.toString() + lookaheadStr;
            Matcher matcher = boundaryPattern.matcher(context);
            if (matcher.find()) {
              // Report the boundary. (The candidate comma is at byteOffset - 1.)
              offsetConsumer.accept(byteOffset - 1);
            }
          }
        }
      }
    }
  }
}
