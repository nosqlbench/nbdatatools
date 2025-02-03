package io.nosqlbench.nbvectors.jsonalyze;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/// A utility class for converting a JSON array of objects into JSONL (JSON Lines) format.
///
/// This class provides a method to efficiently convert a JSON file that contains an array of objects
/// into a JSONL file, where each line is a separate JSON object. It uses Jackson's streaming API for
/// processing large files without loading the entire file into memory.
public final class JsonTestUtility {

  /// Private constructor to prevent instantiation.
  private JsonTestUtility() { }

  /// Converts a JSON array of objects into JSONL format.
  ///
  /// The input file is expected to contain a JSON array of objects. Each object in the array is written as a
  /// single line in the output file in compact JSON format.
  ///
  /// **Parameters:**
  /// - `inputFile`: the JSON file containing an array of objects.
  /// - `outputFile`: the file to which the JSONL output will be written.
  ///
  /// **Throws:** [IOException] if an I/O error occurs during processing.
  public static void convertJsonArrayToJsonl(File inputFile, File outputFile) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonFactory factory = objectMapper.getFactory();

    try (JsonParser parser = factory.createParser(inputFile);
         BufferedWriter writer = new BufferedWriter(
             new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8))) {

      // Verify that the input starts with a JSON array.
      if (parser.nextToken() != JsonToken.START_ARRAY) {
        throw new IOException("Expected JSON array start token");
      }

      // Process each JSON object within the array.
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        // Read the current JSON object.
        Object jsonObject = objectMapper.readValue(parser, Object.class);
        // Write the object as a compact JSON string followed by a newline.
        writer.write(objectMapper.writeValueAsString(jsonObject));
        writer.newLine();

      }
    }
  }
}
