package io.nosqlbench.readers;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.fileio.VectorStreamReader;
import io.nosqlbench.nbvectors.api.services.FileType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

/**
 A Streamer implementation that reads CSV files with embedded JSON arrays.
 This streamer automatically detects which CSV column contains a JSON array of numbers
 and uses that as the source of vector data. */
@Encoding(FileType.csv)
@DataType(float[].class)
public class CsvJsonArrayStreamer implements VectorStreamReader<float[]> {

  private  Path filePath;
  private int vectorColumn = -1;
  private boolean firstLineIsData = false;

  // Pattern to match JSON arrays. Simple pattern to detect likely JSON array candidates
  private static final Pattern JSON_ARRAY_PATTERN = Pattern.compile("\\s*\\[\\s*[\\d.-]+.*\\]\\s*");


  /**
   Initializes the streamer by scanning the CSV file to determine which column
   contains the JSON array of numbers and if the first row is a header.
   */
  private void initialize() {
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
      String firstLine = reader.readLine();
      if (firstLine == null) {
        throw new IOException("Empty CSV file: " + filePath);
      }

      String secondLine = reader.readLine();
      if (secondLine == null) {
        // File has only one line, must be data
        String[] fields = parseCsvLine(firstLine);
        findVectorColumn(fields);
        firstLineIsData = true;
        return;
      }

      // Parse both lines
      String[] firstLineFields = parseCsvLine(firstLine);
      String[] secondLineFields = parseCsvLine(secondLine);

      // Check if first line is likely a header by comparing structure
      boolean firstLineHasVectorArray = false;
      boolean secondLineHasVectorArray = false;

      // Check for vector arrays in both lines
      for (String field : firstLineFields) {
        if (JSON_ARRAY_PATTERN.matcher(field.trim()).matches() && isJsonNumberArray(field.trim())) {
          firstLineHasVectorArray = true;
          break;
        }
      }

      for (String field : secondLineFields) {
        if (JSON_ARRAY_PATTERN.matcher(field.trim()).matches() && isJsonNumberArray(field.trim())) {
          secondLineHasVectorArray = true;
          break;
        }
      }

      // If first line lacks arrays but second has them, first is likely a header
      if (!firstLineHasVectorArray && secondLineHasVectorArray) {
        firstLineIsData = false;
        findVectorColumnInLine(secondLineFields);
      }
      // If both have arrays or neither has arrays, we need more analysis
      else if (firstLineHasVectorArray == secondLineHasVectorArray) {
        // Compare field types to detect header
        boolean structuralDifference =
            detectStructuralDifference(firstLineFields, secondLineFields);

        if (structuralDifference) {
          // Structural difference suggests first line is header
          firstLineIsData = false;
          findVectorColumnInLine(secondLineFields);
        } else {
          // No structural difference, treat first line as data
          firstLineIsData = true;
          findVectorColumnInLine(firstLineFields);
        }
      }
      // First line has arrays but second doesn't - treat first as data
      else {
        firstLineIsData = true;
        findVectorColumnInLine(firstLineFields);
      }

      if (vectorColumn == -1) {
        throw new IOException("No JSON array column found in CSV file: " + filePath);
      }

    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize CSV JSON array streamer", e);
    }
  }

  /**
   Detects structural differences between two CSV rows to determine if the first is likely a header.
   @param firstRow
   First row fields
   @param secondRow
   Second row fields
   @return True if structural differences suggest first row is a header
   */
  private boolean detectStructuralDifference(String[] firstRow, String[] secondRow) {
    // If row lengths differ significantly, this could indicate a header
    if (firstRow.length != secondRow.length) {
      return true;
    }

    int diffCount = 0;
    for (int i = 0; i < Math.min(firstRow.length, secondRow.length); i++) {
      String field1 = firstRow[i].trim();
      String field2 = secondRow[i].trim();

      // Check for patterns that suggest header vs data
      boolean field1IsLikelyHeader = isLikelyHeaderField(field1);
      boolean field2IsLikelyHeader = isLikelyHeaderField(field2);

      // If there's a difference in structure, increment count
      if (field1IsLikelyHeader != field2IsLikelyHeader) {
        diffCount++;
      }

      // If one field has JSON array and the other doesn't, significant difference
      boolean field1HasArray = JSON_ARRAY_PATTERN.matcher(field1).matches();
      boolean field2HasArray = JSON_ARRAY_PATTERN.matcher(field2).matches();

      if (field1HasArray != field2HasArray) {
        diffCount += 2; // Weight this higher
      }
    }

    // If there are enough differences, first row is likely a header
    return diffCount >= Math.max(1, firstRow.length / 3);
  }

  /**
   Checks if a field is likely to be a header field based on typical patterns.
   @param field
   The field to check
   @return True if the field appears to be a header label
   */
  private boolean isLikelyHeaderField(String field) {
    // Headers are typically short words without spaces or special characters
    if (field.length() < 20 && !field.contains(" ")
        && !field.matches(".*\\d{2,}.*"))
    { // No sequences of digits
      // Headers often use names like id, name, label, vector, etc.
      String lowerField = field.toLowerCase();
      return lowerField.matches("^[a-z_]+$") || lowerField.equals("id") || lowerField.contains(
          "name") || lowerField.contains("label") || lowerField.contains("vector")
             || lowerField.contains("desc");
    }
    return false;
  }

  /**
   Finds the vector column in a line of CSV data.
   @param fields
   Array of fields to examine
   @throws IOException
   if no vector column can be found
   */
  private void findVectorColumnInLine(String[] fields) throws IOException {
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i].trim();
      if (JSON_ARRAY_PATTERN.matcher(field).matches() && isJsonNumberArray(field)) {
        if (vectorColumn != -1) {
          throw new IOException(
              "Multiple JSON array columns found in CSV file. Only one is supported.");
        }
        vectorColumn = i;
      }
    }
  }

  /**
   Finds a vector column in a parsed CSV line.
   @param fields
   Array of CSV fields
   @throws IOException
   If multiple vector columns are found
   */
  private void findVectorColumn(String[] fields) throws IOException {
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i].trim();
      if (JSON_ARRAY_PATTERN.matcher(field).matches() && isJsonNumberArray(field)) {
        if (vectorColumn != -1) {
          throw new IOException(
              "Multiple JSON array columns found in CSV file. Only one is supported.");
        }
        vectorColumn = i;
      }
    }
  }

  /**
   Checks if a string is a valid JSON array containing only numbers.
   @param jsonStr
   The string to check
   @return True if the string is a JSON array of numbers, false otherwise
   */
  private boolean isJsonNumberArray(String jsonStr) {
    try {
      JsonElement element = JsonParser.parseString(jsonStr);
      if (!element.isJsonArray()) {
        return false;
      }

      JsonArray array = element.getAsJsonArray();
      for (JsonElement item : array) {
        if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isNumber()) {
          return false;
        }
      }

      return array.size() > 0;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   Parses a CSV line into fields, handling quoted fields properly.
   @param line
   The CSV line to parse
   @return An array of fields
   */
  private String[] parseCsvLine(String line) {
    List<String> fields = new ArrayList<>();
    boolean inQuotes = false;
    StringBuilder currentField = new StringBuilder();

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);

      if (c == '"') {
        inQuotes = !inQuotes;
      } else if (c == ',' && !inQuotes) {
        fields.add(currentField.toString());
        currentField = new StringBuilder();
      } else {
        currentField.append(c);
      }
    }

    // Add the last field
    fields.add(currentField.toString());

    return fields.toArray(new String[0]);
  }

  /**
   Converts a JSON array string to a float array.
   @param jsonArrayStr
   The JSON array string
   @return A float array
   */
  private float[] parseJsonFloatArray(String jsonArrayStr) {
    try {
      JsonArray jsonArray = JsonParser.parseString(jsonArrayStr).getAsJsonArray();
      float[] result = new float[jsonArray.size()];

      for (int i = 0; i < jsonArray.size(); i++) {
        result[i] = jsonArray.get(i).getAsFloat();
      }

      return result;
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON array: " + jsonArrayStr, e);
    }
  }

  /**
   Returns the name of this streamer.
   @return The name of the streamer
   */
  @Override
  public String getName() {
    return "CsvJsonArray(" + this.filePath.getFileName().toString() + ")";
  }

  /**
   Returns an iterator over the float vectors in the CSV file.
   This iterator streams data directly from the file without loading everything into memory.
   @return An iterator over float[]
   */
  @Override
  public Iterator<float[]> iterator() {
    return new CsvStreamIterator();
  }

  /**
   Creates a new CsvJsonArrayStreamer for the given file path.
   @param path
   The path to the CSV file containing JSON arrays
   */
  @Override
  public void open(Path path) {
    this.filePath = path;
    initialize();
  }

  /**
   An iterator that streams data directly from the CSV file.
   */
  private class CsvStreamIterator implements Iterator<float[]> {
    private BufferedReader reader;
    private String nextLine;
    private boolean initialized = false;

    public CsvStreamIterator() {
      try {
        reader = new BufferedReader(new FileReader(filePath.toFile()));

        // Skip the header line if it's not data
        if (!firstLineIsData) {
          reader.readLine();
        }

        // Read the first line to determine if there's data
        advance();
        initialized = true;
      } catch (IOException e) {
        close();
        throw new RuntimeException("Failed to initialize CSV iterator", e);
      }
    }

    private void advance() {
      try {
        nextLine = reader.readLine();

        // Skip any lines that don't have enough fields or have invalid JSON arrays
        while (nextLine != null) {
          String[] fields = parseCsvLine(nextLine);
          if (fields.length > vectorColumn) {
            String jsonArrayStr = fields[vectorColumn].trim();
            if (JSON_ARRAY_PATTERN.matcher(jsonArrayStr).matches()) {
              try {
                // Try to parse it just to validate
                parseJsonFloatArray(jsonArrayStr);
                // If we reach here, the line is valid
                break;
              } catch (Exception e) {
                // Invalid JSON array, skip this line
              }
            }
          }
          // Read the next line
          nextLine = reader.readLine();
        }
      } catch (IOException e) {
        close();
        throw new RuntimeException("Error reading from CSV file", e);
      }
    }

    @Override
    public boolean hasNext() {
      if (!initialized) {
        throw new IllegalStateException("Iterator not initialized");
      }
      return nextLine != null;
    }

    @Override
    public float[] next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      String[] fields = parseCsvLine(nextLine);
      String jsonArrayStr = fields[vectorColumn].trim();
      float[] vector = parseJsonFloatArray(jsonArrayStr);

      // Advance to the next valid line
      advance();

      return vector;
    }

    private void close() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // Ignore
        }
      }
    }

  }
}
