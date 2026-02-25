package io.nosqlbench.command.fetch.subcommands.dlhf;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("DatasetDownloader HTTP error message formatting")
class DatasetDownloaderErrorMessageTest {

  @Test
  @DisplayName("should include parsed JSON error payload details")
  void shouldIncludeParsedJsonErrorDetails() {
    String payload = "{\"error\":\"Repository Not Found\",\"message\":\"Dataset is private\"}";

    String message = DatasetDownloader.formatHttpErrorMessage(
        404,
        "Not Found",
        "downloading",
        "config/train.parquet",
        payload
    );

    assertThat(message).contains("HTTP 404 Not Found");
    assertThat(message).contains("downloading config/train.parquet");
    assertThat(message).contains("Repository Not Found");
    assertThat(message).contains("Dataset is private");
  }

  @Test
  @DisplayName("should normalize plain text payload whitespace")
  void shouldNormalizePlainTextPayloadWhitespace() {
    String payload = "  access denied\n\nfor this dataset\t\tplease request access  ";

    String summary = DatasetDownloader.summarizeErrorPayload(payload);

    assertThat(summary).isEqualTo("access denied for this dataset please request access");
  }

  @Test
  @DisplayName("should extract nested message fields from JSON payload")
  void shouldExtractNestedMessageFieldsFromJson() {
    String payload = "{\"error\":{\"message\":\"Access to this dataset is restricted\"}}";

    String summary = DatasetDownloader.summarizeErrorPayload(payload);

    assertThat(summary).contains("Access to this dataset is restricted");
  }

  @Test
  @DisplayName("should truncate very large payloads")
  void shouldTruncateVeryLargePayloads() {
    String payload = "x".repeat(4000);

    String summary = DatasetDownloader.summarizeErrorPayload(payload);

    assertThat(summary).contains("[truncated]");
    assertThat(summary.length()).isLessThan(900);
  }
}
