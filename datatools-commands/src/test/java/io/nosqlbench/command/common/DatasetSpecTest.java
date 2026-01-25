/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

@DisplayName("DatasetSpec")
class DatasetSpecTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("Parsing - Catalog Specs")
    class CatalogParsingTest {

        @Test
        @DisplayName("should parse simple dataset name as CATALOG")
        void shouldParseSimpleDatasetName() {
            DatasetSpec spec = DatasetSpec.parse("sift-128");

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.CATALOG);
            assertThat(spec.getDatasetName()).contains("sift-128");
            assertThat(spec.isCatalog()).isTrue();
            assertThat(spec.isLocal()).isFalse();
            assertThat(spec.isRemote()).isFalse();
        }

        @ParameterizedTest
        @ValueSource(strings = {
            "sift-128",
            "glove-100",
            "my_dataset",
            "dataset.v2",
            "Dataset123",
            "a",
            "a1"
        })
        @DisplayName("should accept valid catalog dataset names")
        void shouldAcceptValidDatasetNames(String name) {
            DatasetSpec spec = DatasetSpec.parse(name);

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.CATALOG);
            assertThat(spec.getDatasetName()).contains(name);
        }

        @Test
        @DisplayName("should reject invalid dataset names")
        void shouldRejectInvalidDatasetNames() {
            assertThatThrownBy(() -> DatasetSpec.parse("-invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid dataset name");

            assertThatThrownBy(() -> DatasetSpec.parse("_invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid dataset name");

            assertThatThrownBy(() -> DatasetSpec.parse(".invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid dataset name");
        }
    }

    @Nested
    @DisplayName("Parsing - Local Directory Specs")
    class LocalDirectoryParsingTest {

        @Test
        @DisplayName("should parse existing directory with dataset.yaml as LOCAL_DIRECTORY")
        void shouldParseDirectoryWithDatasetYaml() throws Exception {
            Path datasetDir = tempDir.resolve("mydata");
            Files.createDirectory(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yaml"));

            DatasetSpec spec = DatasetSpec.parse(datasetDir.toString());

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.LOCAL_DIRECTORY);
            assertThat(spec.getLocalPath()).contains(datasetDir);
            assertThat(spec.isLocalDirectory()).isTrue();
            assertThat(spec.isLocal()).isTrue();
        }

        @Test
        @DisplayName("should parse relative directory path with dataset.yaml")
        void shouldParseRelativeDirectoryPath() throws Exception {
            Path datasetDir = tempDir.resolve("relative-data");
            Files.createDirectory(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yaml"));

            // Use full path (relative paths are relative to cwd, not tempDir)
            DatasetSpec spec = DatasetSpec.parse(datasetDir.toString());

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.LOCAL_DIRECTORY);
            assertThat(spec.isLocalDirectory()).isTrue();
        }

        @Test
        @DisplayName("should accept dataset.yml as well as dataset.yaml")
        void shouldAcceptDatasetYml() throws Exception {
            Path datasetDir = tempDir.resolve("yml-data");
            Files.createDirectory(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yml"));

            DatasetSpec spec = DatasetSpec.parse(datasetDir.toString());

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.LOCAL_DIRECTORY);
            assertThat(spec.isLocalDirectory()).isTrue();
        }

        @Test
        @DisplayName("should reject directory without dataset.yaml")
        void shouldRejectDirectoryWithoutDatasetYaml() throws Exception {
            Path emptyDir = tempDir.resolve("empty-dir");
            Files.createDirectory(emptyDir);

            assertThatThrownBy(() -> DatasetSpec.parse(emptyDir.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not contain a dataset.yaml");
        }

        @Test
        @DisplayName("getDatasetYamlPath should return correct path for directory")
        void getDatasetYamlPathShouldWork() throws Exception {
            Path datasetDir = tempDir.resolve("data-with-yaml");
            Files.createDirectory(datasetDir);
            Path yamlPath = datasetDir.resolve("dataset.yaml");
            Files.createFile(yamlPath);

            DatasetSpec spec = DatasetSpec.parse(datasetDir.toString());

            assertThat(spec.getDatasetYamlPath()).contains(yamlPath);
            assertThat(spec.getDatasetDirectory()).contains(datasetDir);
        }

        @Test
        @DisplayName("should expand ~ in directory paths when directory exists")
        void shouldExpandTildeInDirectoryPaths() throws Exception {
            // Create a directory in tempDir to test with
            Path datasetDir = tempDir.resolve("tilde-test");
            Files.createDirectory(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yaml"));

            DatasetSpec spec = DatasetSpec.parse(datasetDir.toString());

            // Verify path expansion worked (no ~ in result)
            assertThat(spec.getLocalPath()).isPresent();
            assertThat(spec.getLocalPath().get().toString()).doesNotContain("~");
        }
    }

    @Nested
    @DisplayName("Parsing - Local File Specs")
    class LocalFileParsingTest {

        @Test
        @DisplayName("should parse direct path to dataset.yaml as LOCAL_FILE")
        void shouldParseDirectPathToDatasetYaml() throws Exception {
            Path datasetDir = tempDir.resolve("file-data");
            Files.createDirectory(datasetDir);
            Path yamlPath = datasetDir.resolve("dataset.yaml");
            Files.createFile(yamlPath);

            DatasetSpec spec = DatasetSpec.parse(yamlPath.toString());

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.LOCAL_FILE);
            assertThat(spec.getLocalPath()).contains(yamlPath);
            assertThat(spec.isLocalFile()).isTrue();
            assertThat(spec.isLocal()).isTrue();
        }

        @Test
        @DisplayName("should parse path to dataset.yml")
        void shouldParsePathToDatasetYml() throws Exception {
            Path datasetDir = tempDir.resolve("yml-file-data");
            Files.createDirectory(datasetDir);
            Path ymlPath = datasetDir.resolve("dataset.yml");
            Files.createFile(ymlPath);

            DatasetSpec spec = DatasetSpec.parse(ymlPath.toString());

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.LOCAL_FILE);
            assertThat(spec.getLocalPath()).contains(ymlPath);
        }

        @Test
        @DisplayName("should reject non-existent dataset.yaml file")
        void shouldRejectNonExistentFile() {
            Path nonExistent = tempDir.resolve("nonexistent/dataset.yaml");

            assertThatThrownBy(() -> DatasetSpec.parse(nonExistent.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
        }

        @Test
        @DisplayName("getDatasetDirectory should return parent for file spec")
        void getDatasetDirectoryShouldReturnParent() throws Exception {
            Path datasetDir = tempDir.resolve("parent-test");
            Files.createDirectory(datasetDir);
            Path yamlPath = datasetDir.resolve("dataset.yaml");
            Files.createFile(yamlPath);

            DatasetSpec spec = DatasetSpec.parse(yamlPath.toString());

            assertThat(spec.getDatasetYamlPath()).contains(yamlPath);
            assertThat(spec.getDatasetDirectory()).contains(datasetDir);
        }
    }

    @Nested
    @DisplayName("Parsing - Remote Specs")
    class RemoteParsingTest {

        @Test
        @DisplayName("should parse URL ending with / as REMOTE_BASE")
        void shouldParseBaseUrl() {
            DatasetSpec spec = DatasetSpec.parse("https://example.com/datasets/sift/");

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_BASE);
            assertThat(spec.getRemoteUri()).isPresent();
            assertThat(spec.isRemoteBase()).isTrue();
            assertThat(spec.isRemote()).isTrue();
        }

        @Test
        @DisplayName("should parse URL ending with dataset.yaml as REMOTE_YAML")
        void shouldParseYamlUrl() {
            DatasetSpec spec = DatasetSpec.parse("https://example.com/datasets/sift/dataset.yaml");

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_YAML);
            assertThat(spec.getRemoteUri()).isPresent();
            assertThat(spec.isRemoteYaml()).isTrue();
            assertThat(spec.isRemote()).isTrue();
        }

        @Test
        @DisplayName("should parse URL ending with dataset.yml as REMOTE_YAML")
        void shouldParseYmlUrl() {
            DatasetSpec spec = DatasetSpec.parse("https://example.com/datasets/sift/dataset.yml");

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_YAML);
        }

        @Test
        @DisplayName("should parse HTTP URL as well as HTTPS")
        void shouldParseHttpUrl() {
            DatasetSpec spec = DatasetSpec.parse("http://example.com/datasets/sift/");

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_BASE);
            assertThat(spec.isRemote()).isTrue();
        }

        @Test
        @DisplayName("should parse URL without trailing / as REMOTE_BASE")
        void shouldParseUrlWithoutTrailingSlash() {
            DatasetSpec spec = DatasetSpec.parse("https://example.com/datasets/sift");

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_BASE);
            assertThat(spec.isRemoteBase()).isTrue();
            assertThat(spec.isRemote()).isTrue();
        }

        @Test
        @DisplayName("getDatasetYamlUrl should work for REMOTE_BASE without trailing slash")
        void getDatasetYamlUrlShouldWorkForBaseWithoutSlash() {
            DatasetSpec spec = DatasetSpec.parse("https://example.com/datasets/sift");

            assertThat(spec.getDatasetYamlUrl())
                .contains("https://example.com/datasets/sift/dataset.yaml");
            assertThat(spec.getDatasetBaseUrl())
                .contains("https://example.com/datasets/sift/");
        }

        @Test
        @DisplayName("getDatasetYamlUrl should work for REMOTE_BASE")
        void getDatasetYamlUrlShouldWorkForBase() {
            DatasetSpec spec = DatasetSpec.parse("https://example.com/datasets/sift/");

            assertThat(spec.getDatasetYamlUrl())
                .contains("https://example.com/datasets/sift/dataset.yaml");
            assertThat(spec.getDatasetBaseUrl())
                .contains("https://example.com/datasets/sift/");
        }

        @Test
        @DisplayName("getDatasetBaseUrl should work for REMOTE_YAML")
        void getDatasetBaseUrlShouldWorkForYaml() {
            DatasetSpec spec = DatasetSpec.parse("https://example.com/datasets/sift/dataset.yaml");

            assertThat(spec.getDatasetYamlUrl())
                .contains("https://example.com/datasets/sift/dataset.yaml");
            assertThat(spec.getDatasetBaseUrl())
                .contains("https://example.com/datasets/sift/");
        }
    }

    @Nested
    @DisplayName("Parsing - Error Cases")
    class ErrorCasesTest {

        @Test
        @DisplayName("should reject null input")
        void shouldRejectNull() {
            assertThatThrownBy(() -> DatasetSpec.parse(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
        }

        @Test
        @DisplayName("should reject empty input")
        void shouldRejectEmpty() {
            assertThatThrownBy(() -> DatasetSpec.parse(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
        }

        @Test
        @DisplayName("should reject whitespace-only input")
        void shouldRejectWhitespace() {
            assertThatThrownBy(() -> DatasetSpec.parse("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
        }
    }

    @Nested
    @DisplayName("Helper Methods")
    class HelperMethodsTest {

        @Test
        @DisplayName("toString should return raw spec")
        void toStringShouldReturnRawSpec() {
            String rawSpec = "sift-128";
            DatasetSpec spec = DatasetSpec.parse(rawSpec);

            assertThat(spec.toString()).isEqualTo(rawSpec);
        }

        @Test
        @DisplayName("toDescription should be human readable")
        void toDescriptionShouldBeReadable() throws Exception {
            DatasetSpec catalogSpec = DatasetSpec.parse("sift-128");
            assertThat(catalogSpec.toDescription()).contains("Catalog dataset");

            Path datasetDir = tempDir.resolve("desc-test");
            Files.createDirectory(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yaml"));
            DatasetSpec localSpec = DatasetSpec.parse(datasetDir.toString());
            assertThat(localSpec.toDescription()).contains("Local directory");

            DatasetSpec remoteSpec = DatasetSpec.parse("https://example.com/datasets/sift/");
            assertThat(remoteSpec.toDescription()).contains("Remote dataset");
        }

        @Test
        @DisplayName("equals and hashCode should work correctly")
        void equalsAndHashCodeShouldWork() {
            DatasetSpec spec1 = DatasetSpec.parse("sift-128");
            DatasetSpec spec2 = DatasetSpec.parse("sift-128");
            DatasetSpec spec3 = DatasetSpec.parse("glove-100");

            assertThat(spec1).isEqualTo(spec2);
            assertThat(spec1.hashCode()).isEqualTo(spec2.hashCode());
            assertThat(spec1).isNotEqualTo(spec3);
        }
    }

    @Nested
    @DisplayName("Picocli Converter Integration")
    class PicocliConverterTest {

        @Test
        @DisplayName("should convert string to DatasetSpec")
        void shouldConvertString() throws Exception {
            DatasetSpecConverter converter = new DatasetSpecConverter();

            DatasetSpec spec = converter.convert("sift-128");

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.CATALOG);
        }

        @Test
        @DisplayName("should throw TypeConversionException for invalid input")
        void shouldThrowConversionException() {
            DatasetSpecConverter converter = new DatasetSpecConverter();

            assertThatThrownBy(() -> converter.convert("-invalid"))
                .isInstanceOf(CommandLine.TypeConversionException.class);
        }

        @Test
        @DisplayName("should work with picocli command parsing")
        void shouldWorkWithCommandParsing() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--dataset", "sift-128");

            assertThat(command.dataset).isNotNull();
            assertThat(command.dataset.getSourceType()).isEqualTo(DatasetSpec.SourceType.CATALOG);
        }

        @Test
        @DisplayName("should work with picocli command parsing for remote URL")
        void shouldWorkWithRemoteUrl() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--dataset", "https://example.com/datasets/sift/");

            assertThat(command.dataset).isNotNull();
            assertThat(command.dataset.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_BASE);
        }

        @Test
        @DisplayName("should work with picocli command parsing for remote URL without trailing slash")
        void shouldWorkWithRemoteUrlWithoutTrailingSlash() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--dataset", "https://example.com/datasets/sift");

            assertThat(command.dataset).isNotNull();
            assertThat(command.dataset.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_BASE);
            assertThat(command.dataset.getDatasetYamlUrl())
                .contains("https://example.com/datasets/sift/dataset.yaml");
        }
    }

    @Nested
    @DisplayName("Jetty Server Integration")
    @ExtendWith(JettyFileServerExtension.class)
    class JettyServerIntegrationTest {

        @Test
        @DisplayName("should parse base URL from Jetty test server")
        void shouldParseJettyBaseUrl() {
            String baseUrl = JettyFileServerExtension.getBaseUrl().toString();
            String datasetUrl = baseUrl + "rawdatasets/testxvec/";

            DatasetSpec spec = DatasetSpec.parse(datasetUrl);

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_BASE);
            assertThat(spec.getDatasetYamlUrl()).isPresent();
            assertThat(spec.getDatasetYamlUrl().get()).endsWith("dataset.yaml");
        }

        @Test
        @DisplayName("should parse base URL without trailing slash from Jetty test server")
        void shouldParseJettyBaseUrlWithoutTrailingSlash() {
            String baseUrl = JettyFileServerExtension.getBaseUrl().toString();
            // Remove trailing slash if present from base, then add path without trailing slash
            String base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
            String datasetUrl = base + "/rawdatasets/testxvec";

            DatasetSpec spec = DatasetSpec.parse(datasetUrl);

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_BASE);
            assertThat(spec.getDatasetYamlUrl()).isPresent();
            assertThat(spec.getDatasetYamlUrl().get()).endsWith("testxvec/dataset.yaml");
        }

        @Test
        @DisplayName("should parse dataset.yaml URL from Jetty test server")
        void shouldParseJettyYamlUrl() {
            String baseUrl = JettyFileServerExtension.getBaseUrl().toString();
            String datasetYamlUrl = baseUrl + "rawdatasets/testxvec/dataset.yaml";

            DatasetSpec spec = DatasetSpec.parse(datasetYamlUrl);

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.REMOTE_YAML);
            assertThat(spec.getDatasetYamlUrl()).contains(datasetYamlUrl);
            assertThat(spec.getDatasetBaseUrl()).isPresent();
            assertThat(spec.getDatasetBaseUrl().get()).endsWith("/");
        }
    }

    @Nested
    @DisplayName("Resolution Priority")
    class ResolutionPriorityTest {

        @Test
        @DisplayName("existing local directory should take priority over catalog name")
        void existingDirectoryShouldTakePriority() throws Exception {
            // Create a directory with a name that could be a catalog dataset
            Path datasetDir = tempDir.resolve("sift-128");
            Files.createDirectory(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yaml"));

            DatasetSpec spec = DatasetSpec.parse(datasetDir.toString());

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.LOCAL_DIRECTORY);
        }

        @Test
        @DisplayName("path with slashes should be treated as local path")
        void pathWithSlashesShouldBeLocal() throws Exception {
            Path datasetDir = tempDir.resolve("nested").resolve("dataset");
            Files.createDirectories(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yaml"));

            DatasetSpec spec = DatasetSpec.parse(datasetDir.toString());

            assertThat(spec.isLocal()).isTrue();
        }

        @Test
        @DisplayName("name without path separators and no local match should be CATALOG")
        void nameWithoutSeparatorsShouldBeCatalog() {
            DatasetSpec spec = DatasetSpec.parse("nonexistent-dataset-name");

            assertThat(spec.getSourceType()).isEqualTo(DatasetSpec.SourceType.CATALOG);
        }
    }

    /// Dummy command for picocli integration testing
    private static final class DummyCommand implements Runnable {
        @CommandLine.Option(names = {"--dataset"}, converter = DatasetSpecConverter.class)
        DatasetSpec dataset;

        @Override
        public void run() {
        }
    }
}
