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
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

@DisplayName("VectorDataSpec")
class VectorDataSpecTest {

    @TempDir
    Path tempDir;

    @Nested
    @DisplayName("Parsing - Local File Specs")
    class LocalFileParsingTest {

        @Test
        @DisplayName("should parse unqualified path as LOCAL_FILE")
        void shouldParseUnqualifiedPath() {
            VectorDataSpec spec = VectorDataSpec.parse("./data/vectors.fvec");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
            assertThat(spec.getLocalPath()).isPresent();
            assertThat(spec.getLocalPath().get().toString()).endsWith("data/vectors.fvec");
            assertThat(spec.isLocalFile()).isTrue();
            assertThat(spec.isFacet()).isFalse();
            assertThat(spec.isRemote()).isFalse();
        }

        @Test
        @DisplayName("should parse file: prefixed path as LOCAL_FILE")
        void shouldParseFilePrefixedPath() {
            VectorDataSpec spec = VectorDataSpec.parse("file:./data/vectors.fvec");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
            assertThat(spec.getLocalPath()).isPresent();
            assertThat(spec.getLocalPath().get().toString()).endsWith("data/vectors.fvec");
        }

        @Test
        @DisplayName("should expand ~ in paths")
        void shouldExpandTilde() {
            VectorDataSpec spec = VectorDataSpec.parse("~/vectors.fvec");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
            assertThat(spec.getLocalPath()).isPresent();
            assertThat(spec.getLocalPath().get().toString()).startsWith(System.getProperty("user.home"));
        }

        @Test
        @DisplayName("should expand ${HOME} in paths")
        void shouldExpandHomeVariable() {
            VectorDataSpec spec = VectorDataSpec.parse("${HOME}/vectors.fvec");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
            assertThat(spec.getLocalPath()).isPresent();
            assertThat(spec.getLocalPath().get().toString()).startsWith(System.getProperty("user.home"));
        }

        @ParameterizedTest
        @ValueSource(strings = {
            "vectors.fvec",
            "./vectors.fvec",
            "/absolute/path/vectors.fvec",
            "file:vectors.fvec",
            "file:./vectors.fvec"
        })
        @DisplayName("should parse various local file formats")
        void shouldParseVariousLocalFileFormats(String input) {
            VectorDataSpec spec = VectorDataSpec.parse(input);

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
            assertThat(spec.isLocalFile()).isTrue();
        }
    }

    @Nested
    @DisplayName("Parsing - Facet Specs")
    class FacetParsingTest {

        @Test
        @DisplayName("should parse catalog facet spec with facet: prefix")
        void shouldParseCatalogFacetSpec() {
            VectorDataSpec spec = VectorDataSpec.parse("facet:sift-128:default:base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
            assertThat(spec.getDatasetRef()).contains("sift-128");
            assertThat(spec.getProfileName()).contains("default");
            assertThat(spec.getFacetKind()).contains(TestDataKind.base_vectors);
            assertThat(spec.isCatalogFacet()).isTrue();
            assertThat(spec.isFacet()).isTrue();
        }

        @Test
        @DisplayName("should parse catalog facet spec with facet. prefix using dots")
        void shouldParseCatalogFacetSpecWithDots() {
            VectorDataSpec spec = VectorDataSpec.parse("facet.sift-128.default.base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
            assertThat(spec.getDatasetRef()).contains("sift-128");
            assertThat(spec.getProfileName()).contains("default");
            assertThat(spec.getFacetKind()).contains(TestDataKind.base_vectors);
            assertThat(spec.isCatalogFacet()).isTrue();
            assertThat(spec.isFacet()).isTrue();
        }

        @Test
        @DisplayName("should parse catalog facet spec without facet: prefix (shorthand)")
        void shouldParseCatalogFacetSpecShorthand() {
            VectorDataSpec spec = VectorDataSpec.parse("sift-128:default:base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
            assertThat(spec.getDatasetRef()).contains("sift-128");
            assertThat(spec.getProfileName()).contains("default");
            assertThat(spec.getFacetKind()).contains(TestDataKind.base_vectors);
            assertThat(spec.isCatalogFacet()).isTrue();
            assertThat(spec.isFacet()).isTrue();
        }

        @Test
        @DisplayName("should parse catalog facet spec shorthand using dots")
        void shouldParseCatalogFacetSpecShorthandWithDots() {
            VectorDataSpec spec = VectorDataSpec.parse("sift-128.default.base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
            assertThat(spec.getDatasetRef()).contains("sift-128");
            assertThat(spec.getProfileName()).contains("default");
            assertThat(spec.getFacetKind()).contains(TestDataKind.base_vectors);
            assertThat(spec.isCatalogFacet()).isTrue();
            assertThat(spec.isFacet()).isTrue();
        }

        @Test
        @DisplayName("should parse various shorthand facet formats")
        void shouldParseShorthandFacetFormats() {
            // Various dataset names with profile and facet
            VectorDataSpec spec1 = VectorDataSpec.parse("my-dataset:profile1:query");
            assertThat(spec1.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
            assertThat(spec1.getDatasetRef()).contains("my-dataset");
            assertThat(spec1.getFacetKind()).contains(TestDataKind.query_vectors);

            VectorDataSpec spec2 = VectorDataSpec.parse("dataset_name:test_profile:indices");
            assertThat(spec2.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
            assertThat(spec2.getDatasetRef()).contains("dataset_name");
            assertThat(spec2.getFacetKind()).contains(TestDataKind.neighbor_indices);

            VectorDataSpec spec3 = VectorDataSpec.parse("dotty.profile.query");
            assertThat(spec3.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
            assertThat(spec3.getDatasetRef()).contains("dotty");
            assertThat(spec3.getFacetKind()).contains(TestDataKind.query_vectors);
        }

        @Test
        @DisplayName("should parse local facet spec when directory exists with dataset.yaml")
        void shouldParseLocalFacetSpec() throws Exception {
            // Create a temp directory with dataset.yaml to simulate a local dataset
            Path datasetDir = tempDir.resolve("my-dataset");
            Files.createDirectory(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yaml"));

            VectorDataSpec spec = VectorDataSpec.parse("facet:" + datasetDir + ":default:query");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FACET);
            assertThat(spec.getLocalPath()).contains(datasetDir);
            assertThat(spec.getProfileName()).contains("default");
            assertThat(spec.getFacetKind()).contains(TestDataKind.query_vectors);
            assertThat(spec.isLocalFacet()).isTrue();
            assertThat(spec.isFacet()).isTrue();
        }

        @Test
        @DisplayName("should treat directory without dataset.yaml as catalog reference")
        void shouldTreatDirectoryWithoutDatasetYamlAsCatalog() throws Exception {
            // Create a temp directory WITHOUT dataset.yaml
            Path datasetDir = tempDir.resolve("no-dataset-yaml");
            Files.createDirectory(datasetDir);

            // This should be treated as CATALOG_FACET, not LOCAL_FACET
            VectorDataSpec spec = VectorDataSpec.parse("facet:" + datasetDir.getFileName() + ":default:base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
        }

        @Test
        @DisplayName("should prioritize local directory with dataset.yaml over catalog for shorthand format")
        void shouldPrioritizeLocalDirectoryOverCatalog() throws Exception {
            // Create a temp directory with dataset.yaml that could be a catalog dataset
            Path datasetDir = tempDir.resolve("local-dataset");
            Files.createDirectory(datasetDir);
            Files.createFile(datasetDir.resolve("dataset.yaml"));

            // Using shorthand format with existing directory containing dataset.yaml
            VectorDataSpec spec = VectorDataSpec.parse(datasetDir + ":default:base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FACET);
            assertThat(spec.getLocalPath()).contains(datasetDir);
        }

        @ParameterizedTest
        @CsvSource({
            "base, base_vectors",
            "query, query_vectors",
            "indices, neighbor_indices",
            "distances, neighbor_distances",
            "base_vectors, base_vectors",
            "query_vectors, query_vectors",
            "neighbor_indices, neighbor_indices",
            "neighbor_distances, neighbor_distances",
            "train, base_vectors",
            "test, query_vectors",
            "neighbors, neighbor_indices",
            "gt, neighbor_indices"
        })
        @DisplayName("should accept facet aliases")
        void shouldAcceptFacetAliases(String input, String expected) {
            VectorDataSpec spec = VectorDataSpec.parse("facet:dataset:profile:" + input);

            assertThat(spec.getFacetKind()).isPresent();
            assertThat(spec.getFacetKind().get().name()).isEqualTo(expected);
        }

        @ParameterizedTest
        @CsvSource({
            "base, base_vectors",
            "query, query_vectors",
            "indices, neighbor_indices",
            "distances, neighbor_distances"
        })
        @DisplayName("should accept facet aliases in shorthand format")
        void shouldAcceptFacetAliasesInShorthand(String input, String expected) {
            VectorDataSpec spec = VectorDataSpec.parse("dataset:profile:" + input);

            assertThat(spec.getFacetKind()).isPresent();
            assertThat(spec.getFacetKind().get().name()).isEqualTo(expected);
        }

        @Test
        @DisplayName("should accept facet aliases in dot shorthand format")
        void shouldAcceptFacetAliasesInDotShorthand() {
            VectorDataSpec spec = VectorDataSpec.parse("dataset.profile.base");

            assertThat(spec.getFacetKind()).isPresent();
            assertThat(spec.getFacetKind().get().name()).isEqualTo("base_vectors");
        }

        @Test
        @DisplayName("should reject invalid facet format")
        void shouldRejectInvalidFacetFormat() {
            assertThatThrownBy(() -> VectorDataSpec.parse("facet:only-one-part"))
                .isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> VectorDataSpec.parse("facet:two:parts"))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("should reject empty facet components")
        void shouldRejectEmptyFacetComponents() {
            assertThatThrownBy(() -> VectorDataSpec.parse("facet::profile:base"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");

            assertThatThrownBy(() -> VectorDataSpec.parse("facet:dataset::base"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");

            assertThatThrownBy(() -> VectorDataSpec.parse("facet:dataset:profile:"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
        }

        @Test
        @DisplayName("should reject unknown facet names")
        void shouldRejectUnknownFacetNames() {
            assertThatThrownBy(() -> VectorDataSpec.parse("facet:dataset:profile:unknown"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown facet");
        }

        @Test
        @DisplayName("shorthand with unknown facet should be treated as local file")
        void shorthandWithUnknownFacetShouldBeLocalFile() {
            // "dataset:profile:unknown" - unknown is not a valid facet, so it's a local file
            VectorDataSpec spec = VectorDataSpec.parse("dataset:profile:notafacet");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
        }

        @Test
        @DisplayName("dot shorthand with unknown facet should be treated as local file")
        void dotShorthandWithUnknownFacetShouldBeLocalFile() {
            VectorDataSpec spec = VectorDataSpec.parse("dataset.profile.notafacet");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
        }
    }

    @Nested
    @DisplayName("Parsing - Remote Specs")
    class RemoteParsingTest {

        @Test
        @DisplayName("should parse HTTP URL as REMOTE_FILE")
        void shouldParseHttpUrl() {
            VectorDataSpec spec = VectorDataSpec.parse("http://example.com/vectors.fvec");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.REMOTE_FILE);
            assertThat(spec.getRemoteUri()).isPresent();
            assertThat(spec.getRemoteUri().get().toString()).isEqualTo("http://example.com/vectors.fvec");
            assertThat(spec.isRemoteFile()).isTrue();
            assertThat(spec.isRemote()).isTrue();
        }

        @Test
        @DisplayName("should parse HTTPS URL as REMOTE_FILE")
        void shouldParseHttpsUrl() {
            VectorDataSpec spec = VectorDataSpec.parse("https://example.com/data/vectors.fvec");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.REMOTE_FILE);
            assertThat(spec.isRemoteFile()).isTrue();
        }

        @Test
        @DisplayName("should reject URL with trailing slash as ambiguous")
        void shouldRejectDatasetBaseUrl() {
            assertThatThrownBy(() -> VectorDataSpec.parse("https://example.com/datasets/sift/"))
                .isInstanceOf(VectorDataSpec.AmbiguousDatasetBaseException.class)
                .hasMessageContaining("ambiguous");
        }

        @Test
        @DisplayName("AmbiguousDatasetBaseException should provide base URL info")
        void ambiguousExceptionShouldProvideInfo() {
            String baseUrl = "https://example.com/datasets/sift/";

            try {
                VectorDataSpec.parse(baseUrl);
                fail("Expected AmbiguousDatasetBaseException");
            } catch (VectorDataSpec.AmbiguousDatasetBaseException e) {
                assertThat(e.getBaseUrl()).isEqualTo(baseUrl);
                assertThat(e.getDatasetYamlUrl()).isEqualTo(baseUrl + "dataset.yaml");
            }
        }

        @Test
        @DisplayName("should parse explicit dataset.yaml URL as REMOTE_DATASET_YAML")
        void shouldParseDatasetYamlUrl() {
            VectorDataSpec spec = VectorDataSpec.parse("https://example.com/datasets/sift/dataset.yaml");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.REMOTE_DATASET_YAML);
            assertThat(spec.isRemoteDataset()).isTrue();
            assertThat(spec.getDatasetYamlUrl()).contains("https://example.com/datasets/sift/dataset.yaml");
            assertThat(spec.getDatasetBaseUrl()).contains("https://example.com/datasets/sift/");
        }

        @Test
        @DisplayName("should parse dataset.yml URL as REMOTE_DATASET_YAML")
        void shouldParseDatasetYmlUrl() {
            VectorDataSpec spec = VectorDataSpec.parse("https://example.com/datasets/sift/dataset.yml");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.REMOTE_DATASET_YAML);
            assertThat(spec.getDatasetBaseUrl()).contains("https://example.com/datasets/sift/");
        }
    }

    @Nested
    @DisplayName("Parsing - Incomplete Dataset Spec")
    class IncompleteDatasetSpecTest {

        @Test
        @DisplayName("should reject dataset:profile format as incomplete")
        void shouldRejectIncompleteDatasetSpec() {
            assertThatThrownBy(() -> VectorDataSpec.parse("sift-128:default"))
                .isInstanceOf(VectorDataSpec.IncompleteDatasetSpecException.class)
                .hasMessageContaining("Incomplete vector data spec")
                .hasMessageContaining("sift-128:default")
                .hasMessageContaining("Did you mean")
                .hasMessageContaining("sift-128.default.base");
        }

        @Test
        @DisplayName("should preserve dot form in error message")
        void shouldPreserveDotFormInErrorMessage() {
            assertThatThrownBy(() -> VectorDataSpec.parse("sift-128.default"))
                .isInstanceOf(VectorDataSpec.IncompleteDatasetSpecException.class)
                .hasMessageContaining("sift-128.default")
                .hasMessageContaining("sift-128.default.base");
        }

        @Test
        @DisplayName("should provide helpful error message with all facet options")
        void shouldProvideHelpfulErrorMessage() {
            try {
                VectorDataSpec.parse("my-dataset:profile1");
                fail("Expected IncompleteDatasetSpecException");
            } catch (VectorDataSpec.IncompleteDatasetSpecException e) {
                assertThat(e.getMessage())
                    .contains("my-dataset.profile1.base")
                    .contains("my-dataset.profile1.query")
                    .contains("my-dataset.profile1.indices")
                    .contains("my-dataset.profile1.distances");
                assertThat(e.getDatasetName()).isEqualTo("my-dataset");
                assertThat(e.getProfileName()).isEqualTo("profile1");
            }
        }

        @Test
        @DisplayName("should throw TypeConversionException via converter for incomplete spec")
        void converterShouldThrowForIncompleteSpec() {
            VectorDataSpecConverter converter = new VectorDataSpecConverter();

            assertThatThrownBy(() -> converter.convert("sift-128:default"))
                .isInstanceOf(CommandLine.TypeConversionException.class)
                .hasMessageContaining("sift-128.default.base")
                .hasMessageContaining("sift-128.default.query");
        }

        @ParameterizedTest
        @ValueSource(strings = {
            "sift-128:default",
            "my_dataset:some_profile",
            "dataset.name:profile.name",
            "UPPERCASE:PROFILE",
            "sift-128.default"
        })
        @DisplayName("should detect various incomplete dataset:profile formats")
        void shouldDetectVariousIncompleteFormats(String spec) {
            assertThatThrownBy(() -> VectorDataSpec.parse(spec))
                .isInstanceOf(VectorDataSpec.IncompleteDatasetSpecException.class);
        }

        @Test
        @DisplayName("should NOT treat file paths with single colon as incomplete spec")
        void shouldNotTreatFilePathsAsIncompleteSpec() {
            // These should be treated as local files, not incomplete specs
            // ./path:something - contains path separator
            VectorDataSpec spec1 = VectorDataSpec.parse("./path:something");
            assertThat(spec1.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);

            // ~/data:name - starts with ~
            VectorDataSpec spec2 = VectorDataSpec.parse("~/data:name");
            assertThat(spec2.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
        }

        @Test
        @DisplayName("should NOT treat existing paths as incomplete spec")
        void shouldNotTreatExistingPathsAsIncompleteSpec() throws Exception {
            // Create a file with a colon-like name (on filesystems that support it)
            Path existingFile = tempDir.resolve("dataset");
            Files.createFile(existingFile);

            // The path exists, so it should be treated as a file, not incomplete spec
            VectorDataSpec spec = VectorDataSpec.parse(existingFile.toString());
            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
        }

        @Test
        @DisplayName("should NOT treat dataset.yaml or dataset.yml as incomplete spec")
        void shouldNotTreatDatasetYamlAsIncompleteSpec() {
            VectorDataSpec yamlSpec = VectorDataSpec.parse("dataset.yaml");
            assertThat(yamlSpec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);

            VectorDataSpec ymlSpec = VectorDataSpec.parse("dataset.yml");
            assertThat(ymlSpec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.LOCAL_FILE);
        }
    }

    @Nested
    @DisplayName("Parsing - Error Cases")
    class ErrorCasesTest {

        @Test
        @DisplayName("should reject null input")
        void shouldRejectNull() {
            assertThatThrownBy(() -> VectorDataSpec.parse(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
        }

        @Test
        @DisplayName("should reject empty input")
        void shouldRejectEmpty() {
            assertThatThrownBy(() -> VectorDataSpec.parse(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
        }

        @Test
        @DisplayName("should reject whitespace-only input")
        void shouldRejectWhitespace() {
            assertThatThrownBy(() -> VectorDataSpec.parse("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
        }

        @Test
        @DisplayName("should reject empty file: prefix")
        void shouldRejectEmptyFilePrefix() {
            assertThatThrownBy(() -> VectorDataSpec.parse("file:"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a path");
        }
    }

    @Nested
    @DisplayName("Helper Methods")
    class HelperMethodsTest {

        @Test
        @DisplayName("toString should return raw spec")
        void toStringShouldReturnRawSpec() {
            String rawSpec = "facet.sift-128.default.base";
            VectorDataSpec spec = VectorDataSpec.parse(rawSpec);

            assertThat(spec.toString()).isEqualTo(rawSpec);
        }

        @Test
        @DisplayName("toString should return raw spec for shorthand format")
        void toStringShouldReturnRawSpecForShorthand() {
            String rawSpec = "sift-128:default:base";
            VectorDataSpec spec = VectorDataSpec.parse(rawSpec);

            assertThat(spec.toString()).isEqualTo(rawSpec);
        }

        @Test
        @DisplayName("toDescription should be human readable")
        void toDescriptionShouldBeReadable() {
            VectorDataSpec localFile = VectorDataSpec.parse("./vectors.fvec");
            assertThat(localFile.toDescription()).contains("Local file");

            VectorDataSpec catalogFacet = VectorDataSpec.parse("facet.sift-128.default.base");
            assertThat(catalogFacet.toDescription()).contains("Catalog facet");

            VectorDataSpec shorthandFacet = VectorDataSpec.parse("sift-128:default:base");
            assertThat(shorthandFacet.toDescription()).contains("Catalog facet");

            VectorDataSpec remoteFile = VectorDataSpec.parse("https://example.com/vectors.fvec");
            assertThat(remoteFile.toDescription()).contains("Remote file");
        }

        @Test
        @DisplayName("equals and hashCode should work correctly")
        void equalsAndHashCodeShouldWork() {
            VectorDataSpec spec1 = VectorDataSpec.parse("facet.sift-128.default.base");
            VectorDataSpec spec2 = VectorDataSpec.parse("facet.sift-128.default.base");
            VectorDataSpec spec3 = VectorDataSpec.parse("facet.sift-128.default.query");

            assertThat(spec1).isEqualTo(spec2);
            assertThat(spec1.hashCode()).isEqualTo(spec2.hashCode());
            assertThat(spec1).isNotEqualTo(spec3);
        }
    }

    @Nested
    @DisplayName("Picocli Converter Integration")
    class PicocliConverterTest {

        @Test
        @DisplayName("should convert string to VectorDataSpec")
        void shouldConvertString() throws Exception {
            VectorDataSpecConverter converter = new VectorDataSpecConverter();

            VectorDataSpec spec = converter.convert("facet.sift-128.default.base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
        }

        @Test
        @DisplayName("should convert shorthand facet string")
        void shouldConvertShorthandFacetString() throws Exception {
            VectorDataSpecConverter converter = new VectorDataSpecConverter();

            VectorDataSpec spec = converter.convert("sift-128.default.base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
        }

        @Test
        @DisplayName("should convert colon-separated facet string for compatibility")
        void shouldConvertColonSeparatedFacetString() throws Exception {
            VectorDataSpecConverter converter = new VectorDataSpecConverter();

            VectorDataSpec spec = converter.convert("sift-128:default:base");

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
        }

        @Test
        @DisplayName("should throw TypeConversionException for invalid input")
        void shouldThrowConversionException() {
            VectorDataSpecConverter converter = new VectorDataSpecConverter();

            assertThatThrownBy(() -> converter.convert("facet:invalid"))
                .isInstanceOf(CommandLine.TypeConversionException.class);
        }

        @Test
        @DisplayName("should throw TypeConversionException with suggestions for ambiguous URL")
        void shouldThrowConversionExceptionWithSuggestionsForAmbiguousUrl() {
            VectorDataSpecConverter converter = new VectorDataSpecConverter();

            assertThatThrownBy(() -> converter.convert("https://example.com/datasets/sift/"))
                .isInstanceOf(CommandLine.TypeConversionException.class)
                .hasMessageContaining("ambiguous")
                .hasMessageContaining("facet");
        }

        @Test
        @DisplayName("should work with picocli command parsing")
        void shouldWorkWithCommandParsing() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--vectors", "facet.sift-128.default.base");

            assertThat(command.vectors).isNotNull();
            assertThat(command.vectors.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
        }

        @Test
        @DisplayName("should work with picocli command parsing using shorthand")
        void shouldWorkWithCommandParsingShorthand() {
            DummyCommand command = new DummyCommand();
            new CommandLine(command).parseArgs("--vectors", "sift-128:default:base");

            assertThat(command.vectors).isNotNull();
            assertThat(command.vectors.getSourceType()).isEqualTo(VectorDataSpec.SourceType.CATALOG_FACET);
        }

        @Test
        @DisplayName("should reject incomplete spec for positional argument")
        void shouldRejectIncompleteSpecForPositional() {
            DummyPositionalCommand command = new DummyPositionalCommand();

            assertThatThrownBy(() -> new CommandLine(command).parseArgs("sift-128:default"))
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("Incomplete vector data spec")
                .hasMessageContaining("sift-128.default.base");
        }
    }

    @Nested
    @DisplayName("Jetty Server Integration")
    @ExtendWith(JettyFileServerExtension.class)
    class JettyServerIntegrationTest {

        @Test
        @DisplayName("should parse URL from Jetty test server")
        void shouldParseJettyUrl() {
            String baseUrl = JettyFileServerExtension.getBaseUrl().toString();
            String fileUrl = baseUrl + "basic.txt";

            VectorDataSpec spec = VectorDataSpec.parse(fileUrl);

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.REMOTE_FILE);
            assertThat(spec.getRemoteUri()).isPresent();
        }

        @Test
        @DisplayName("should reject dataset base URL from Jetty test server as ambiguous")
        void shouldRejectJettyDatasetBaseUrl() {
            String baseUrl = JettyFileServerExtension.getBaseUrl().toString();
            String datasetUrl = baseUrl + "rawdatasets/testxvec/";

            assertThatThrownBy(() -> VectorDataSpec.parse(datasetUrl))
                .isInstanceOf(VectorDataSpec.AmbiguousDatasetBaseException.class);
        }

        @Test
        @DisplayName("should parse explicit dataset.yaml URL from Jetty test server")
        void shouldParseJettyDatasetYamlUrl() {
            String baseUrl = JettyFileServerExtension.getBaseUrl().toString();
            String datasetYamlUrl = baseUrl + "rawdatasets/testxvec/dataset.yaml";

            VectorDataSpec spec = VectorDataSpec.parse(datasetYamlUrl);

            assertThat(spec.getSourceType()).isEqualTo(VectorDataSpec.SourceType.REMOTE_DATASET_YAML);
            assertThat(spec.getDatasetYamlUrl()).contains(datasetYamlUrl);
        }
    }

    /// Dummy command for picocli integration testing
    private static final class DummyCommand implements Runnable {
        @CommandLine.Option(names = {"--vectors"}, converter = VectorDataSpecConverter.class)
        VectorDataSpec vectors;

        @Override
        public void run() {
        }
    }

    private static final class DummyPositionalCommand implements Runnable {
        @CommandLine.Parameters(
            paramLabel = "VECTORS",
            arity = "1",
            converter = VectorDataSpecConverter.class)
        VectorDataSpec vectors;

        @Override
        public void run() {
        }
    }
}
