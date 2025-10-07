package io.nosqlbench.command.version;

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

import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.nbdatatools.api.services.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

/// Display version information for nbdatatools
///
/// This command displays detailed version information including:
/// - Maven coordinates (groupId, artifactId, version)
/// - Build information (build number, timestamp)
/// - Runtime environment (Java version, OS details)
///
/// # Usage
/// ```
/// version
/// version --short
/// version --json
/// ```
@Selector("version")
@CommandLine.Command(name = "version",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "Display version information for nbdatatools",
    description = "Displays detailed version information including Maven coordinates, " +
        "build timestamp, and runtime environment details.",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {"0:success", "1:error"})
public class CMD_version implements BundledCommand, Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_version.class);

    @CommandLine.Option(names = {"-s", "--short"},
        description = "Display short version only (just version number)")
    private boolean shortVersion = false;

    @CommandLine.Option(names = {"-j", "--json"},
        description = "Output version information in JSON format")
    private boolean jsonFormat = false;

    @CommandLine.Option(names = {"-v", "--verbose"},
        description = "Display all available version details")
    private boolean verbose = false;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    private Properties versionProperties;

    public CMD_version() {
        loadVersionProperties();
    }

    private void loadVersionProperties() {
        versionProperties = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("version.properties")) {
            if (is != null) {
                versionProperties.load(is);
                logger.debug("Loaded version properties from version.properties");
            } else {
                logger.warn("version.properties not found in classpath");
                // Set default values
                versionProperties.setProperty("version", "unknown");
                versionProperties.setProperty("groupId", "io.nosqlbench");
                versionProperties.setProperty("artifactId", "nbdatatools");
                versionProperties.setProperty("buildNumber", "development");
                versionProperties.setProperty("buildTimestamp", "development");
            }
        } catch (IOException e) {
            logger.error("Failed to load version.properties", e);
            versionProperties.setProperty("version", "error");
        }

        // Add runtime properties
        versionProperties.setProperty("javaVersion", System.getProperty("java.version", "unknown"));
        versionProperties.setProperty("javaVendor", System.getProperty("java.vendor", "unknown"));
        versionProperties.setProperty("osName", System.getProperty("os.name", "unknown"));
        versionProperties.setProperty("osArch", System.getProperty("os.arch", "unknown"));
        versionProperties.setProperty("osVersion", System.getProperty("os.version", "unknown"));
    }

    @Override
    public Integer call() throws Exception {
        if (shortVersion) {
            System.out.println(versionProperties.getProperty("version", "unknown"));
            return 0;
        }

        if (jsonFormat) {
            printJsonFormat();
        } else {
            printTextFormat();
        }

        return 0;
    }

    private void printTextFormat() {
        System.out.println("NBDataTools Version Information");
        System.out.println("================================");
        System.out.println();

        // Core version info
        System.out.println("Version:        " + versionProperties.getProperty("version", "unknown"));
        System.out.println("Group ID:       " + versionProperties.getProperty("groupId", "unknown"));
        System.out.println("Artifact ID:    " + versionProperties.getProperty("artifactId", "unknown"));

        // Build info
        String buildNumber = versionProperties.getProperty("buildNumber", "unknown");
        String buildTimestamp = versionProperties.getProperty("buildTimestamp", "unknown");

        if (!"unknown".equals(buildNumber) && !"development".equals(buildNumber)) {
            System.out.println();
            System.out.println("Build Information:");
            System.out.println("  Build Number:    " + buildNumber);
            System.out.println("  Build Time:      " + buildTimestamp);
        }

        // Runtime environment
        if (verbose) {
            System.out.println();
            System.out.println("Runtime Environment:");
            System.out.println("  Java Version:    " + versionProperties.getProperty("javaVersion"));
            System.out.println("  Java Vendor:     " + versionProperties.getProperty("javaVendor"));
            System.out.println("  OS Name:         " + versionProperties.getProperty("osName"));
            System.out.println("  OS Architecture: " + versionProperties.getProperty("osArch"));
            System.out.println("  OS Version:      " + versionProperties.getProperty("osVersion"));

            // Maven info if available
            String mavenVersion = versionProperties.getProperty("mavenVersion");
            if (mavenVersion != null && !mavenVersion.isEmpty()) {
                System.out.println("  Maven Version:   " + mavenVersion);
            }
        }
    }

    private void printJsonFormat() {
        StringBuilder json = new StringBuilder("{\n");
        json.append("  \"version\": \"").append(escapeJson(versionProperties.getProperty("version", "unknown"))).append("\",\n");
        json.append("  \"groupId\": \"").append(escapeJson(versionProperties.getProperty("groupId", "unknown"))).append("\",\n");
        json.append("  \"artifactId\": \"").append(escapeJson(versionProperties.getProperty("artifactId", "unknown"))).append("\",\n");

        String buildNumber = versionProperties.getProperty("buildNumber", "unknown");
        String buildTimestamp = versionProperties.getProperty("buildTimestamp", "unknown");

        json.append("  \"buildNumber\": \"").append(escapeJson(buildNumber)).append("\",\n");
        json.append("  \"buildTimestamp\": \"").append(escapeJson(buildTimestamp)).append("\"");

        if (verbose) {
            json.append(",\n");
            json.append("  \"runtime\": {\n");
            json.append("    \"javaVersion\": \"").append(escapeJson(versionProperties.getProperty("javaVersion"))).append("\",\n");
            json.append("    \"javaVendor\": \"").append(escapeJson(versionProperties.getProperty("javaVendor"))).append("\",\n");
            json.append("    \"osName\": \"").append(escapeJson(versionProperties.getProperty("osName"))).append("\",\n");
            json.append("    \"osArch\": \"").append(escapeJson(versionProperties.getProperty("osArch"))).append("\",\n");
            json.append("    \"osVersion\": \"").append(escapeJson(versionProperties.getProperty("osVersion"))).append("\"");

            String mavenVersion = versionProperties.getProperty("mavenVersion");
            if (mavenVersion != null && !mavenVersion.isEmpty()) {
                json.append(",\n");
                json.append("    \"mavenVersion\": \"").append(escapeJson(mavenVersion)).append("\"");
            }

            json.append("\n  }");
        }

        json.append("\n}");
        System.out.println(json.toString());
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\")
                   .replace("\"", "\\\"")
                   .replace("\n", "\\n")
                   .replace("\r", "\\r")
                   .replace("\t", "\\t");
    }

    public static void main(String[] args) {
        logger.info("Creating version command");
        CMD_version cmd = new CMD_version();
        logger.info("Executing command line");
        int exitCode = new CommandLine(cmd)
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setOptionsCaseInsensitive(true)
            .execute(args);
        logger.info("Exiting main with code: {}", exitCode);
        System.exit(exitCode);
    }
}