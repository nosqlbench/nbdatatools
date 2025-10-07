package io.nosqlbench.command.hdf5.subcommands;

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

import io.nosqlbench.command.hdf5.subcommands.export_hdf5.CMD_export_hdf5;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/// Export HDF5 KNN answer-keys from other formats
@CommandLine.Command(name = "export",
    description = "export HDF5 KNN answer-keys from other formats",
    subcommands = {CommandLine.HelpCommand.class})
public class ExportHdf5 implements Callable<Integer> {
    /// Logger for this class
    private static final Logger logger = LogManager.getLogger(ExportHdf5.class);

    @CommandLine.Option(names = {"-o", "--outfile"},
        required = true,
        defaultValue = "[model][_d{dims*}][_b{vectors*}][_q{queries*}][_i{indices*}][_mk{max_k*}].hdf5",
        description = "The HDF5 file to write\ndefault: ${DEFAULT-VALUE}")
    private String outfile;

    @CommandLine.Option(names = {"-m", "--mapping-file"},
        required = false,
        description = "The mapping file(s) to read. If provided, exports multiple files according to contents of the mapping files.")
    private List<Path> mappingFiles;

    @CommandLine.Option(names = {"--query_vectors", "--query"},
        required = false,
        description = "The query_vectors file to read")
    private Path query_vectors;

    @CommandLine.Option(names = {"--query_terms"},
        required = false,
        description = "The query_terms file to read")
    private Path query_terms;

    @CommandLine.Option(names = {"--query_filters"},
        required = false,
        description = "The query_filters file to read")
    private Path query_filters;

    @CommandLine.Option(names = {"--neighbor_indices", "--indices"},
        required = false,
        description = "The query_neighbors file to read")
    private Path neighbors;

    @CommandLine.Option(names = {"--neighbor_distances", "--distances"},
        required = false,
        description = "The query_distances file to read")
    private Path distances;

    @CommandLine.Option(names = {"--base_vectors", "--base"},
        required = false,
        description = "The base_vectors file to read")
    private Path base_vectors;

    @CommandLine.Option(names = {"--base_content"},
        required = false,
        description = "The base_content file to read")
    private Path base_content;

    @CommandLine.Option(names = {"--metadata"},
        required = false,
        description = "The metadata file to read")
    private Path metadataFile;

    @CommandLine.Option(names = {"--layout"},
        required = false,
        description = "The layout file to read")
    private Path layout;

    @CommandLine.Option(names = {"--force"},
        description = "Force overwrite of existing HDF5 files, even if no changes to mapping since last export")
    private boolean force = false;

    /// Create a new ExportHdf5 command
    /// 
    /// Default constructor that initializes a new export subcommand instance.
    public ExportHdf5() {
    }

    @Override
    public Integer call() throws Exception {
        logger.info("Executing export command");
        
        // Create and configure the CMD_export_hdf5 instance
        CMD_export_hdf5 cmd = new CMD_export_hdf5();
        
        // Use reflection to copy all the fields from this class to the CMD_export_hdf5 instance
        java.lang.reflect.Field[] fields = ExportHdf5.class.getDeclaredFields();
        for (java.lang.reflect.Field field : fields) {
            if (field.getName().equals("logger")) {
                continue;
            }
            
            try {
                java.lang.reflect.Field targetField = CMD_export_hdf5.class.getDeclaredField(field.getName());
                field.setAccessible(true);
                targetField.setAccessible(true);
                Object value = field.get(this);
                targetField.set(cmd, value);
            } catch (NoSuchFieldException e) {
                // Skip fields that don't exist in the target class
                logger.debug("Field {} not found in CMD_export_hdf5", field.getName());
            }
        }
        
        // Call the CMD_export_hdf5 implementation
        Integer result = cmd.call();
        
        logger.info("Export command completed with result: {}", result);
        return result;
    }
}