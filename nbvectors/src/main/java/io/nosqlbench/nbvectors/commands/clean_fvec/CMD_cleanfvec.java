package io.nosqlbench.nbvectors.commands.catalog_hdf5;

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


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Alternative implementation of the catalog command, building hierarchical catalogs
 * in a simpler, declarative pass over all entries.
 */
@CommandLine.Command(name = "cleanfvec",
    header = "Clean an input fvec file by removing zero and duplicate vectors",
    description = "When given an input fvec file, does a partitioned sort maintaining indices",
    exitCodeList = {"0: success", "1: error processing files"})
public class CMD_cleanfvec implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_cleanfvec.class);

    @CommandLine.Parameters(description = "Files and/or directories to catalog; Directories will be traversed to find dataset.yaml and .hdf5 files", arity = "0..*")
    private List<Path> inputs;

    public static void main(String[] args) {
        CMD_catalog2 cmd = new CMD_catalog2();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        return 2;
    }

}
