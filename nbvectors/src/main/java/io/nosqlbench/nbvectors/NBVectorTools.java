package io.nosqlbench.nbvectors;

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


import io.nosqlbench.nbvectors.buildhdf5.CMD_BuildHDF5;
import io.nosqlbench.nbvectors.jjq.CMD_jjq;
import io.nosqlbench.nbvectors.showhdf5.CMD_ShowHDF5;
import io.nosqlbench.nbvectors.taghdf.CMD_TagHDF5;
import io.nosqlbench.nbvectors.verifyknn.CMD_VerifyKNN;
import io.nosqlbench.nbvectors.verifyknn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

/// A collection of tools for working with vector test data
@CommandLine.Command(name = "nbv", subcommands = {
    CMD_VerifyKNN.class, CMD_TagHDF5.class, CMD_jjq.class, CMD_BuildHDF5.class, CMD_ShowHDF5.class
})
public class NBVectorTools {

  /// run a nbv command
  /// @param args command line args
  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );
    Logger logger = LogManager.getLogger(NBVectorTools.class);

    NBVectorTools command = new NBVectorTools();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }
}
