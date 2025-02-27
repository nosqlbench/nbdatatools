package io.nosqlbench.nbvectors;

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

@CommandLine.Command(name = "nbv", subcommands = {
    CMD_VerifyKNN.class, CMD_TagHDF5.class, CMD_jjq.class, CMD_BuildHDF5.class, CMD_ShowHDF5.class
})
public class NBVectorTools {
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
