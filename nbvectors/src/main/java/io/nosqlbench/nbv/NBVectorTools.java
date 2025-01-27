package io.nosqlbench.nbv;

import io.nosqlbench.nbvectors.app.VerifyKNN;
import io.nosqlbench.nbvectors.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

@CommandLine.Command(name = "nbv", subcommands = {VerifyKNN.class})
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
