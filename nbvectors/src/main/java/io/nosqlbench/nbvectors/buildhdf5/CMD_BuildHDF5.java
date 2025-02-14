package io.nosqlbench.nbvectors.buildhdf5;

import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.nosqlbench.nbvectors.verifyknn.logging.CustomConfigurationFactory;
import io.nosqlbench.nbvectors.verifyknn.options.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "buildhdf5",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "build HDF5 KNN test data answer-keys from JSON",
    description = """
        TBD
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
    })
public class CMD_BuildHDF5 implements Callable<Integer> {

  private static Logger logger = LogManager.getLogger(CMD_BuildHDF5.class);

  @Option(names = {"-o", "--outfile"},
      required = true,
      defaultValue = "out.hdf5",
      description = "The " + "HDF5" + " file to " + "write")
  private Path hdfOutPath;

  @Option(names = {"-l", "--layout"},
      required = true,
      defaultValue = "layout.yaml",
      description = "The yaml file containing the layout " + "instructions.")
  private Path layoutPath;

  @Option(names = {"--_diaglevel", "-_d"}, hidden = true, description = """
      Internal diagnostic level, sends content directly to the console.""", defaultValue = "ERROR")
  ConsoleDiagnostics diaglevel;

  public static void main(String[] args) {

    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_BuildHDF5 command = new CMD_BuildHDF5();
    logger.info("instancing commandline");
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    logger.info("executing commandline");
    int exitCode = commandLine.execute(args);
    logger.info("exiting main");
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {

    MapperConfig config = MapperConfig.file(layoutPath);
    WritableHdfFile writable = HdfFile.write(hdfOutPath);
    JsonLoader jloader = new JsonLoader();
    try (KnnDataWriter kwriter = new KnnDataWriter(hdfOutPath)) {

      System.out.println("writing neighbors stream...");
      kwriter.writeNeighborsStream(JsonLoader.readNeighborsStream(config));

      System.out.println("writing training stream...");
      kwriter.writeTrainingStream(JsonLoader.readTrainingStream(config));

      System.out.println("writing test stream...");
      kwriter.writeTestStream(JsonLoader.readTestStream(config));

      System.out.println("writing distances stream...");
      kwriter.writeDistancesStream(JsonLoader.readDistancesStream(config));
    }
    ;
    //    KnnDataWriter.writeTraining(trainingStream,writable);

    return 0;
  }

}