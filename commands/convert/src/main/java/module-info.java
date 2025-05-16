/**
 Converter module for vector file formats
 */
module convert {
  requires info.picocli;
  requires org.apache.logging.log4j;
  requires testdata.apis;
  // Make the package accessible for reflection by the picocli module
  opens io.nosqlbench.nbdatatools.commands.convert;
  provides io.nosqlbench.nbvectors.api.commands.BundledCommand with io.nosqlbench.nbdatatools.commands.convert.CMD_convert;
}