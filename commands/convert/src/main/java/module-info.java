/**
 Converter module for vector file formats
 */
module convert {
  requires info.picocli;
  requires org.apache.logging.log4j;
  requires testdata.apis;
  provides io.nosqlbench.nbvectors.api.commands.BundledCommand with io.nosqlbench.nbdatatools.commands.convert.CMD_convert;
}