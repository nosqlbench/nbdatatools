
# nbdatatools

This is a module is a part of the NoSQLBench project, focusing on test data management. So far, 
it only contains one tool, but it is expected to grow.

Tools:

## nbvectors

This is a collection of utilities for working with vector test data, hosted under a single 
command line tool. It is packaged as an executable jar with several sub-commands.

```sh
# see all the options
java -jar nbvectors.jar --help


```



## Test Data Diagnostics

Testing tools that require specialized data sometimes need an additional line of defense to ensure
that the data is appropriate. This repo is a place to put such tools in support of NoSQLBench and
other testing systems.

### nbvectors

Starting out, the nbvectors tool does the following:

* Reads an hdf5 KNN answer key and self-checks neighborhoods against newly computed results
* Documents a standard format for HDF5 vector KNN answer keys (inspired initially by ann-benchmark)
* Prints out HDF5 vector data shapes, types, and metadata

## Java Tooling Prototype

This repo also serves as a fresh look at libraries and usage modes on tools. At a minimum, the way
the tools here are used will be aggressively refined around an optimal user and developer experience
for basic tools targeted at simple and specific problems. These tools may find their way into the
NoSQLBench main distribution, but will stand alone on their own first and foremost.

Goals for this repo as a tooling template:

* Consistent methods of finding documentation and getting CLI help
* Provide native binaries a-la graalvm
* Provide simple parameterization, such as with picocli
* Use the terminal to maximum effect
  * present timely and intuitive status
  * allow for users to interrupt or customize ongoing analysis
* provide basic results in stdout, live progress view, and file formats
* provide auto-completion where appropriate

# Project Toolchain

The modules in this project will track a recent Java release. Further, the _native_ modules here are
built with GraalVM (when the native profile is used).
