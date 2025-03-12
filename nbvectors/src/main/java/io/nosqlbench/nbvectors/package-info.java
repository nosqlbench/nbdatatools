/// a collection of tools for working with vector test data, available as sub-commands under the
/// nbvectors command.
///
/// The {@link io.nosqlbench.nbvectors.CMD_nbvectors} class is the main entry point.
///
/// Each sub-command is implemented in a package directly below this one, with a name like `CMD_<name>`.
///
/// {@link picocli.CommandLine.Command} annotations are used to define the command line
/// interface, which also serves as a form of documentation.
///
/// This is not visible within javadoc. To see the CLI documentation, run a tool with the "--help"
/// option, or review the annotations directly.

package io.nosqlbench.nbvectors;