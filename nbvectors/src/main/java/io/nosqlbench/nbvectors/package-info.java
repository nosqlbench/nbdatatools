/// a collection of tools for working with vector test data, available as sub-commands under the
/// nbvectors command.
///
/// The {@link io.nosqlbench.nbvectors.commands.CMD_nbvectors} class is the main entry point.
///
/// Each sub-command is implemented in a package directly below this one, with a name like `CMD_<name>`.
///
/// {@link picocli.CommandLine.Command} annotations are used to define the command line
/// interface, which also serves as a form of documentation.
///
/// This is not visible within javadoc. To see the CLI documentation, run a tool with the "--help"
/// option, or review the annotations directly.

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

