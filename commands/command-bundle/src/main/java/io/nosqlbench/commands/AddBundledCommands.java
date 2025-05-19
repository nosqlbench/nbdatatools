package io.nosqlbench.commands;

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


import io.nosqlbench.nbvectors.api.commands.BundledCommand;
import picocli.CommandLine;

import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

public class AddBundledCommands implements CommandLine.IModelTransformer {
  @Override
  public CommandLine.Model.CommandSpec transform(CommandLine.Model.CommandSpec commandSpec) {
    ServiceLoader<BundledCommand> load = ServiceLoader.load(BundledCommand.class);
    Set<String> extant = commandSpec.subcommands().keySet();
    load.stream().filter(c -> {
      CommandLine.Command canno = c.type().getAnnotation(CommandLine.Command.class);
      if (canno == null) {
        return false;
      }
      if (extant.contains(canno.name())) {
        throw new RuntimeException("Command name '" + canno.name() + "' is already defined, but "
                                   + "found another under the same name in services manifest.");
      }
      return true;
    }).forEach(c -> {
      CommandLine.Command canno = c.type().getAnnotation(CommandLine.Command.class);
      Objects.requireNonNull(canno);
      String name = canno.name();
      CommandLine cl = new CommandLine(c.type());
      commandSpec.addSubcommand(name, cl);
    });
    return commandSpec;
  }
}
