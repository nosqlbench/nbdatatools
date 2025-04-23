package io.nosqlbench.nbvectors.commands;

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


import io.nosqlbench.nbvectors.commands.hugging_dl.BundledCommand;
import io.nosqlbench.nbvectors.services.Selector;
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
      Selector anno = c.type().getAnnotation(Selector.class);
      if (anno == null || extant.contains(anno.value())) {
        return false;
      }
      return true;
    }).forEach(c -> {
      CommandLine cl = new CommandLine(c.type());
      commandSpec.addSubcommand(
          Objects.requireNonNull(c.type().getAnnotation(Selector.class)).value(), cl);
    });
    return commandSpec;
  }
}
