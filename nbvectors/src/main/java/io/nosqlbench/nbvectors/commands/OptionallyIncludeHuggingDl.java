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


import io.nosqlbench.nbvectors.commands.hugging_dl.CMD_hugging_dl;
import picocli.CommandLine;

public class OptionallyIncludeHuggingDl implements CommandLine.IModelTransformer {
  @Override
  public CommandLine.Model.CommandSpec transform(CommandLine.Model.CommandSpec commandSpec) {
    try {
      Class<?> hugging_dl =
          getClass().getClassLoader().loadClass(CMD_hugging_dl.class.getCanonicalName().toString());
      CommandLine hugging_dl_cmdline = new CommandLine(hugging_dl);
      commandSpec.subcommands().put("hugging_dl",hugging_dl_cmdline);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return commandSpec;
  }
}
