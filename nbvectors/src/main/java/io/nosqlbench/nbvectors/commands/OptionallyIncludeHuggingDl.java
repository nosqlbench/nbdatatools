package io.nosqlbench.nbvectors.commands;

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
