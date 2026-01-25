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

import picocli.CommandLine;

import java.io.PrintWriter;

/// Generate bash completion script with dynamic completion hooks.
@CommandLine.Command(name = "generate-completion",
    version = "generate-completion " + CommandLine.VERSION,
    mixinStandardHelpOptions = true,
    helpCommand = true,
    description = {
        "Generate bash completion script for ${ROOT-COMMAND-NAME:-the root command of this command}.",
        "Run the following command to give `${ROOT-COMMAND-NAME:-$PARENTCOMMAND}` TAB completion in the current shell:",
        "",
        "  source <(${PARENT-COMMAND-FULL-NAME:-$PARENTCOMMAND} ${COMMAND-NAME})",
        ""}
)
public class NbvectorsGenerateCompletion implements Runnable {

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @Override
    public void run() {
        String commandName = spec.root().name();
        String script = buildScript(commandName);
        PrintWriter out = spec.commandLine().getOut();
        out.print(script);
        out.print('\n');
        out.flush();
    }

    private static String buildScript(String commandName) {
        String completeCommand = commandName + " complete";
        return "# Bash completion support for the '" + commandName + "' command.\n"
            + "# This uses dynamic completion via a -C hook.\n"
            + "complete -o default -o nospace -C '" + completeCommand + "' "
            + commandName + " " + commandName + ".sh " + commandName + ".bash\n";
    }
}
