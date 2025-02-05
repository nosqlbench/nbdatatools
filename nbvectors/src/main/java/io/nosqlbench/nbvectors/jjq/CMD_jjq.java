package io.nosqlbench.nbvectors.jjq;

import static picocli.CommandLine.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.nosqlbench.nbvectors.jjq.bulkio.*;
import io.nosqlbench.nbvectors.jjq.evaluator.JqProc;
import io.nosqlbench.nbvectors.jjq.evaluator.JsonNodeMapper;
import io.nosqlbench.nbvectors.jjq.functions.NBJQFunction;
import io.nosqlbench.nbvectors.jjq.functions.NBStateFunction;
import io.nosqlbench.nbvectors.jjq.outputs.JsonlFileOutput;
import io.nosqlbench.nbvectors.jjq.outputs.PrettyConsoleOutput;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.internal.functions.EnvFunction;
import net.thisptr.jackson.jq.module.ModuleLoader;
import net.thisptr.jackson.jq.module.loaders.BuiltinModuleLoader;
import net.thisptr.jackson.jq.module.loaders.ChainedModuleLoader;
import net.thisptr.jackson.jq.module.loaders.FileSystemModuleLoader;
import picocli.CommandLine;

import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

@Command(name = "jjq")
public class CMD_jjq implements Callable<Integer> {

  @Option(names = {"-i", "--in"}, required = true)
  private Path inFile;

  @Option(names = {"-o", "--out"}, required = false)
  private Path outPath;

  @Option(names = {"-q", "--jq", "--query"}, required = false, defaultValue = ".")
  private String jq;

  @Option(names = {"-t", "--threads"}, required = false, defaultValue = "0")
  private int threads;

  @Option(names = {"-p", "--parts"}, required = false, defaultValue = "0")
  private int parts;

  public static void main(String[] args) {
    CMD_jjq command = new CMD_jjq();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    long result_count = 0;
    Function<String, JsonNode> mapper = new JsonNodeMapper();
    LinkedList<Future<?>> futures;

    Scope rootScope = rootScope();

    try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
      System.out.println("partitioning");
      int partitionCount = threads != 0 ? threads : Runtime.getRuntime().availableProcessors() - 1;
      FilePartitions partitions = FilePartition.of(inFile).partition(partitionCount);
      System.out.println(partitions);
      Output output = null;
      if (outPath != null) {
        output = new JsonlFileOutput(outPath);
      } else {
        output = new PrettyConsoleOutput();
      }

      JsonQuery query = JsonQuery.compile(this.jq, Versions.JQ_1_6);

      futures = new LinkedList<>();

      int count = 0;
      for (FilePartition partition : partitions) {
        count++;
        Scope scope = Scope.newChildScope(rootScope);
        Iterable<String> lines = partition.asStringIterable();
        JqProc f = new JqProc(partition.toString(), scope, lines, mapper, query, output);
        Future<?> future = exec.submit(f);
        futures.addLast(future);
      }

      while (!futures.isEmpty()) {
        try {
          Future<?> f = futures.removeLast();
          Object result = f.get();
          System.out.println("result:" + (result != null ? result : "NULL"));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        //        exec.shutdownNow();
      }

    }

    List<NBJQFunction> registeredFunctions = NBJJQ.getRegisteredFunctions(rootScope);
    for (NBJQFunction registeredFunction : registeredFunctions) {
      System.out.println("registered function after:" + registeredFunction);
      registeredFunction.finish();
    }
    System.out.println("STATE:");
    NBJJQ.getState(rootScope).forEach((k,v) -> {
      System.out.println("k:"+k+", v:" + v);
    });

    return 0;

    //    while (true) {
    //      while (!output.isEmpty()) {
    //        JsonNode result = output.take();
    //        result_count++;
    //        if ((result_count % 100)==0) {
    //          System.out.println("result_count:" + result_count);
    //        }
    //      }
    //      Thread.sleep(1000);
    //    }
  }

  private Scope rootScope() throws URISyntaxException {
    Scope scope = Scope.newEmptyScope();
    BuiltinFunctionLoader.getInstance().loadFunctions(Version.LATEST, scope);
//    BuiltinFunctionLoader.getInstance().listFunctions(Versions.JQ_1_6, scope)
//        .forEach((k, v) -> System.out.println("function: " + k));

    //    scope.addFunction("env", 0, new EnvFunction());

    scope.setModuleLoader(new ChainedModuleLoader(new ModuleLoader[]{
        BuiltinModuleLoader.getInstance(), new FileSystemModuleLoader(
        scope,
        Version.LATEST,
        FileSystems.getDefault().getPath("").toAbsolutePath()
    ),
        }));

    NBStateFunction nbsf = new NBStateFunction();
    scope.addFunction("nbstate",nbsf);

    return scope;


    //
    //    Scope rootScope = Scope.newEmptyScope();
    //    BuiltinFunctionLoader bifl = BuiltinFunctionLoader.getInstance();
    //
    //    bifl.listFunctions(Versions.JQ_1_6, rootScope)
    //        .forEach((k, v) -> System.out.println("function: " + k));
    //
    //    FileSystemModuleLoader fsml = new FileSystemModuleLoader(
    //        rootScope,
    //        Versions.JQ_1_6,
    //        FileSystems.getDefault().getPath("").toAbsolutePath(),
    //        Paths.get(Scope.class.getClassLoader().getResource("").toURI())
    //    );
    //
    //    rootScope.setModuleLoader(new ChainedModuleLoader(new ModuleLoader[]{
    //        BuiltinModuleLoader.getInstance(), fsml
    //    }));
    //
    //
    //    BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_6, rootScope);
    //
    //
    //    return rootScope;
    //    //    Scope workScope = Scope.newChildScope(rootScope);
    //    //    return workScope;
  }
}
