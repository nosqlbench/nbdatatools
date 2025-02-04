package io.nosqlbench.nbvectors.jjq;

import static picocli.CommandLine.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.nosqlbench.nbvectors.jjq.bulkio.BytebufChunker;
import io.nosqlbench.nbvectors.jjq.bulkio.ConvertingIterable;
import io.nosqlbench.nbvectors.jjq.bulkio.FilePartition;
import io.nosqlbench.nbvectors.jjq.bulkio.FilePartitions;
import io.nosqlbench.nbvectors.jjq.evaluator.JqFunctionLoop;
import io.nosqlbench.nbvectors.jjq.evaluator.JsonNodeMapper;
import io.nosqlbench.nbvectors.jjq.evaluator.NodeOutput;
import net.thisptr.jackson.jq.*;
import picocli.CommandLine;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.function.Function;

@Command(name = "jjq")
public class CMD_JacksonJsonQuery implements Callable<Integer> {

  private int concurrency;

  @Option(names = {"-i", "--in"}, required = true)
  private Path inFile;

  @Option(names = {"-o", "--out"}, required = false)
  private Path outFile;

  @Option(names = {"-q", "--jq"}, required = false, defaultValue = ".")
  private String jq;

  public static void main(String[] args) {
    CMD_JacksonJsonQuery command = new CMD_JacksonJsonQuery();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    long result_count = 0;
    Function<String, JsonNode> mapper = new JsonNodeMapper();
    this.concurrency = Runtime.getRuntime().availableProcessors() - 1;
    ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();

    System.out.println("partitioning");
    FilePartitions partitions = FilePartition.of(inFile).partition(concurrency);
    System.out.println(partitions);
    LinkedBlockingQueue<FilePartition> partitionQueue = new LinkedBlockingQueue<>(concurrency);
    NodeOutput output = new NodeOutput(1000);

    //    JobFeeder jobFeeder = new JobFeeder(exec, concurrency, partitionQueue::remove, output);
    Scope rootScope = rootScope();
    JsonQuery query = JsonQuery.compile(this.jq, Versions.JQ_1_6);

    for (FilePartition partition : partitions) {
      Scope threadScope = Scope.newChildScope(rootScope);
      ByteBuffer byteBuffer = partition.mapFile();
      BytebufChunker chunker = new BytebufChunker(byteBuffer, 1000000);
      ConvertingIterable<CharBuffer, String> ci =
          new ConvertingIterable<>(chunker, Object::toString);

      JqFunctionLoop f = new JqFunctionLoop(threadScope, ci, mapper, query, output);
      exec.submit(f);
    }

    while (true) {
      while (!output.isEmpty()) {
        JsonNode result = output.take();
        result_count++;
        if ((result_count % 100)==0) {
          System.out.println("result_count:" + result_count);
        }
      }
      Thread.sleep(1000);
    }
  }

  private Scope rootScope() throws URISyntaxException {
    Scope rootScope = Scope.newEmptyScope();
    BuiltinFunctionLoader.getInstance().listFunctions(Versions.JQ_1_6, rootScope);
    //    rootScope.setModuleLoader(new ChainedModuleLoader(new ModuleLoader[]{
    //        BuiltinModuleLoader.getInstance(), new FileSystemModuleLoader(
    //        rootScope,
    //        Versions.JQ_1_6,
    //        FileSystems.getDefault().getPath("").toAbsolutePath(),
    //        Paths.get(Scope.class.getClassLoader().getResource("classpath_modules").toURI())
    //    ),
    //        }));

    return rootScope;
    //    Scope workScope = Scope.newChildScope(rootScope);
    //    return workScope;
  }
}
