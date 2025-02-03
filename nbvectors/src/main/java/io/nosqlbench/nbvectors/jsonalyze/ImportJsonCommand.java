package io.nosqlbench.nbvectors.jsonalyze;

import static picocli.CommandLine.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.thisptr.jackson.jq.*;
import picocli.CommandLine;

import java.net.URISyntaxException;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Command(name = "jsonalyze")
public class ImportJsonCommand implements Callable<Integer> {

  @Option(names = {"-i", "--in"}, required = true)
  private Path inFile;

  @Option(names = {"-o", "--out"}, required = false)
  private Path outFile;

  @Option(names = {"-q", "--jq"}, required = false, defaultValue = ".")
  private String jq;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) {
    ImportJsonCommand command = new ImportJsonCommand();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    AtomicLong total = new AtomicLong();
    LineChunker lineChunker = new LineChunker(inFile, 0, 1000);
    Scope workScope = initScope();
    JsonQuery query = JsonQuery.compile(this.jq, Versions.JQ_1_6);
    BlockingQueue<NodeOutput> outputQueue = new LinkedBlockingQueue<>(50);

    ExecutorService exec =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * (int) 1.5,
            Thread.ofVirtual().factory()
        );

    exec.submit(() -> {
      for (CharBuffer buf : lineChunker) {
        Future<NodeOutput> results = exec.submit(() -> {
          NodeOutput output = new NodeOutput();
          System.out.println("chunk at " + lineChunker.lastOffset);
          String img = buf.toString();
          String[] lines = img.split("\n");
          for (String line : lines) {
            JsonNode in = null;
            try {
              in = MAPPER.readTree(line);
              query.apply(workScope, in, output);
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          }
          return output;
        });
      }
    });

    return 0;
  }

  private Scope initScope() throws URISyntaxException {
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

    Scope workScope = Scope.newChildScope(rootScope);
    return workScope;
  }
}
