package io.nosqlbench.command.json.subcommands.jjq.evaluator;

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


import com.fasterxml.jackson.databind.JsonNode;
import io.nosqlbench.command.json.subcommands.jjq.apis.NBJJQ;
import io.nosqlbench.command.json.subcommands.jjq.apis.NBStateContext;
import io.nosqlbench.command.json.subcommands.jjq.apis.NBStateContextHolderHack;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.module.loaders.BuiltinModuleLoader;
import net.thisptr.jackson.jq.module.loaders.ChainedModuleLoader;
import net.thisptr.jackson.jq.module.loaders.FileSystemModuleLoader;

import java.nio.file.FileSystems;
import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

/// Because the invoker does stateful work, including saving results at the max
/// you should really run it in a try-with-resources block.
public class JJQInvoker implements Runnable, AutoCloseable {

  private final String expr;
  private final Output output;
  private final Supplier<String> lines;
  private Scope rootScope;
  private NBStateContextHolderHack nbContext;

  /// create a jjq invoker
  /// @param lines the source of JSONL data to process
  /// @param expr the jq expression to apply
  /// @param output the output to write results to
  public JJQInvoker(Supplier<String> lines, String expr, Output output) {
    this.lines = lines;
    this.expr = expr;
    this.output = output;
  }

  /// run the invoker
  @Override
  public void run() {
    try {
      this.nbContext = new NBStateContextHolderHack();
      this.rootScope = rootScope(nbContext);
      Function<String, JsonNode> mapper = new JsonNodeMapper();
      LinkedList<Future<?>> futures;
      JsonQuery query = JsonQuery.compile(expr, Versions.JQ_1_6);
      Scope scope = Scope.newChildScope(rootScope);
      JqProc f = new JqProc("diagnostic evaluation", scope, lines, mapper, query, output);
      f.run();

      //      if (diagnose) {
      //        try {
      //          for (FilePartition partition : partitions) {
      //            try (ConcurrentSupplier<String> lines = partitions.getFirst().asConcurrentSupplier();) {
      //              JqProc f = new JqProc("diagnostic evaluation", scope, lines, mapper, query, output);
      //              f.run();
      //            }
      //          }
      //        } catch (Exception e) {
      //          throw new RuntimeException(e);
      //        }
      //      } else {
      //        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
      //          futures = new LinkedList<>();
      //
      //          int count = 0;
      //          for (FilePartition partition : partitions) {
      //            count++;
      //            ConcurrentSupplier<String> supplier = partition.asConcurrentSupplier();
      //            JqProc f = new JqProc(partition.toString(), scope, supplier, mapper, query, output);
      //            Future<?> future = exec.submit(f);
      //            futures.addLast(future);
      //          }
      //
      //          while (!futures.isEmpty()) {
      //            try {
      //              Future<?> f = futures.removeLast();
      //              Object result = f.get();
      //              if (result != null) {
      //                System.out.println("result:" + result);
      //              }
      //            } catch (Exception e) {
      //              throw new RuntimeException(e);
      //            }
      //          }
      //        }
      //      }

      System.out.println("NbState:");
      NBJJQ.getState(rootScope).forEach((k, v) -> System.out.println(" max_k:" + k + ", v:" + v));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /// get the jq scope
  /// @return the jq scope
  public Scope getScope() {
    return this.rootScope;
  }

  /// get the state context
  /// @return the state context
  public NBStateContext getContext() {
    return this.nbContext;
  }

  private Scope rootScope(NBStateContextHolderHack context) {
    try {
      Scope scope = Scope.newEmptyScope();
      BuiltinFunctionLoader.getInstance().loadFunctions(Version.LATEST, scope);
      //    scope.addFunction("env", 0, new EnvFunction());

      scope.setModuleLoader(new ChainedModuleLoader(
          BuiltinModuleLoader.getInstance(), new FileSystemModuleLoader(
          scope,
          Version.LATEST,
          FileSystems.getDefault().getPath("").toAbsolutePath()
      )
      ));

      scope.addFunction("nbstate", context);

      return scope;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }


    //
    //    Scope rootScope = Scope.newEmptyScope();
    //    BuiltinFunctionLoader bifl = BuiltinFunctionLoader.getInstance();
    //
    //    bifl.listFunctions(Versions.JQ_1_6, rootScope)
    //        .forEach((max_k, v) -> System.out.println("function: " + max_k));
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

  @Override
  public void close() throws Exception {
    this.nbContext.close();
  }
}
