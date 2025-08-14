package io.nosqlbench.command.fetch.subcommands;

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


import io.nosqlbench.command.convert.CMD_convert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

public class TempTestHarness {

  @Test
  public void testConvertMsMarcoParquet(@TempDir Path tempDir) throws IOException {

    System.out.println("temp dir:" + tempDir);

    CMD_convert.main(new String[]{
        "-i","/home/jshook/dswork/s3/huggingface/cache/datasets--Cohere--wikipedia-2023-11-embed"
             + "-multilingual-v3/snapshots/37feace541fadccf70579e9f289c3cf8e8b186d7/es",
        "--input-format", "parquet",
        "-o", tempDir.resolve("test.fvec").toString(),
        "--force"
    });
    //0100.parquet

    System.out.println("done");
  }

}
