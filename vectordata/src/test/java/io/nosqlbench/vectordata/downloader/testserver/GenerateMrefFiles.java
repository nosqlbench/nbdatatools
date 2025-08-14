package io.nosqlbench.vectordata.downloader.testserver;

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


import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @deprecated This test has been replaced by MasterMrefFileGenerator.
 * Use MasterMrefFileGenerator for canonical .mref file generation.
 */
@Deprecated
public class GenerateMrefFiles {
    
    @Test
    @Disabled("Replaced by MasterMrefFileGenerator to avoid file conflicts")
    public void generateMrefFilesForTestData() throws Exception {
        String testDataDir = "src/test/resources/testserver/rawdatasets/testxvec/";
        
        String[] files = {
            "testxvec_base.fvec",
            "testxvec_query.fvec", 
            "testxvec_distances.fvec",
            "testxvec_indices.ivec"
        };
        
        for (String filename : files) {
            Path dataFile = Paths.get(testDataDir + filename);
            Path mrefFile = Paths.get(testDataDir + filename + ".mref");
            
            System.out.println("Generating .mref for " + filename);
            System.out.println("Data file size: " + java.nio.file.Files.size(dataFile));
            
            var progress = MerkleRefFactory.fromData(dataFile);
            var merkleRef = progress.getFuture().get();
            
            System.out.println("Merkle shape - content size: " + merkleRef.getShape().getTotalContentSize());
            System.out.println("Merkle shape - chunk size: " + merkleRef.getShape().getChunkSize());
            
            merkleRef.save(mrefFile);
            
            System.out.println("Generated " + mrefFile + " with size: " + java.nio.file.Files.size(mrefFile));
        }
        
        System.out.println("Done!");
    }
}
