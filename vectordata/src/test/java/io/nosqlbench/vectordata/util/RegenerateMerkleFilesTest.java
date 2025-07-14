package io.nosqlbench.vectordata.util;

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
import io.nosqlbench.vectordata.merklev2.MerkleData;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Files;

/**
 * Test to regenerate .mref files for test data with correct merklev2 format
 */
public class RegenerateMerkleFilesTest {
    
    @Test
    public void regenerateMerkleFiles() throws Exception {
        String[] testFiles = {
            "src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec",
            "src/test/resources/testserver/rawdatasets/testxvec/testxvec_distances.fvec", 
            "src/test/resources/testserver/rawdatasets/testxvec/testxvec_indices.ivec",
            "src/test/resources/testserver/rawdatasets/testxvec/testxvec_query.fvec"
        };
        
        for (String testFile : testFiles) {
            Path dataPath = Path.of(testFile);
            if (!Files.exists(dataPath)) {
                System.out.println("File not found: " + testFile);
                continue;
            }
            
            Path mrefPath = Path.of(testFile + ".mref");
            
            System.out.println("Regenerating: " + mrefPath);
            
            // Remove existing merkle file
            Files.deleteIfExists(mrefPath);
            
            // Create new merkle data from source file using merklev2
            var progress = MerkleRefFactory.fromData(dataPath);
            var merkleData = progress.getFuture().get();
            
            // Save the merkle data
            merkleData.save(mrefPath);
            
            System.out.println("Generated: " + mrefPath + " (size: " + Files.size(mrefPath) + " bytes)");
            
            // Close the merkle data
            merkleData.close();
        }
        
        System.out.println("Done regenerating .mref files with merklev2 format!");
    }
}
