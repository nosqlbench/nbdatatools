package io.nosqlbench.vectordata.downloader.testserver;

import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class GenerateMrefFiles {
    
    @Test
    public void generateMrefFilesForTestData() throws Exception {
        String testDataDir = "/home/jshook/IdeaProjects/nbdatatools/vectordata/src/test/resources/testserver/rawdatasets/testxvec/";
        
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