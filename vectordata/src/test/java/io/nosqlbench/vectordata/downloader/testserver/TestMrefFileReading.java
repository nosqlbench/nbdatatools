package io.nosqlbench.vectordata.downloader.testserver;

import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.Merklev2Footer;
import org.junit.jupiter.api.Test;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestMrefFileReading {
    
    @Test
    public void testReadMrefFile() throws Exception {
        String testDataDir = "/home/jshook/IdeaProjects/nbdatatools/vectordata/src/test/resources/testserver/rawdatasets/testxvec/";
        Path mrefFile = Paths.get(testDataDir + "testxvec_base.fvec.mref");
        
        System.out.println("Testing reading of: " + mrefFile);
        System.out.println("File size: " + java.nio.file.Files.size(mrefFile));
        
        // Try to read footer directly
        try (FileChannel channel = FileChannel.open(mrefFile, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long footerPosition = fileSize - Merklev2Footer.FIXED_FOOTER_SIZE;
            
            System.out.println("Footer position: " + footerPosition);
            System.out.println("Footer size: " + Merklev2Footer.FIXED_FOOTER_SIZE);
            
            Merklev2Footer footer = Merklev2Footer.readFromChannel(channel, footerPosition);
            
            System.out.println("Read footer:");
            System.out.println("  chunkSize: " + footer.chunkSize());
            System.out.println("  totalContentSize: " + footer.totalContentSize());
            System.out.println("  totalChunks: " + footer.totalChunks());
            System.out.println("  leafCount: " + footer.leafCount());
            System.out.println("  footerLength: " + footer.footerLength());
        }
        
        // Try to load via MerkleRefFactory
        try {
            var loaded = MerkleRefFactory.load(mrefFile);
            System.out.println("Successfully loaded via MerkleRefFactory!");
            System.out.println("  content size: " + loaded.getShape().getTotalContentSize());
            System.out.println("  chunk size: " + loaded.getShape().getChunkSize());
        } catch (Exception e) {
            System.out.println("Failed to load via MerkleRefFactory: " + e.getMessage());
            e.printStackTrace();
        }
    }
}