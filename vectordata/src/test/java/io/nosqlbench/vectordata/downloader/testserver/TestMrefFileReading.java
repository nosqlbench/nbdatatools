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
import io.nosqlbench.vectordata.merklev2.Merklev2Footer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestMrefFileReading {
    
    private static final Path TEMP_MREF_DIR = 
        Paths.get("src/test/resources/testserver/temp/generated_mref_files");
    
    @BeforeEach
    void checkPrerequisites() {
        Path mrefFile = TEMP_MREF_DIR.resolve("testxvec_base.fvec.mref");
        assumeTrue(Files.exists(mrefFile), 
            "Requires master .mref files - run MasterMrefFileGenerator first");
    }
    
    @Test
    public void testReadMrefFile() throws Exception {
        Path mrefFile = TEMP_MREF_DIR.resolve("testxvec_base.fvec.mref");
        
        System.out.println("Testing reading of master-generated .mref file: " + mrefFile);
        System.out.println("File size: " + Files.size(mrefFile));
        
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
