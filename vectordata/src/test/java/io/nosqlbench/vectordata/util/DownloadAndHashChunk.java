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


import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

/**
 * Utility to download a chunk from HTTP server and compute its hash.
 */
public class DownloadAndHashChunk {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: DownloadAndHashChunk <url> <chunk_index> <chunk_size> <total_size>");
            System.exit(1);
        }
        
        String urlStr = args[0];
        int chunkIndex = Integer.parseInt(args[1]);
        int chunkSize = Integer.parseInt(args[2]);
        long totalSize = Long.parseLong(args[3]);
        
        // Calculate chunk boundaries
        long startOffset = (long) chunkIndex * chunkSize;
        long endOffset = Math.min(startOffset + chunkSize - 1, totalSize - 1);
        int expectedSize = (int)(endOffset - startOffset + 1);
        
        System.out.println("Downloading chunk " + chunkIndex + " from: " + urlStr);
        System.out.println("Range: bytes=" + startOffset + "-" + endOffset);
        System.out.println("Expected size: " + expectedSize + " bytes");
        
        // Create connection with range header
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("Range", "bytes=" + startOffset + "-" + endOffset);
        
        int responseCode = conn.getResponseCode();
        System.out.println("Response code: " + responseCode);
        
        if (responseCode != 206) { // 206 = Partial Content
            System.err.println("Unexpected response code. Expected 206 (Partial Content)");
            System.exit(1);
        }
        
        // Read the chunk data
        ByteBuffer buffer = ByteBuffer.allocate(expectedSize);
        try (InputStream in = conn.getInputStream()) {
            byte[] temp = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(temp)) != -1) {
                buffer.put(temp, 0, bytesRead);
            }
        }
        
        buffer.flip();
        byte[] chunkData = new byte[buffer.remaining()];
        buffer.get(chunkData);
        
        System.out.println("Downloaded " + chunkData.length + " bytes");
        
        // Compute hash
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(chunkData);
        String hexHash = bytesToHex(hash);
        
        System.out.println("\nChunk " + chunkIndex + " hash: " + hexHash);
        
        // Show first few bytes
        System.out.println("\nFirst 32 bytes of chunk data (hex):");
        int bytesToShow = Math.min(32, chunkData.length);
        byte[] firstBytes = new byte[bytesToShow];
        System.arraycopy(chunkData, 0, firstBytes, 0, bytesToShow);
        System.out.println(bytesToHex(firstBytes));
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
