package io.nosqlbench.vectordata.downloader;

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

/**
 * Metadata about a remote resource.
 * 
 * @param size The size of the resource in bytes, or -1 if unknown
 * @param supportsRanges Whether the resource supports HTTP range requests
 * @param exists Whether the resource exists
 * @param contentType The MIME content type of the resource
 * @param lastModified The last modified timestamp of the resource
 * @param etag The ETag value for the resource
 */
public record ResourceMetadata(
    long size,
    boolean supportsRanges,
    boolean exists,
    String contentType,
    String lastModified,
    String etag
) {
    
    /**
     * Creates metadata for a resource that doesn't exist.
     * 
     * @return A ResourceMetadata instance indicating the resource was not found
     */
    public static ResourceMetadata notFound() {
        return new ResourceMetadata(-1, false, false, null, null, null);
    }
    
    /**
     * Creates metadata for an existing resource with basic information.
     * 
     * @param size The size of the resource in bytes
     * @param supportsRanges Whether the resource supports HTTP range requests
     * @return A ResourceMetadata instance with basic information
     */
    public static ResourceMetadata basic(long size, boolean supportsRanges) {
        return new ResourceMetadata(size, supportsRanges, true, null, null, null);
    }
    
    /**
     * Creates metadata for an existing resource with full information.
     * 
     * @param size The size of the resource in bytes
     * @param supportsRanges Whether the resource supports HTTP range requests
     * @param contentType The MIME content type of the resource
     * @param lastModified The last modified timestamp of the resource
     * @param etag The ETag value for the resource
     * @return A ResourceMetadata instance with complete information
     */
    public static ResourceMetadata full(long size, boolean supportsRanges, 
                                      String contentType, String lastModified, String etag) {
        return new ResourceMetadata(size, supportsRanges, true, contentType, lastModified, etag);
    }
    
    /**
     * Checks if the resource has a valid size.
     * 
     * @return true if the resource exists and has a non-negative size
     */
    public boolean hasValidSize() {
        return exists && size >= 0;
    }
}