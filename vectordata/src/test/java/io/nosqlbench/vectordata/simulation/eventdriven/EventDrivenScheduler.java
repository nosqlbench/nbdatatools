package io.nosqlbench.vectordata.simulation.eventdriven;

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

import java.util.*;

/// Base class for event-driven scheduler implementations.
/// 
/// Event-driven schedulers make decisions about which chunks to download
/// based on the current simulation state. Unlike real-time schedulers,
/// these operate on computed time and generate events that will occur
/// in the future.
/// 
/// The scheduler maintains:
/// - Knowledge of which chunks are already valid
/// - Currently in-progress downloads
/// - Available network connections
/// - Pending download queue
public abstract class EventDrivenScheduler {
    
    protected final String name;
    protected final Map<Integer, Double> inProgressDownloads;
    protected final Set<Integer> validChunks;
    
    /// Creates an event-driven scheduler.
    /// 
    /// @param name Human-readable name for this scheduler
    protected EventDrivenScheduler(String name) {
        this.name = name;
        this.inProgressDownloads = new HashMap<>();
        this.validChunks = new HashSet<>();
    }
    
    /// Schedules downloads to satisfy a read request.
    /// 
    /// This method examines the requested byte range and determines which
    /// chunks need to be downloaded. It returns a list of download events
    /// that should be scheduled in the simulation.
    /// 
    /// @param currentTime Current simulation time
    /// @param offset Starting byte offset of the read
    /// @param length Number of bytes to read
    /// @param chunkSize Size of each chunk
    /// @param fileSize Total file size
    /// @param availableConnections Number of available network connections
    /// @return List of download start events to schedule
    public abstract List<DownloadStartEvent> scheduleDownloads(
        double currentTime, long offset, int length, long chunkSize, long fileSize,
        int availableConnections);
    
    /// Marks a chunk as valid after successful download.
    /// 
    /// @param chunkIndex The chunk that was downloaded
    public void markChunkValid(int chunkIndex) {
        validChunks.add(chunkIndex);
        inProgressDownloads.remove(chunkIndex);
    }
    
    /// Marks a download as failed.
    /// 
    /// @param chunkIndex The chunk that failed to download
    public void markDownloadFailed(int chunkIndex) {
        inProgressDownloads.remove(chunkIndex);
    }
    
    /// Records that a download is in progress.
    /// 
    /// @param chunkIndex The chunk being downloaded
    /// @param startTime When the download started
    public void recordDownloadStart(int chunkIndex, double startTime) {
        inProgressDownloads.put(chunkIndex, startTime);
    }
    
    /// Checks if a chunk is already valid.
    /// 
    /// @param chunkIndex The chunk to check
    /// @return true if the chunk is valid
    protected boolean isChunkValid(int chunkIndex) {
        return validChunks.contains(chunkIndex);
    }
    
    /// Checks if a chunk is currently being downloaded.
    /// 
    /// @param chunkIndex The chunk to check
    /// @return true if the chunk is being downloaded
    protected boolean isChunkInProgress(int chunkIndex) {
        return inProgressDownloads.containsKey(chunkIndex);
    }
    
    /// Gets the chunk index for a given byte offset.
    /// 
    /// @param offset Byte offset in the file
    /// @param chunkSize Size of each chunk
    /// @return Chunk index
    protected int getChunkIndex(long offset, long chunkSize) {
        return (int) (offset / chunkSize);
    }
    
    /// Gets the scheduler name.
    /// 
    /// @return Scheduler name
    public String getName() {
        return name;
    }
    
    /// Gets statistics about the scheduler state.
    /// 
    /// @return Scheduler statistics
    public SchedulerStats getStats() {
        return new SchedulerStats(
            validChunks.size(),
            inProgressDownloads.size(),
            name
        );
    }
    
    /// Statistics about scheduler state.
    public static class SchedulerStats {
        private final int validChunks;
        private final int inProgressDownloads;
        private final String schedulerName;
        
        public SchedulerStats(int validChunks, int inProgressDownloads, String schedulerName) {
            this.validChunks = validChunks;
            this.inProgressDownloads = inProgressDownloads;
            this.schedulerName = schedulerName;
        }
        
        public int getValidChunks() { return validChunks; }
        public int getInProgressDownloads() { return inProgressDownloads; }
        public String getSchedulerName() { return schedulerName; }
    }
}

/// Default event-driven scheduler implementation.
/// 
/// This scheduler downloads only the chunks that are explicitly needed
/// for the current read request, without any prefetching or speculation.
class DefaultEventDrivenScheduler extends EventDrivenScheduler {
    
    public DefaultEventDrivenScheduler() {
        super("Default Event-Driven");
    }
    
    @Override
    public List<DownloadStartEvent> scheduleDownloads(double currentTime, long offset, int length,
                                                     long chunkSize, long fileSize, int availableConnections) {
        List<DownloadStartEvent> downloads = new ArrayList<>();
        
        // Find range of chunks needed
        int startChunk = getChunkIndex(offset, chunkSize);
        int endChunk = getChunkIndex(offset + length - 1, chunkSize);
        
        int connectionId = 0;
        for (int chunkIndex = startChunk; chunkIndex <= endChunk && connectionId < availableConnections; chunkIndex++) {
            if (!isChunkValid(chunkIndex) && !isChunkInProgress(chunkIndex)) {
                long chunkOffset = chunkIndex * chunkSize;
                long chunkEnd = Math.min((chunkIndex + 1) * chunkSize, fileSize);
                long chunkByteSize = chunkEnd - chunkOffset;
                
                downloads.add(new DownloadStartEvent(
                    currentTime, chunkIndex, chunkOffset, chunkByteSize, connectionId++));
                recordDownloadStart(chunkIndex, currentTime);
            }
        }
        
        return downloads;
    }
}

/// Aggressive event-driven scheduler implementation.
/// 
/// This scheduler prefetches additional chunks beyond what's immediately
/// needed, optimizing for high-bandwidth scenarios where over-downloading
/// is acceptable.
class AggressiveEventDrivenScheduler extends EventDrivenScheduler {
    
    private static final int PREFETCH_CHUNKS = 4;
    
    public AggressiveEventDrivenScheduler() {
        super("Aggressive Event-Driven");
    }
    
    @Override
    public List<DownloadStartEvent> scheduleDownloads(double currentTime, long offset, int length,
                                                     long chunkSize, long fileSize, int availableConnections) {
        List<DownloadStartEvent> downloads = new ArrayList<>();
        
        // Find range of chunks needed
        int startChunk = getChunkIndex(offset, chunkSize);
        int endChunk = getChunkIndex(offset + length - 1, chunkSize);
        
        // Expand range for prefetching
        int expandedStart = Math.max(0, startChunk - 1);
        int expandedEnd = Math.min((int) (fileSize / chunkSize), endChunk + PREFETCH_CHUNKS);
        
        int connectionId = 0;
        for (int chunkIndex = expandedStart; chunkIndex <= expandedEnd && connectionId < availableConnections; chunkIndex++) {
            if (!isChunkValid(chunkIndex) && !isChunkInProgress(chunkIndex)) {
                long chunkOffset = chunkIndex * chunkSize;
                long chunkEnd = Math.min((chunkIndex + 1) * chunkSize, fileSize);
                long chunkByteSize = chunkEnd - chunkOffset;
                
                downloads.add(new DownloadStartEvent(
                    currentTime, chunkIndex, chunkOffset, chunkByteSize, connectionId++));
                recordDownloadStart(chunkIndex, currentTime);
            }
        }
        
        return downloads;
    }
}

/// Conservative event-driven scheduler implementation.
/// 
/// This scheduler downloads only exactly what's needed, minimizing
/// bandwidth usage at the cost of potentially more requests.
class ConservativeEventDrivenScheduler extends EventDrivenScheduler {
    
    public ConservativeEventDrivenScheduler() {
        super("Conservative Event-Driven");
    }
    
    @Override
    public List<DownloadStartEvent> scheduleDownloads(double currentTime, long offset, int length,
                                                     long chunkSize, long fileSize, int availableConnections) {
        List<DownloadStartEvent> downloads = new ArrayList<>();
        
        // Only download chunks that are explicitly needed
        int startChunk = getChunkIndex(offset, chunkSize);
        int endChunk = getChunkIndex(offset + length - 1, chunkSize);
        
        // Prioritize chunks in order of access
        List<Integer> neededChunks = new ArrayList<>();
        for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
            if (!isChunkValid(chunkIndex) && !isChunkInProgress(chunkIndex)) {
                neededChunks.add(chunkIndex);
            }
        }
        
        // Download up to available connections, but prefer to use fewer
        int connectionsToUse = Math.min(availableConnections, Math.max(1, neededChunks.size() / 2));
        
        int connectionId = 0;
        for (int chunkIndex : neededChunks) {
            if (connectionId >= connectionsToUse) break;
            
            long chunkOffset = chunkIndex * chunkSize;
            long chunkEnd = Math.min((chunkIndex + 1) * chunkSize, fileSize);
            long chunkByteSize = chunkEnd - chunkOffset;
            
            downloads.add(new DownloadStartEvent(
                currentTime, chunkIndex, chunkOffset, chunkByteSize, connectionId++));
            recordDownloadStart(chunkIndex, currentTime);
        }
        
        return downloads;
    }
}

/// Adaptive event-driven scheduler implementation.
/// 
/// This scheduler adjusts its strategy based on observed network performance
/// and access patterns.
class AdaptiveEventDrivenScheduler extends EventDrivenScheduler {
    
    private int aggressivenessLevel = 2; // 1=conservative, 5=aggressive
    private final List<Double> recentTransferTimes = new ArrayList<>();
    private final List<Boolean> recentHits = new ArrayList<>();
    private static final int HISTORY_SIZE = 20;
    
    public AdaptiveEventDrivenScheduler() {
        super("Adaptive Event-Driven");
    }
    
    @Override
    public List<DownloadStartEvent> scheduleDownloads(double currentTime, long offset, int length,
                                                     long chunkSize, long fileSize, int availableConnections) {
        // Adapt strategy based on recent performance
        adaptStrategy();
        
        List<DownloadStartEvent> downloads = new ArrayList<>();
        
        int startChunk = getChunkIndex(offset, chunkSize);
        int endChunk = getChunkIndex(offset + length - 1, chunkSize);
        
        // Adjust prefetching based on aggressiveness
        int prefetchAmount = Math.max(0, aggressivenessLevel - 2);
        int expandedStart = Math.max(0, startChunk - (prefetchAmount / 2));
        int expandedEnd = Math.min((int) (fileSize / chunkSize), endChunk + prefetchAmount);
        
        // Adjust connection usage based on aggressiveness
        int connectionsToUse = Math.min(availableConnections, 
            Math.max(1, availableConnections * aggressivenessLevel / 5));
        
        int connectionId = 0;
        for (int chunkIndex = expandedStart; chunkIndex <= expandedEnd && connectionId < connectionsToUse; chunkIndex++) {
            if (!isChunkValid(chunkIndex) && !isChunkInProgress(chunkIndex)) {
                long chunkOffset = chunkIndex * chunkSize;
                long chunkEnd = Math.min((chunkIndex + 1) * chunkSize, fileSize);
                long chunkByteSize = chunkEnd - chunkOffset;
                
                downloads.add(new DownloadStartEvent(
                    currentTime, chunkIndex, chunkOffset, chunkByteSize, connectionId++));
                recordDownloadStart(chunkIndex, currentTime);
            }
        }
        
        return downloads;
    }
    
    /// Records performance data for adaptation.
    /// 
    /// @param transferTime Time taken for a transfer
    /// @param wasNeeded Whether the downloaded chunk was actually needed
    public void recordPerformance(double transferTime, boolean wasNeeded) {
        recentTransferTimes.add(transferTime);
        recentHits.add(wasNeeded);
        
        // Keep only recent history
        if (recentTransferTimes.size() > HISTORY_SIZE) {
            recentTransferTimes.remove(0);
            recentHits.remove(0);
        }
    }
    
    /// Adapts the scheduling strategy based on recent performance.
    private void adaptStrategy() {
        if (recentTransferTimes.size() < 5) return; // Need some history
        
        // Calculate hit rate (how often prefetched chunks were useful)
        double hitRate = recentHits.stream().mapToDouble(hit -> hit ? 1.0 : 0.0).average().orElse(0.5);
        
        // Calculate average transfer time
        double avgTransferTime = recentTransferTimes.stream().mapToDouble(Double::doubleValue).average().orElse(1.0);
        
        // Adapt based on performance
        if (hitRate > 0.8 && avgTransferTime < 0.5) {
            // Good hit rate and fast transfers - be more aggressive
            aggressivenessLevel = Math.min(5, aggressivenessLevel + 1);
        } else if (hitRate < 0.4 || avgTransferTime > 2.0) {
            // Poor hit rate or slow transfers - be more conservative
            aggressivenessLevel = Math.max(1, aggressivenessLevel - 1);
        }
    }
}