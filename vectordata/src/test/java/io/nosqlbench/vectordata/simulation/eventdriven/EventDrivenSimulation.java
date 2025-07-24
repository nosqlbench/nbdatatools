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

/// Main event-driven simulation engine.
/// 
/// This class manages the discrete event simulation, processing events
/// in chronological order and maintaining simulation state. The simulation
/// uses computed time rather than real time, making it extremely fast
/// and reproducible.
/// 
/// The simulation maintains:
/// - Event queue ordered by simulation time
/// - Network connection pool
/// - Chunk validation state
/// - Performance statistics
/// 
/// Events are processed in strict chronological order, with the simulation
/// clock advancing to each event's scheduled time as it is processed.
public class EventDrivenSimulation {
    
    private final long fileSize;
    private final long chunkSize;
    private final int totalChunks;
    private final double simulationDuration;
    
    private NetworkModel networkModel;
    private EventDrivenScheduler scheduler;
    private WorkloadGenerator workloadGenerator;
    
    private final PriorityQueue<SimulationEvent> eventQueue;
    private final Set<Integer> validChunks;
    private final Map<Integer, Integer> activeDownloads; // nodeIndex -> connectionId
    private final Set<Integer> availableConnections;
    private final Map<Integer, Double> connectionBusyUntil; // connectionId -> time
    private final Map<Long, ReadRequestEvent> pendingRequests; // requestId -> request event
    private int currentSessionConcurrency = 0; // Current concurrent transfers per session
    
    private double currentTime;
    private final SimulationStatistics statistics;
    
    /// Creates an event-driven simulation.
    /// 
    /// @param fileSize Total size of the file being accessed
    /// @param chunkSize Size of each chunk
    /// @param simulationDuration Duration of simulation in seconds
    public EventDrivenSimulation(long fileSize, long chunkSize, double simulationDuration) {
        this.fileSize = fileSize;
        this.chunkSize = chunkSize;
        this.totalChunks = (int) Math.ceil((double) fileSize / chunkSize);
        this.simulationDuration = simulationDuration;
        
        this.eventQueue = new PriorityQueue<>();
        this.validChunks = new HashSet<>();
        this.activeDownloads = new HashMap<>();
        this.availableConnections = new HashSet<>();
        this.connectionBusyUntil = new HashMap<>();
        this.pendingRequests = new HashMap<>();
        
        this.currentTime = 0.0;
        this.statistics = new SimulationStatistics();
    }
    
    /// Sets the network model for the simulation.
    /// 
    /// @param networkModel The network model to use
    /// @return This simulation for method chaining
    public EventDrivenSimulation withNetworkModel(NetworkModel networkModel) {
        this.networkModel = networkModel;
        
        // Initialize connection pool
        availableConnections.clear();
        connectionBusyUntil.clear();
        for (int i = 0; i < networkModel.getMaxConcurrentConnections(); i++) {
            availableConnections.add(i);
            connectionBusyUntil.put(i, 0.0);
        }
        
        return this;
    }
    
    /// Sets the scheduler for the simulation.
    /// 
    /// @param scheduler The scheduler to use
    /// @return This simulation for method chaining
    public EventDrivenSimulation withScheduler(EventDrivenScheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }
    
    /// Sets the workload generator for the simulation.
    /// 
    /// @param workloadGenerator The workload generator to use
    /// @return This simulation for method chaining
    public EventDrivenSimulation withWorkloadGenerator(WorkloadGenerator workloadGenerator) {
        this.workloadGenerator = workloadGenerator;
        return this;
    }
    
    /// Runs the simulation and returns results.
    /// 
    /// @return Simulation results
    public SimulationResults run() {
        if (networkModel == null || scheduler == null || workloadGenerator == null) {
            throw new IllegalStateException("Network model, scheduler, and workload generator must be set");
        }
        
        // Reset simulation state
        reset();
        
        // Generate initial workload events
        List<ReadRequestEvent> workloadEvents = workloadGenerator.generateEvents(simulationDuration);
        for (ReadRequestEvent event : workloadEvents) {
            eventQueue.offer(event);
        }
        
        // Add simulation end event
        eventQueue.offer(new SimulationEndEvent(simulationDuration));
        
        statistics.recordSimulationStart(currentTime);
        
        // Process events in chronological order
        while (!eventQueue.isEmpty()) {
            SimulationEvent event = eventQueue.poll();
            currentTime = event.getScheduledTime();
            
            // Update connection availability
            updateConnectionAvailability();
            
            // Process the event
            event.process(this);
            
            // Stop if we've exceeded simulation duration
            if (currentTime >= simulationDuration) {
                break;
            }
        }
        
        statistics.recordSimulationEnd(currentTime);
        
        return new SimulationResults(statistics, scheduler.getName(), 
                                   networkModel.getName(), workloadGenerator.getName());
    }
    
    /// Handles a read request event.
    /// 
    /// @param event The read request event
    public void handleReadRequest(ReadRequestEvent event) {
        statistics.recordReadRequest(event.getOffset(), event.getLength(), currentTime);
        
        // Track this request for latency measurement
        long requestId = generateRequestId(event.getOffset(), event.getLength(), currentTime);
        pendingRequests.put(requestId, event);
        
        // Check if request can be satisfied immediately from cache
        if (isRequestSatisfiedByCache(event.getOffset(), event.getLength())) {
            // Request is already satisfied - record completion immediately
            statistics.recordRequestCompletion(event.getOffset(), event.getLength(), 
                                             currentTime, currentTime);
            pendingRequests.remove(requestId);
            return;
        }
        
        // Ask scheduler to determine what downloads are needed
        List<DownloadStartEvent> downloads = scheduler.scheduleDownloads(
            currentTime, event.getOffset(), event.getLength(), chunkSize, fileSize,
            availableConnections.size());
        
        // Schedule the downloads
        for (DownloadStartEvent download : downloads) {
            if (availableConnections.contains(download.getConnectionId())) {
                eventQueue.offer(download);
                statistics.recordDownloadScheduled(download.getNodeIndex(), currentTime);
            }
        }
    }
    
    /// Handles a download start event.
    /// 
    /// @param event The download start event
    public void handleDownloadStart(DownloadStartEvent event) {
        int connectionId = event.getConnectionId();
        int nodeIndex = event.getNodeIndex();
        
        // Reserve the connection and update session concurrency
        availableConnections.remove(connectionId);
        activeDownloads.put(nodeIndex, connectionId);
        currentSessionConcurrency++;
        
        // Compute transfer time with session concurrency consideration
        double transferTime = networkModel.computeTransferTime(event.getByteSize(), 
                                                              getActiveConnectionCount(),
                                                              currentSessionConcurrency);
        
        // Mark connection as busy
        connectionBusyUntil.put(connectionId, currentTime + transferTime);
        
        statistics.recordDownloadStart(nodeIndex, currentTime, event.getByteSize());
        
        // Schedule completion or failure
        if (networkModel.shouldFail()) {
            DownloadFailedEvent failureEvent = new DownloadFailedEvent(
                currentTime + transferTime * 0.5, // Fail partway through
                nodeIndex, connectionId, "Network error");
            eventQueue.offer(failureEvent);
        } else {
            DownloadCompleteEvent completeEvent = new DownloadCompleteEvent(
                currentTime + transferTime, nodeIndex, connectionId, 
                currentTime, event.getByteSize());
            eventQueue.offer(completeEvent);
        }
    }
    
    /// Handles a download complete event.
    /// 
    /// @param event The download complete event
    public void handleDownloadComplete(DownloadCompleteEvent event) {
        int nodeIndex = event.getNodeIndex();
        int connectionId = event.getConnectionId();
        
        // Mark chunk as valid and update session concurrency
        validChunks.add(nodeIndex);
        activeDownloads.remove(nodeIndex);
        scheduler.markChunkValid(nodeIndex);
        currentSessionConcurrency = Math.max(0, currentSessionConcurrency - 1);
        
        // Record performance for adaptive schedulers
        if (scheduler instanceof AdaptiveEventDrivenScheduler) {
            ((AdaptiveEventDrivenScheduler) scheduler).recordPerformance(
                event.getDuration(), true); // Assume downloaded chunks are needed
        }
        
        statistics.recordDownloadComplete(nodeIndex, currentTime, 
                                        event.getBytesTransferred(), event.getDuration());
        
        // Check if any pending requests can now be satisfied
        checkPendingRequestCompletion();
        
        // Connection will be released when it's no longer busy
    }
    
    /// Handles a download failed event.
    /// 
    /// @param event The download failed event
    public void handleDownloadFailed(DownloadFailedEvent event) {
        int nodeIndex = event.getNodeIndex();
        int connectionId = event.getConnectionId();
        
        activeDownloads.remove(nodeIndex);
        scheduler.markDownloadFailed(nodeIndex);
        currentSessionConcurrency = Math.max(0, currentSessionConcurrency - 1);
        
        statistics.recordDownloadFailed(nodeIndex, currentTime, event.getReason());
        
        // Connection will be released when it's no longer busy
        // Could schedule a retry here if desired
    }
    
    /// Updates connection availability based on current time.
    private void updateConnectionAvailability() {
        for (Map.Entry<Integer, Double> entry : connectionBusyUntil.entrySet()) {
            int connectionId = entry.getKey();
            double busyUntil = entry.getValue();
            
            if (currentTime >= busyUntil && !availableConnections.contains(connectionId)) {
                availableConnections.add(connectionId);
            }
        }
    }
    
    /// Gets the number of active connections.
    /// 
    /// @return Number of active connections
    private int getActiveConnectionCount() {
        return networkModel.getMaxConcurrentConnections() - availableConnections.size();
    }
    
    /// Resets simulation state for a new run.
    private void reset() {
        eventQueue.clear();
        validChunks.clear();
        activeDownloads.clear();
        availableConnections.clear();
        connectionBusyUntil.clear();
        pendingRequests.clear();
        currentSessionConcurrency = 0;
        
        currentTime = 0.0;
        statistics.reset();
        
        // Reinitialize connection pool
        if (networkModel != null) {
            for (int i = 0; i < networkModel.getMaxConcurrentConnections(); i++) {
                availableConnections.add(i);
                connectionBusyUntil.put(i, 0.0);
            }
        }
    }
    
    /// Gets current simulation time.
    /// 
    /// @return Current simulation time in seconds
    public double getCurrentTime() {
        return currentTime;
    }
    
    /// Gets the set of valid chunks.
    /// 
    /// @return Set of valid chunk indices
    public Set<Integer> getValidChunks() {
        return Collections.unmodifiableSet(validChunks);
    }
    
    /// Gets simulation statistics.
    /// 
    /// @return Current simulation statistics
    public SimulationStatistics getStatistics() {
        return statistics;
    }
    
    /// Checks if a request can be satisfied by already downloaded chunks.
    /// 
    /// @param offset Byte offset of the request
    /// @param length Length of the request
    /// @return true if request can be satisfied from cache
    private boolean isRequestSatisfiedByCache(long offset, int length) {
        int startChunk = (int) (offset / chunkSize);
        int endChunk = (int) ((offset + length - 1) / chunkSize);
        
        for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
            if (!validChunks.contains(chunkIndex)) {
                return false;
            }
        }
        return true;
    }
    
    /// Checks pending requests for completion and records latencies.
    private void checkPendingRequestCompletion() {
        Iterator<Map.Entry<Long, ReadRequestEvent>> iterator = pendingRequests.entrySet().iterator();
        
        while (iterator.hasNext()) {
            Map.Entry<Long, ReadRequestEvent> entry = iterator.next();
            ReadRequestEvent request = entry.getValue();
            
            if (isRequestSatisfiedByCache(request.getOffset(), request.getLength())) {
                // Request is now satisfied - record completion
                statistics.recordRequestCompletion(request.getOffset(), request.getLength(),
                                                 request.getScheduledTime(), currentTime);
                iterator.remove();
            }
        }
    }
    
    /// Generates a unique request ID for tracking.
    /// 
    /// @param offset Byte offset
    /// @param length Request length
    /// @param time Request time
    /// @return Unique request identifier
    private long generateRequestId(long offset, int length, double time) {
        long timeMillis = (long) (time * 1000000); // Convert to microseconds for uniqueness
        return offset ^ (((long) length) << 32) ^ timeMillis;
    }
}

/// Event representing the end of simulation.
class SimulationEndEvent extends SimulationEvent {
    
    public SimulationEndEvent(double scheduledTime) {
        super(scheduledTime, EventType.SIMULATION_END);
    }
    
    @Override
    public void process(EventDrivenSimulation simulation) {
        // End of simulation - no action needed
    }
}