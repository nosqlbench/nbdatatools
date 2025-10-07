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

import java.util.Objects;

/// Base class for all simulation events.
/// 
/// Events represent discrete actions that occur at specific points in
/// simulation time. Each event has a scheduled time and a type that
/// determines how it will be processed.
/// 
/// The simulation engine processes events in chronological order,
/// advancing the simulation clock to each event's time as it is processed.
/// 
/// Common event types include:
/// - Read requests arriving
/// - Downloads starting
/// - Downloads completing
/// - Failures occurring
/// - Retries being scheduled
public abstract class SimulationEvent implements Comparable<SimulationEvent> {
    
    private final double scheduledTime;
    private final EventType type;
    private final long eventId;
    
    private static long nextEventId = 0;
    
    /// Creates a new simulation event.
    /// 
    /// @param scheduledTime The simulation time when this event should occur
    /// @param type The type of event
    protected SimulationEvent(double scheduledTime, EventType type) {
        if (scheduledTime < 0) {
            throw new IllegalArgumentException("Event time cannot be negative");
        }
        this.scheduledTime = scheduledTime;
        this.type = type;
        this.eventId = nextEventId++;
    }
    
    /// Gets the simulation time when this event is scheduled to occur.
    /// 
    /// @return The scheduled time in simulation seconds
    public double getScheduledTime() {
        return scheduledTime;
    }
    
    /// Gets the type of this event.
    /// 
    /// @return The event type
    public EventType getType() {
        return type;
    }
    
    /// Gets the unique identifier for this event.
    /// 
    /// @return The event ID
    public long getEventId() {
        return eventId;
    }
    
    /// Processes this event in the simulation.
    /// 
    /// This method is called by the simulation engine when the event's
    /// scheduled time is reached. It should update the simulation state
    /// and potentially generate new events.
    /// 
    /// @param simulation The simulation context
    public abstract void process(EventDrivenSimulation simulation);
    
    @Override
    public int compareTo(SimulationEvent other) {
        // Sort by time first, then by event ID for stability
        int timeComparison = Double.compare(this.scheduledTime, other.scheduledTime);
        if (timeComparison != 0) {
            return timeComparison;
        }
        return Long.compare(this.eventId, other.eventId);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimulationEvent that = (SimulationEvent) o;
        return eventId == that.eventId;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }
    
    @Override
    public String toString() {
        return String.format("%s[id=%d, time=%.3f]", 
            getClass().getSimpleName(), eventId, scheduledTime);
    }
    
    /// Event types for categorization and statistics.
    public enum EventType {
        READ_REQUEST,
        DOWNLOAD_START,
        DOWNLOAD_COMPLETE,
        DOWNLOAD_FAILED,
        RETRY_SCHEDULED,
        CHUNK_VALIDATED,
        SIMULATION_END
    }
}

/// Event representing a read request arriving.
class ReadRequestEvent extends SimulationEvent {
    private final long offset;
    private final int length;
    
    public ReadRequestEvent(double scheduledTime, long offset, int length) {
        super(scheduledTime, EventType.READ_REQUEST);
        this.offset = offset;
        this.length = length;
    }
    
    public long getOffset() { return offset; }
    public int getLength() { return length; }
    
    @Override
    public void process(EventDrivenSimulation simulation) {
        simulation.handleReadRequest(this);
    }
}

/// Event representing a download starting.
class DownloadStartEvent extends SimulationEvent {
    private final int nodeIndex;
    private final long byteOffset;
    private final long byteSize;
    private final int connectionId;
    
    public DownloadStartEvent(double scheduledTime, int nodeIndex, long byteOffset, 
                             long byteSize, int connectionId) {
        super(scheduledTime, EventType.DOWNLOAD_START);
        this.nodeIndex = nodeIndex;
        this.byteOffset = byteOffset;
        this.byteSize = byteSize;
        this.connectionId = connectionId;
    }
    
    public int getNodeIndex() { return nodeIndex; }
    public long getByteOffset() { return byteOffset; }
    public long getByteSize() { return byteSize; }
    public int getConnectionId() { return connectionId; }
    
    @Override
    public void process(EventDrivenSimulation simulation) {
        simulation.handleDownloadStart(this);
    }
}

/// Event representing a download completing.
class DownloadCompleteEvent extends SimulationEvent {
    private final int nodeIndex;
    private final int connectionId;
    private final double startTime;
    private final long bytesTransferred;
    
    public DownloadCompleteEvent(double scheduledTime, int nodeIndex, int connectionId,
                                double startTime, long bytesTransferred) {
        super(scheduledTime, EventType.DOWNLOAD_COMPLETE);
        this.nodeIndex = nodeIndex;
        this.connectionId = connectionId;
        this.startTime = startTime;
        this.bytesTransferred = bytesTransferred;
    }
    
    public int getNodeIndex() { return nodeIndex; }
    public int getConnectionId() { return connectionId; }
    public double getStartTime() { return startTime; }
    public long getBytesTransferred() { return bytesTransferred; }
    public double getDuration() { return getScheduledTime() - startTime; }
    
    @Override
    public void process(EventDrivenSimulation simulation) {
        simulation.handleDownloadComplete(this);
    }
}

/// Event representing a download failure.
class DownloadFailedEvent extends SimulationEvent {
    private final int nodeIndex;
    private final int connectionId;
    private final String reason;
    
    public DownloadFailedEvent(double scheduledTime, int nodeIndex, int connectionId, String reason) {
        super(scheduledTime, EventType.DOWNLOAD_FAILED);
        this.nodeIndex = nodeIndex;
        this.connectionId = connectionId;
        this.reason = reason;
    }
    
    public int getNodeIndex() { return nodeIndex; }
    public int getConnectionId() { return connectionId; }
    public String getReason() { return reason; }
    
    @Override
    public void process(EventDrivenSimulation simulation) {
        simulation.handleDownloadFailed(this);
    }
}