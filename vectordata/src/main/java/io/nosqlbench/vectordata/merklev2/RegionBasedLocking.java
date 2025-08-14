package io.nosqlbench.vectordata.merklev2;

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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/// Region-based locking mechanism to reduce contention in MAFileChannel.
/// 
/// Instead of using a single global read/write lock that blocks all concurrent
/// read operations, this implementation provides fine-grained locking based
/// on file regions. This allows multiple readers to operate concurrently
/// as long as they access different regions of the file.
/// 
/// Key benefits:
/// 1. Concurrent reads from different regions don't block each other
/// 2. Reduced lock contention under high concurrency
/// 3. Better scalability for workloads with distributed access patterns
/// 4. Maintains correctness guarantees for overlapping regions
/// 
/// The implementation uses a segmented approach where the file is divided
/// into logical regions, each with its own read/write lock.
public class RegionBasedLocking {
    
    /// Size of each locking region in bytes
    private static final long REGION_SIZE = 1024 * 1024; // 1MB regions
    
    /// Map of region index to its corresponding lock
    private final ConcurrentMap<Long, ReadWriteLock> regionLocks = new ConcurrentHashMap<>();
    
    /// Global lock for operations that affect the entire file
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();
    
    /// Gets the read lock for a specific byte range.
    /// 
    /// For ranges that span multiple regions, this returns a composite lock
    /// that must acquire all relevant region locks in a consistent order
    /// to prevent deadlocks.
    /// 
    /// @param offset Starting byte offset
    /// @param length Number of bytes
    /// @return Lock handle that can be acquired and released
    public RegionLockHandle getReadLock(long offset, int length) {
        long startRegion = offset / REGION_SIZE;
        long endRegion = (offset + length - 1) / REGION_SIZE;
        
        if (startRegion == endRegion) {
            // Single region - simple case
            ReadWriteLock regionLock = getOrCreateRegionLock(startRegion);
            return new SingleRegionLockHandle(regionLock.readLock(), false);
        } else {
            // Multiple regions - need composite lock
            return new MultiRegionLockHandle(startRegion, endRegion, false);
        }
    }
    
    /// Gets the write lock for a specific byte range.
    /// 
    /// Write operations require exclusive access to all affected regions.
    /// 
    /// @param offset Starting byte offset
    /// @param length Number of bytes
    /// @return Lock handle that can be acquired and released
    public RegionLockHandle getWriteLock(long offset, int length) {
        long startRegion = offset / REGION_SIZE;
        long endRegion = (offset + length - 1) / REGION_SIZE;
        
        if (startRegion == endRegion) {
            // Single region - simple case
            ReadWriteLock regionLock = getOrCreateRegionLock(startRegion);
            return new SingleRegionLockHandle(regionLock.writeLock(), true);
        } else {
            // Multiple regions - need composite lock
            return new MultiRegionLockHandle(startRegion, endRegion, true);
        }
    }
    
    /// Gets the global read lock for operations that need to read the entire file state.
    /// 
    /// @return Global read lock handle
    public RegionLockHandle getGlobalReadLock() {
        return new SingleRegionLockHandle(globalLock.readLock(), false);
    }
    
    /// Gets the global write lock for operations that modify the entire file structure.
    /// 
    /// @return Global write lock handle
    public RegionLockHandle getGlobalWriteLock() {
        return new SingleRegionLockHandle(globalLock.writeLock(), true);
    }
    
    /// Gets or creates a lock for the specified region.
    private ReadWriteLock getOrCreateRegionLock(long regionIndex) {
        return regionLocks.computeIfAbsent(regionIndex, k -> new ReentrantReadWriteLock());
    }
    
    /// Interface for lock handles that can be acquired and released.
    public interface RegionLockHandle {
        /// Acquires the lock.
        void lock();
        
        /// Releases the lock.
        void unlock();
        
        /// Checks if this is a write lock.
        /// @return true if this is a write lock, false for read lock
        boolean isWriteLock();
    }
    
    /// Lock handle for a single region.
    private static class SingleRegionLockHandle implements RegionLockHandle {
        private final java.util.concurrent.locks.Lock lock;
        private final boolean isWrite;
        
        SingleRegionLockHandle(java.util.concurrent.locks.Lock lock, boolean isWrite) {
            this.lock = lock;
            this.isWrite = isWrite;
        }
        
        @Override
        public void lock() {
            lock.lock();
        }
        
        @Override
        public void unlock() {
            lock.unlock();
        }
        
        @Override
        public boolean isWriteLock() {
            return isWrite;
        }
    }
    
    /// Lock handle for multiple regions that must be acquired in order.
    private class MultiRegionLockHandle implements RegionLockHandle {
        private final long startRegion;
        private final long endRegion;
        private final boolean isWrite;
        private boolean locked = false;
        
        MultiRegionLockHandle(long startRegion, long endRegion, boolean isWrite) {
            this.startRegion = startRegion;
            this.endRegion = endRegion;
            this.isWrite = isWrite;
        }
        
        @Override
        public void lock() {
            if (locked) {
                throw new IllegalStateException("Lock already acquired");
            }
            
            // Acquire locks in order to prevent deadlocks
            for (long region = startRegion; region <= endRegion; region++) {
                ReadWriteLock regionLock = getOrCreateRegionLock(region);
                if (isWrite) {
                    regionLock.writeLock().lock();
                } else {
                    regionLock.readLock().lock();
                }
            }
            locked = true;
        }
        
        @Override
        public void unlock() {
            if (!locked) {
                throw new IllegalStateException("Lock not acquired");
            }
            
            // Release locks in reverse order
            for (long region = endRegion; region >= startRegion; region--) {
                ReadWriteLock regionLock = regionLocks.get(region);
                if (regionLock != null) {
                    if (isWrite) {
                        regionLock.writeLock().unlock();
                    } else {
                        regionLock.readLock().unlock();
                    }
                }
            }
            locked = false;
        }
        
        @Override
        public boolean isWriteLock() {
            return isWrite;
        }
    }
    
    /// Gets statistics about the current locking state.
    /// 
    /// @return Statistics about region locks
    public LockingStats getStats() {
        return new LockingStats(regionLocks.size(), REGION_SIZE);
    }
    
    /// Statistics about the region-based locking system.
    public static class LockingStats {
        private final int activeRegions;
        private final long regionSize;
        
        LockingStats(int activeRegions, long regionSize) {
            this.activeRegions = activeRegions;
            this.regionSize = regionSize;
        }
        
        /// Gets the number of regions that have active locks.
        /// @return Number of active region locks
        public int getActiveRegions() {
            return activeRegions;
        }
        
        /// Gets the size of each locking region.
        /// @return Region size in bytes
        public long getRegionSize() {
            return regionSize;
        }
        
        @Override
        public String toString() {
            return String.format("LockingStats{activeRegions=%d, regionSize=%d}", 
                               activeRegions, regionSize);
        }
    }
}