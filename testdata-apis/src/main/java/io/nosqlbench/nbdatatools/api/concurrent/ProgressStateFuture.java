package io.nosqlbench.nbdatatools.api.concurrent;

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


import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A state-driven progress tracking future where progress values are set as properties.
 * When the future is marked complete, the current work is automatically set to total work.
 */
public class ProgressStateFuture<T> extends CompletableFuture<T> implements
                                                                         ProgressIndicator<T>
{
    
    private final CompletableFuture<T> underlying;
    private final AtomicReference<Double> totalWork = new AtomicReference<>(0.0);
    private final AtomicReference<Double> currentWork = new AtomicReference<>(0.0);
    private final List<BiConsumer<Double, Double>> progressListeners = new CopyOnWriteArrayList<>();
    
    /**
     * Creates a new state-driven progress tracking future.
     * 
     * @param underlying The existing CompletableFuture to wrap
     */
    public ProgressStateFuture(CompletableFuture<T> underlying) {
        this.underlying = underlying;
        
        // Forward completion from underlying future
        underlying.whenComplete((result, throwable) -> {
            // Automatically set current work to total work when completing
            currentWork.set(totalWork.get());
            
            if (throwable != null) {
                this.completeExceptionally(throwable);
            } else {
                this.complete(result);
            }
        });
    }
    
    @Override
    public double getTotalWork() {
        return totalWork.get();
    }
    
    @Override
    public double getCurrentWork() {
        return currentWork.get();
    }
    
    /**
     * Sets the total work value.
     * 
     * @param totalWork The total amount of work to be done
     */
    public void setTotalWork(double totalWork) {
        this.totalWork.set(totalWork);
    }
    
    /**
     * Sets the current work value.
     * 
     * @param currentWork The current amount of work completed
     */
    public void setCurrentWork(double currentWork) {
        this.currentWork.set(currentWork);
        notifyProgressListeners();
    }
    
    /**
     * Updates the current work by adding the specified amount.
     * 
     * @param workDelta The amount of work to add to current work
     */
    public void addCurrentWork(double workDelta) {
        currentWork.updateAndGet(current -> current + workDelta);
        notifyProgressListeners();
    }
    
    private void notifyProgressListeners() {
        double current = getCurrentWork();
        double total = getTotalWork();
        for (BiConsumer<Double, Double> listener : progressListeners) {
            try {
                listener.accept(current, total);
            } catch (Exception e) {
                // Ignore listener exceptions to prevent breaking progress tracking
            }
        }
    }
    
    @Override
    public void addProgressListener(BiConsumer<Double, Double> progressListener) {
        progressListeners.add(progressListener);
    }
    
    @Override
    public boolean removeProgressListener(BiConsumer<Double, Double> progressListener) {
        return progressListeners.remove(progressListener);
    }
    
    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return this;
    }
}
