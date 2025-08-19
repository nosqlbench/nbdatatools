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
import java.util.function.Supplier;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A wrapper that adds progress tracking to an existing CompletableFuture using callbacks
 * to retrieve progress values. The callbacks are assumed to be thread-safe and non-blocking.
 */
public class ProgressIndicatingFuture<T> extends CompletableFuture<T> implements
                                                                            ProgressIndicator<T>
{
    
    private final CompletableFuture<T> underlying;
    private final Supplier<Double> totalWorkCallback;
    private final Supplier<Double> currentWorkCallback;
    private final List<BiConsumer<Double, Double>> progressListeners = new CopyOnWriteArrayList<>();
    
    /**
     * Creates a new callback-based progress tracking future.
     * 
     * @param underlying The existing CompletableFuture to wrap
     * @param totalWorkCallback Callback to get total work (must be thread-safe and non-blocking)
     * @param currentWorkCallback Callback to get current work (must be thread-safe and non-blocking)
     */
    public ProgressIndicatingFuture(CompletableFuture<T> underlying,
                                    Supplier<Double> totalWorkCallback,
                                    Supplier<Double> currentWorkCallback) {
        this.underlying = underlying;
        this.totalWorkCallback = totalWorkCallback;
        this.currentWorkCallback = currentWorkCallback;
        
        // Forward completion from underlying future
        underlying.whenComplete((result, throwable) -> {
            if (throwable != null) {
                this.completeExceptionally(throwable);
            } else {
                this.complete(result);
            }
        });
    }
    
    @Override
    public double getTotalWork() {
        return totalWorkCallback.get();
    }
    
    @Override
    public double getCurrentWork() {
        return currentWorkCallback.get();
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
