/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.nosqlbench.command.common;

import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.vectordata.spec.datasets.types.VectorDatasetView;

import java.nio.file.Path;
import java.util.AbstractList;

/// Adapter to expose a DatasetView as a VectorFileArray.
///
/// @param <T> the vector element type
public final class DatasetViewVectorFileArray<T> extends AbstractList<T> implements VectorFileArray<T> {

    private final VectorDatasetView<T> vectorDatasetView;
    private final String name;

    /// Creates a new DatasetViewVectorFileArray wrapping the given dataset view.
    /// @param vectorDatasetView the dataset view to wrap
    /// @param name the name for this array
    public DatasetViewVectorFileArray(VectorDatasetView<T> vectorDatasetView, String name) {
        if (vectorDatasetView == null) {
            throw new IllegalArgumentException("datasetView must not be null");
        }
        this.vectorDatasetView = vectorDatasetView;
        this.name = name != null ? name : "dataset";
    }

    @Override
    public T get(int index) {
        return vectorDatasetView.get(index);
    }

    @Override
    public int size() {
        return vectorDatasetView.getCount();
    }

    @Override
    public int getSize() {
        return vectorDatasetView.getCount();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void open(Path filePath) {
        // No-op: DatasetView is already open
    }

    @Override
    public void close() {
        // No-op: DatasetView lifecycle handled elsewhere
    }
}
