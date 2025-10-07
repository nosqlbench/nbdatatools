/// The hdf5 command is an umbrella command for all other hdf5-specific commands
///
/// This package contains the main {@link io.nosqlbench.command.hdf5.CMD_hdf5} command which serves as an
/// umbrella for the following subcommands:
///
/// - `tag`: Read or write HDF5 attributes ({@link io.nosqlbench.command.hdf5.subcommands.TagHdf5})
/// - `show`: Show details of HDF5 KNN test data files ({@link io.nosqlbench.command.hdf5.subcommands.ShowHdf5})
/// - `build`: Build HDF5 KNN test data answer-keys from JSON ({@link io.nosqlbench.command.hdf5.subcommands.BuildHdf5})
package io.nosqlbench.command.hdf5;

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

