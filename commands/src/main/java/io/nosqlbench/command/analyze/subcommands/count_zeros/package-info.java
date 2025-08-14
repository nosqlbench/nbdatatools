/// ## count_zeros
///
/// This command should count the "zero vectors" in any supported file type.
/// * Arguments are files to count zeroes in.
/// * A "zero vector" is one which has zero as its value for every dimensional component.
/// * A progress bar should be provided for both the file in the set of all files specified as
/// well as the progress within the current file.
/// * At the end, a small summary should be printed out for each file scanned indicating the
/// number of zero vectors and the total number of vectors scanned.
/// * The implementation should not do any direct file IO on its own, but should instead indirect
///  it's access to such files through [io.nosqlbench.nbdatatools.api.services.VectorFileIO]
///  methods.
package io.nosqlbench.command.analyze.subcommands.count_zeros;

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

