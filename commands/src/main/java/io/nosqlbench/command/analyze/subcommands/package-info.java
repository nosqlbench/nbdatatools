/// Subcommands for the analyze command
///
/// This package contains subcommands that help understand the contents of test data files.
///
/// ## count_zeros
///
/// This command counts the "zero vectors" in any supported file type.
/// * Arguments are files to count zeroes in.
/// * A "zero vector" is one which has zero as its value for every dimensional component.
/// * A progress bar is provided for both the file in the set of all files specified as
/// well as the progress within the current file.
/// * At the end, a small summary is printed out for each file scanned indicating the
/// number of zero vectors and the total number of vectors scanned.
/// * The implementation does not do any direct file IO on its own, but instead indirects
///  its access to such files through [io.nosqlbench.nbdatatools.api.services.VectorFileIO]
///  methods.
///
/// ## verify_knn
///
/// This command reads data from an HDF5 file in the standard vector KNN answer key format,
/// computing correct neighborhoods and comparing them to the provided ones.
/// * Arguments are HDF5 files to verify.
/// * The command computes KNN neighborhoods and compares them against the answer-key data given.
/// * It is a pure Java implementation which requires no other vector processing libraries or hardware.
/// * It is not as fast as a GPU or TPU, but it is a simpler implementation which makes it easier
///   to rely on as a basic verification tool.
/// * This utility is meant to be used in concert with other tools which are faster, but which may
///   benefit from the assurance of a basic coherence check.
package io.nosqlbench.command.analyze.subcommands;

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

