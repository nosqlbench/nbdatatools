/// # Synopsis
/// This package contains methods for understanding vector spaces.
/// It should support the following:
/// 1. Analysis of vector spaces to determine unique characteristics
/// 2. Summarization and reporting on vector spaces
/// 3. Extraction of descriptive statistical or other numerical metrics
/// 4. Possible construction of similar vector spaces from extracted measures
///
/// ## Data Flow
/// The way that calculations in this package will be organized is thus:
/// 1. There will be multiple computational artifacts, i.e. JSON files with data representing the
/// results of a particular kind of analysis.
/// 2. Some types of computations will benefit from using these computational artifacts as
/// intermediate cached results.
/// 3. Each new computation which is added to the analyzer logic will be positioned on a
/// maintained DAG view, which makes the incremental processing steps obvious. This should be
/// maintained in analysis_dag.md.
///
/// ## Included Measures
/// This is the list of measures to be included in the analyzer phase. It will be added to over
/// time, and thus the DAG view will expand with it. Each measure will have a one-word mnemonic
/// to capture it's role within the DAG.
///
/// 1. LID: Local Intrinsic Dimensionality
/// 2. Margin: Nearest-Neighbor Margin
/// 3. Hubness: Reverse-kNN in-degree skew
///
/// ## Libraries
/// Initially, this module will rely solely on the ML4J and closely related libraries, since they
///  provide a pre-bundled GPU-enabling library format.
///
package io.nosqlbench.vshapes;

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

