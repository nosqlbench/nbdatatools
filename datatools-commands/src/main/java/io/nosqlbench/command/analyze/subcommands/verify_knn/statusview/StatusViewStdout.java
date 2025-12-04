package io.nosqlbench.command.analyze.subcommands.verify_knn.statusview;

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

import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.command.analyze.subcommands.verify_knn.computation.NeighborhoodComparison;

/// Print status events to stdout, buffering until the max if needed,
/// but flushing intermittently if not
public class StatusViewStdout implements StatusView {

  /// create a status view with the given number of summaries
  /// @param flushall whether to flush all output immediately, or to wait for the end
  public StatusViewStdout(boolean flushall) {
    this.flushall = flushall;
  }

  private int totalQueryVectors;
  private int currentQueryVector;

  private int errors = 0;
  StringBuilder sb = new StringBuilder();
  boolean flushall = false;

  @Override
  public void onStart(int totalQueryVectors) {
    this.totalQueryVectors = totalQueryVectors;
    sb.append("Total query vectors: ").append(totalQueryVectors).append("\n");
    flushIf();
  }

  private synchronized void flushIf() {
    if (flushall) {
      System.out.print(sb.toString());
      sb.setLength(0);
    }
  }

  @Override
  public void onQueryVector(Indexed<float[]> vector, long index, long end) {
    sb.append(++currentQueryVector).append("/").append(totalQueryVectors).append(": ");
    sb.append("Indexed{index=").append(vector.index()).append(", value=");
    sb.append(formatVector(vector.value()));
    sb.append("}");
    flushIf();
  }

  /// Format a float vector showing first and last components with ellipses in middle
  /// @param vector The vector to format
  /// @return Formatted string like "[0.123, 0.456, ..., 0.789]" (dim=1024)
  private String formatVector(float[] vector) {
    if (vector == null) {
      return "null";
    }
    if (vector.length == 0) {
      return "[]";
    }
    if (vector.length <= 6) {
      // Show all components for short vectors
      StringBuilder sb = new StringBuilder("[");
      for (int i = 0; i < vector.length; i++) {
        if (i > 0) sb.append(", ");
        sb.append(String.format("%.3f", vector[i]));
      }
      sb.append("]");
      return sb.toString();
    }
    // Show first 3 and last 3 components with ellipses
    return String.format("[%.3f, %.3f, %.3f, ..., %.3f, %.3f, %.3f] (dim=%d)",
        vector[0], vector[1], vector[2],
        vector[vector.length-3], vector[vector.length-2], vector[vector.length-1],
        vector.length);
  }


  @Override
  public void onChunk(int chunk, int chunkSize, int totalTrainingVectors) {
    if (chunk==0) {
      sb.append("chunks/").append(chunkSize).append(" ");
    }
    sb.append(".");
    flushIf();
  }

  @Override
  public void onNeighborhoodComparison(NeighborhoodComparison comparison) {
    sb.append("\n");
    sb.append(comparison).append("\n");
    errors += comparison.isError() ? 1 : 0;
    flushIf();
  }

  @Override
  public void end() {
    sb.append("(pass,fail,total)=(").append(totalQueryVectors - errors).append(",").append(errors)
        .append(",").append(totalQueryVectors).append(")").append("\n");
    flushIf();
  }

  @Override
  public void close() throws Exception {
    System.out.print(sb.toString());
  }
}
