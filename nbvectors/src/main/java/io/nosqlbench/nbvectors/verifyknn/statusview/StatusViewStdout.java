package io.nosqlbench.nbvectors.verifyknn.statusview;

import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.computation.NeighborhoodComparison;

/// Print status events to stdout, buffering until the end if needed,
/// but flushing intermittently if not
public class StatusViewStdout implements StatusView {

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
  public void onQueryVector(LongIndexedFloatVector vector, long index, long end) {
    sb.append(++currentQueryVector).append("/").append(totalQueryVectors).append(": ");
    sb.append(vector);
    flushIf();
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
