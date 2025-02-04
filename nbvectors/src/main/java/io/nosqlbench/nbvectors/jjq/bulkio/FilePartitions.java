package io.nosqlbench.nbvectors.jjq.bulkio;

import java.util.ArrayList;

public class FilePartitions extends ArrayList<FilePartition> {
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int index = 0;
    for (FilePartition filePartition : this) {
      sb.append(String.format("%05d ",index++)).append(filePartition.toString()).append("\n");
    }
    return sb.toString();
  }
}
