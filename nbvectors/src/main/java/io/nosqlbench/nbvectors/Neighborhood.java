package io.nosqlbench.nbvectors;

import java.util.ArrayList;
import java.util.Arrays;

public class Neighborhood extends ArrayList<NeighborIndex> {
  public Neighborhood(NeighborIndex[] lastResults) {
    this.addAll(Arrays.asList(lastResults));
  }
  public Neighborhood() {
    super();
  }
}
