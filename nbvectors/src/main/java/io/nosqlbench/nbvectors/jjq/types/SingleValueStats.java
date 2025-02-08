package io.nosqlbench.nbvectors.jjq.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

public class SingleValueStats {

  @JsonProperty
  private long idx;
  @JsonProperty
  private long count;

  public SingleValueStats() {}
  public SingleValueStats(long idx, long count) {
    this.idx = idx;
    this.count = count;
  }

  @JsonSetter("count")
  public void setCount(long count) {
    this.count = count;
  }

  @JsonSetter("idx")
  public void setIdx(long idx) {
    this.idx = idx;
  }
  public long getCount() {
    return this.count;
  }

  public long getIdx() {
    return idx;
  }

  public synchronized void increment() {
    this.count+=1;
  }
}
