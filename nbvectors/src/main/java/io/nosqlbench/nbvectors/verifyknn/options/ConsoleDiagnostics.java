package io.nosqlbench.nbvectors.verifyknn.options;

import org.apache.logging.log4j.Level;

public enum ConsoleDiagnostics {
  FATAL(Level.FATAL),
  ERROR(Level.ERROR),
  WARN(Level.WARN),
  INFO(Level.INFO),
  DEBUG(Level.DEBUG),
  TRACE(Level.TRACE);

  private final Level level;

  ConsoleDiagnostics(Level fatal) {
    this.level = fatal;
  }
  public Level getLevel() {
    return level;
  }
}
