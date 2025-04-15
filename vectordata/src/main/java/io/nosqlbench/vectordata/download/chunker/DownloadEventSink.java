package io.nosqlbench.vectordata.download.chunker;

public interface DownloadEventSink {
    void debug(String format, Object... args);
    void info(String format, Object... args);
    void warn(String format, Object... args);
    void warn(String message, Throwable t);
    void error(String format, Object... args);
    void error(String message, Throwable t);
    void trace(String format, Object... args);
}