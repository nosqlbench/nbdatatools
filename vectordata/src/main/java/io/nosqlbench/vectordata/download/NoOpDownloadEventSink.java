package io.nosqlbench.vectordata.download;

import io.nosqlbench.vectordata.download.chunker.DownloadEventSink;

public class NoOpDownloadEventSink implements DownloadEventSink {
    @Override
    public void debug(String format, Object... args) {}

    @Override
    public void info(String format, Object... args) {}

    @Override
    public void warn(String format, Object... args) {}

    @Override
    public void warn(String message, Throwable t) {}

    @Override
    public void error(String format, Object... args) {}

    @Override
    public void error(String message, Throwable t) {}

    @Override
    public void trace(String format, Object... args) {}
}