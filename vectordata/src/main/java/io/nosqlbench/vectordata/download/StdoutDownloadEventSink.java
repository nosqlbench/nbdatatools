package io.nosqlbench.vectordata.download;

import io.nosqlbench.vectordata.download.chunker.DownloadEventSink;

public class StdoutDownloadEventSink implements DownloadEventSink {
    @Override
    public void debug(String format, Object... args) {
        System.out.println("DEBUG: " + String.format(format.replace("{}", "%s"), args));
    }

    @Override
    public void info(String format, Object... args) {
        System.out.println("INFO: " + String.format(format.replace("{}", "%s"), args));
    }

    @Override
    public void warn(String format, Object... args) {
        System.err.println("WARN: " + String.format(format.replace("{}", "%s"), args));
    }

    @Override
    public void warn(String message, Throwable t) {
        System.err.println("WARN: " + message);
        t.printStackTrace(System.err);
    }

    @Override
    public void error(String format, Object... args) {
        System.err.println("ERROR: " + String.format(format.replace("{}", "%s"), args));
    }

    @Override
    public void error(String message, Throwable t) {
        System.err.println("ERROR: " + message);
        t.printStackTrace(System.err);
    }

    @Override
    public void trace(String format, Object... args) {
        System.out.println("TRACE: " + String.format(format.replace("{}", "%s"), args));
    }
}