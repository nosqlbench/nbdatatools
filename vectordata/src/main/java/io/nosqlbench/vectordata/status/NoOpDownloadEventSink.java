package io.nosqlbench.vectordata.status;

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


/// A no-operation implementation of DownloadEventSink.
///
/// This class implements the DownloadEventSink interface but does nothing with the events.
/// It's useful when you don't need to log or process download events.
public class NoOpDownloadEventSink implements EventSink {

    /// Construct a NoOpDownloadEventSink.
    ///
    /// Creates a new instance that ignores all events.
    public NoOpDownloadEventSink() {}

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
