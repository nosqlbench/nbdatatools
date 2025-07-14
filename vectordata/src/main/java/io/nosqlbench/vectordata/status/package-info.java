/// # REQUIREMENTS
///
/// EventSink and EventType define a basic interface for local logging implementations, modeled
/// after on the API of log4j and similar. However, it avoids the requirement to pull it in and
/// deal with the myriad of issues that often go with dependency version reconciliation for
/// logging libraries. If so needed, a sink can be implemented to forward to such files.
///
/// ## EventType
/// * Provides a base type which can be logged with more strict formatting and structure than
/// basic log lines.
/// * Implementors of EventType can be logged by the event sink natively.
/// * The EventType class provides support for specifying field groupings in implementing class
/// members, such as param(...) and tuple(...) which can include params.
///
/// ## LogFileEventSink
///
/// * This sink logs data to a log file.
/// * The time is the time since the file was created, which should be logged on the first line
/// in ISO 8601 format with a special event type.
/// * When resuming logging, this timestamp should be parsed as the base time for subsequent
/// logged events.
/// * It uses a timestamp format of HHMMSS.millis, representing hours minutes and seconds on 24-hour
/// clock time. For example "340321.234238".
/// * When the timestamp is for less than one day, the above format is used, being right
/// justified and filling the left side of the timestamp with spaces to maintain alignment.
/// * When the timestamp is for more than a day, then the format is changed to the ISO 8601 format.
/// * After the timestamp a single character indicating the severity is used. T for trace, D for
/// debug, I for info, W for warn, and E for error.
/// * After the severity, the log event name is given, and it should be left justified to ensure
///  it always takes the maximum size of the enum for the type in question.
/// * After the log event name is given, the named parameters for it are provided in the order
/// they were declared for the event enum member.
/// * parameter names should be single-word lower-case and be symbolic without being long. For
/// example, 'chunkIndex' should be 'index', and 'endChunk' should be 'end'.
/// * The format of named parameters as they are logged should be simply the parameter name, a
/// colon ':' and then the value.
/// * When parameters constitute some type of pair, such as start, end, then the format should be
///  changed to be a structured name and then value offset. For example, start and end chunk
/// could be represented as `chunk(start,end)=(23432,23435)`
/// * When tuples are provided in events, they are handled exactly like the structure tuple
/// examples above.
/// * This event sink should maintain an active re-order buffer to sort incoming messages. It
/// should flush all events that are older than 500 milliseconds, or any events at the tail (
/// oldest) end of the buffer which would exceed the default buffer size of 100 events. Effort
/// should be made to write the events in timestamp order if possible using this buffer.
/// * A shutdown event handler should be registered to flush all remaining events to the file
/// so they aren't lost.
///
/// ## Memory Event Sink
/// This is a separate sink which does what the logfile event sink does, except that it
/// ultimately records its results into a memory buffer which is simply a list of events.
/// * It can have a configured limit of events, but this defaults to 10000
/// * The events are reordered according to timestamp lazily, as in the first time they are
/// accessed.
/// * An atomically safe consume and purge method should be provided which allows for concurrent
/// reading of the events without losing any.
/// * If the event limit is reached, then extra events are droped into a System.err output in the
///  same format as you would see for the log file sink.
///
///
///
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

