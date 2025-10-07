/// The test web server in this directory should provide basic web services
/// to a local base resource path. URLs which resolve to files should be handled normally, with
/// all the standard GET, HEAD, and other read http actions.
/// URLs which resolve to a directory should yield a 404 error with an explanation.
/// Any errors during servicing a request should be trapped with an appropriate http status code
/// and a matching message in the response body. Otherwise, no special behavior is expected by
/// the test web server.
///
package io.nosqlbench.vectordata.downloader.testserver;

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

