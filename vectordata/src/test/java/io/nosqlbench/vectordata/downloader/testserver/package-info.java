/// The test web server in this directory should provide basic web services
/// to a local base resource path. URLs which resolve to files should be handled normally, with
/// all the standard GET, HEAD, and other read http actions.
/// URLs which resolve to a directory should yield a 404 error with an explanation.
/// Any errors during servicing a request should be trapped with an appropriate http status code
/// and a matching message in the response body. Otherwise, no special behavior is expected by
/// the test web server.
///
package io.nosqlbench.vectordata.downloader.testserver;