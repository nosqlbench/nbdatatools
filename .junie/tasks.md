# backfill javadoc

When I ask you to backfill javadoc, I want you to use the output of 
`mvn javadoc:javadoc -DadditionalJOption=-Xdoclint:all` to determine what errors and warnings 
remain, and then add markdown-style javadoc using three slashes at the start of each javadoc line,
so that all public members are properly documented, with args, exceptions, return types, and 
some diagrammatic illustrations where appropriate. You can also create no-arg constructors for 
the sake of documenting default instances.

# isolate performance bottlenecks

When I ask you to isolate performance bottlenecks, I intend for you to do the following:
1. If there is pending git state, or uncommitted files, stop and warn me instead of completing 
   the task.
2. If there are no uncommitted files, then proceed.
3. For the class or package in focus, ensure that there are enough event markers included using 
   the local event sink capability for key phases of processing to be measured in a performance 
   test.
4. Create a performance test as a unit test, but mark it specifically as part of a performance 
   test suite.
5. Iterate on the test, isolating the most expensive calls, and subdividing the implementation 
   by breaking out methods where needed for finer grain views. Before running any performance 
   unit tests, be sure to run all other unit tests for that code first to ensure you are testing 
   a valid configuration.
6. If necessary, instrument a unit test with jfr and do some testing with that. Further 
   subdivide methods as needed to find the hottest code paths for optimization.
7. 
