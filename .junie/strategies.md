* Event-Based tracing
* Isolation of Requirements
* Inclusion of Learnings
* Diagrammatic Documentation
* Setting Off-Limits Boundaries for certain content
* Clarify starting states, preconditions
* Elaborate full path, only add optimizing skips later
* Focus on idempotent operations
* There are certain APIs which should be favored as much as possible, these should be called out
  * The VectorFileIO entry point, for example
* Establish strong scaffolding patterns in the code base and then ask the agent to implement 
  similar ones
* For documentation, establish an orderly layout of sections:
  * package and their requirements and responsibilities
  * components within each package and their relationship to each other
  * Diagrammatic view of:
    * The relationship between individual components within a package
    * Data structures, algorithms, data types, or operations
* Testing Layers - incrementally improve testing by tightening the bolts
  * Invariants
  * Concurrency
  * Coverage
  * Preconditions
* Clarify the nature of each component or code section or class
  * Throughput sensitive
  * Processing Time sensitive
  * optimize for what type of resource
  * parallelize over what type of resource
* Asynchrony
  * Use future subtypes which allow for progress tracking
* Use Design Language
  * Algebraic Types
  * Value Types
  * Immutability
  * Invariants
  * Preconditions
  * Assertions

Design Blueprints
* Parallel Tasker
  * concurrent blocking queue for managing effective concurrency
  * mode 1: priming futures
  * mode 2: awaiting futures and backfilling
  * mode 3: awaiting futures
* Event Reactor
* Chronotron

* Concurrency
  * Use the latest Java.NIO library features that support the largest files and random access.
  * For multi-threaded code, use absolute offsets and not relative ones so that concurrent code 
    can share types like file channels

* If you find out-of-scope or invasive change that were made as part of another change set, 
  consider removing or modifying the commit they were introduced in rather than working to 
  nullify them after.

* Create a series of your own command templates

* For potentially ephemeral changes, _ensure_ that git has no uncommitted files automatically

* Implement a tier tagging system to lock down fundamental types and build on them like axioms, etc.

* Identify "core types" ....

* For more complex issues, make the agent work harder for a more thorough analysis first, and 
  then use that as the starting point for a new context.
* Do some defensive codebase conditioning, such as directly reducing complexity, eliminating 
  duplicitous code, defining new central algebraic or value types, etc.

* Phrase "fix" tasks with a strong bias towards fixing bugs detected by the test, or fixing tests to match implementation, or studying in depth to determine which is likely teh culprit first.