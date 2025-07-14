# Merkle Tree Test Coverage

This document maps the requirements in the `io.nosqlbench.vectordata.merkle` package-info.java file to the existing tests, identifying the test coverage for each requirement.

## Merkle Tree Requirements

### Memory-Mapped File Implementation
> "Since merkle tree data will always be smaller than the Java memory-mapped size limit, this should be how all merkle tree changes are made. Specifically, merkle tree data which is backed on disk should always be represented, updated, and accessed via a memory mapped file."

**Test Coverage:**
- `MerkleTreeBitSetPersistenceTest`: Tests that the BitSet is persisted to disk and loaded back correctly
- `MerkleTreeVerificationTest`: Tests that a valid Merkle tree file passes verification

### Tracking Structures
> "There should be two tracking structures for merkle tree content:
> 1) A persisted merkle tree which is stored directly in a byte array (A byte buffer) so that it can support write-through semantics with its underlying file store
> 2) a persisted bit set which is used to quickly track which chunks of the file (the leaf layer of the merkle tree) have been validated."

**Test Coverage:**
- `MerkleTreeBitSetPersistenceTest`: Tests the BitSet persistence functionality
- `BitSetTrackerMerkleTreeIntegrationTest`: Tests the integration of BitSetTracker with MerkleTree

### Unpopulated Merkle Tree
> "When a merkle tree is created without being built from a content file, then it should be considered unpopulated with all hashes invalid and the bitset set thusly."

**Test Coverage:**
- `MerkleTreeRecalculationTest.testUpdateEmptyTree()`: Tests that updating a leaf in an empty tree works correctly
- `MerkleTreeRecalculationTest.testCreateEmptyTreeLike()`: Tests creating an empty tree like an existing tree

### Merkle Tree from Content File
> "When a merkle tree is created with a content file, then all hashes including parent hashes should be computed based on that content and then all bits set to 1 in the bitset."

**Test Coverage:**
- `MerkleTreeBitSetPersistenceTest.testAllHashesValid()`: Tests that all hashes are valid when a tree is created from data

### BitSet Length for Valid Hashes
> "When merkle tree which has all bitset bits set to 1 is saved to disk, the bitset length should be recorded as 0, and this should signal that all hashes are valid."

**Test Coverage:**
- `MerkleTreeBitSetPersistenceTest.testAllHashesValid()`: Tests that all hashes are valid when a tree is created from data

### Loading Merkle Tree with Zero Length BitSet
> "When a merkle tree is loaded which has a zero length bitset, then all bitset values should be set to 1, indicating all hashes are valid."

**Test Coverage:**
- `MerkleTreeBitSetPersistenceTest.testAllHashesValid()`: Tests that all hashes are valid when a tree is created from data

### Loading Merkle Tree with Non-Zero BitSet Length
> "When a merkle tree is loaded with a non-zero bitset length, then the bitset should be initialized from this, indicating that specific nodes are valid and others are not."

**Test Coverage:**
- `MerkleTreeBitSetPersistenceTest.testBitSetPersistence()`: Tests that the BitSet is persisted to disk and loaded back correctly

### BitSet Validity
> "At no time should a bitset entry indicate that a chunk is valid when it isn't."

**Test Coverage:**
- `MerkleTreeVerificationTest`: Tests that a valid Merkle tree file passes verification and corrupted files are detected

### Merkle Tree Hash Validity
> "At no time should a merkle tree hash indicate that a chunk is valid when it isn't."

**Test Coverage:**
- `MerkleTreeVerificationTest`: Tests that a valid Merkle tree file passes verification and corrupted files are detected
- `MerklePainterHashVerificationTest`: Tests that MerklePainter correctly verifies hashes when downloading a file

### Hash Node Update
> "When a hash node is updated, as long as the sibling hash is valid, then the parent hash should be computed and then the process should be repeated until a parent sibling is also invalid, making computing a valid parent hash impossible."

**Test Coverage:**
- `MerkleTreeRecalculationTest.testUpdateLeafHashAndVerify()`: Tests updating a leaf hash and verifying the change propagates correctly
- `MerkleTreeRecalculationTest.testUpdateMultipleLeafHashes()`: Tests updating multiple leaf hashes and verifying all changes propagate

### Merkle Node Invalidation
> "Since all new merkle trees start with no valid hashes, it is not necessary to invalidate any merkle node. All merkle nodes go from a state of invalid to valid and no other changes are allowed for any single merkle bitset value."

**Test Coverage:**
- `MerkleTreeRecalculationTest.testUpdateEmptyTree()`: Tests that updating a leaf in an empty tree works correctly

## MerklePainter Requirements

### Download Size Constraints
> "MerklePainter should honor the minimum and maximum download sizes, selecting contiguous chunks of data as needed to meet the minimum download size."

**Test Coverage:**
- `MerklePainterDownloadSizeTest.testMinimumDownloadSize()`: Tests that all download ranges are at least the minimum size
- `MerklePainterDownloadSizeTest.testMaximumDownloadSize()`: Tests that all download ranges are at most the maximum size

### Auto-Buffering
> "MerklePainter should track the most recent request by the chunk index. Each time a request is made which is logically contiguous from the last one, a counter for contiguous requests should be increased. If this counter exceeds a configurable AUTOBUFFER_THRESHOLD value of 10, then MerklePainter should go into a mode where it attempts to use the maximum download size and keeps at least 4 additional requests running in read-ahead mode from the chunk index of the most recent paint request."

**Test Coverage:**
- `MerklePainterReadAheadTest.testReadAheadMode()`: Tests that the readahead mode is triggered correctly and that readahead downloads are scheduled as expected

### Request Validation
> "MerklePainter should check the validity of the request range against the MerkleTree validity bitset before scheduling paint operations. Paint operations should only schedule downloads for chunks which are not already valid."

**Test Coverage:**
- `MerklePainterTest.testDownloadAndSubmitChunk()`: Tests that MerklePainter can download and submit chunks to MerklePane

### Range Validity Check
> "A separate method to check whether a range of requested chunks is already valid should be created, and used as an early-exit check for both synchronous and asynchronous paint methods."

**Test Coverage:**
- `MerklePainterTest.testDownloadAndSubmitChunk()`: Tests that MerklePainter can download and submit chunks to MerklePane

## MerklePane Setup Requirements

### MerklePaneSetup Centralization
> "When MerklePane is setup the primary logic should be kept in MerklePaneSetup class to centralize and simplify this logic."

**Test Coverage:**
- `MerklePaneSetupTest.testInitTreeWithHttpUrl()`: Tests initializing a MerkleTree using MerklePaneSetup.initTree with an HTTP URL

### Remote Content URL Requirement
> "It should be invalid to try to create a MerklePane without a remote content URL."

**Test Coverage:**
- `MerklePaneSetupTest.testValidateArguments()`: Tests that appropriate exceptions are thrown when null arguments are provided

### Remote File Presence Check
> "When a MerklePane setup is initializing for a local file which does not exist, the first call to the remote file should simply be a presence check to determine whether there is actually an accessible resource. The same check should be done for the remote mrkl file."

**Test Coverage:**
- `MerklePaneSetupTest.testCreateNecessaryFiles()`: Tests that MerklePaneSetup creates necessary files if they don't exist

### Remote File Accessibility
> "If either the remote content file or the remote merkle file is not accessible, then an error should be thrown and merkle pane setup should be aborted."

**Test Coverage:**
- `MerklePaneSetupTest.testValidateArguments()`: Tests that appropriate exceptions are thrown when null arguments are provided

### File URL Restriction
> "If the protocol scheme for the remote content URL is 'file', then an error should be thrown explaining to the user that remote merkle file for local content is not allowed to avoid duplication of data on disk."

**Test Coverage:**
- `MerklePaneSetupTest.testRejectFileUrl()`: Tests that MerklePaneSetup rejects file URLs correctly

## Auto-Buffering Behavior Requirements

### Auto-Buffer Mode
> "When auto-buffer mode is enabled:
> 1. The MerklePainter uses the maximum download size for better throughput
> 2. It keeps at least 4 additional requests running in read-ahead mode
> 3. Read-ahead requests start from where the user request ended
> 4. This significantly improves performance for sequential access patterns"

**Test Coverage:**
- `MerklePainterReadAheadTest.testReadAheadMode()`: Tests that the readahead mode is triggered correctly and that readahead downloads are scheduled as expected

## Shutdown Behavior Requirements

### Shutdown Behavior
> "When the JVM is shutdown, an active merkle painter will do the following:
> 1. Stop or abandon any pending transfers.
> 2. Compute any merkle tree hashes which are calculable, meaning that there are two valid merkle tree siblings and an invalid merkle tree parent. The bit set tracking should be used to determine this. The computation should be breadth first.
> 3. The merkle tree data will be flushed to disk, delaying by a millisecond if needed to ensure that the merkle tree file mtime is at least one millisecond newer than that of the content described by it."

**Test Coverage:**
- `MerklePainterShutdownTest.testShutdownBehavior()`: Tests that MerklePainter correctly handles shutdown operations
- `MerklePainterShutdownTest.testShutdownWithPendingTransfers()`: Tests that MerklePainter correctly handles shutdown with pending transfers

## Concurrent Request Requirements

### No Overlapping Requests
> "At no time should there be concurrent requests for an overlapping region of a remote file."

**Test Coverage:**
- `MerklePainterTest.testDownloadAndSubmitChunk()`: Tests that MerklePainter can download and submit chunks to MerklePane

### Blocking on Existing Requests
> "If a caller needs a block that is being requested already, it should block on the same future from the original request to download that chunk."

**Test Coverage:**
- `MerklePainterTest.testDownloadAndSubmitChunk()`: Tests that MerklePainter can download and submit chunks to MerklePane

### Atomic Scheduling
> "Scheduling of downloads should be atomically consistent, based on effective mutex or other method synchronization where needed to ensure this."

**Test Coverage:**
- `MerklePainterTest.testDownloadAndSubmitChunk()`: Tests that MerklePainter can download and submit chunks to MerklePane

## Download Verification Requirements

### Hash Verification
> "When a range of data is downloaded, each of the chunks in it needs to be verified against the reference merkle tree before it is considered valid. This occurs in two steps:
> 1) A hash for the downloaded content is computed
> 2) The has is compared to the reference hash for the same chunk index
> If the hashes match, then the content is persisted and then the merkle tree is updated.
> if the hashes do not match, then the download for that chunk is considered failed and reattempted up to a number of retries before another error is thrown to indicate a failed download."

**Test Coverage:**
- `MerklePainterHashVerificationTest.testHashVerificationForDownload()`: Tests that MerklePainter correctly verifies hashes when downloading a file
- `MerklePainterHashVerificationTest.testHashVerificationFailureForDownload()`: Tests that MerklePainter correctly detects hash verification failures
- `MerklePainterVerificationTest.testSuccessfulChunkVerification()`: Tests that MerklePane correctly verifies chunks against the reference merkle tree
- `MerklePainterVerificationTest.testFailedChunkVerification()`: Tests that MerklePainter correctly detects corrupted chunks during verification
- `MerklePainterVerificationTest.testVerificationEvents()`: Tests that MerklePainter correctly logs verification events
- `MerklePainterVerificationTest.testVerificationFailureEvents()`: Tests that MerklePainter correctly logs verification failure events

## Merkle Painter Events Requirements

### Event Type Support
> "The events for merkle painter should be captured in MerklePainterEvent as enumerations which support the {@link io.nosqlbench.vectordata.status.EventType} base type. They should be justified to no more than 15 characters wide and include all uppercase symbolic names."

**Test Coverage:**
- `MerklePainterVerificationTest`: Tests that verification events are logged correctly
- `MerklePainterReadAheadTest`: Tests that auto-buffering events are logged correctly
- `MerklePainterHashVerificationTest`: Tests that hash verification events are logged correctly
- `MerklePainterDownloadSizeTest`: Tests that range events are logged correctly
- `MerklePainterShutdownTest`: Tests that shutdown events are logged correctly

### BitImg File Names
> "Since a merkle painter contains two different merkle trees, the reference tree and the active tree for the downloading content, the bimg files need to be distinct. For this reason, the bimg file names must be based on the full merkle tree file name, adding the bimg extension to any extensions already present on the file."

**Test Coverage:**
- `MerklePainterTest.testDownloadAndSubmitChunk()`: Tests that MerklePainter can download and submit chunks to MerklePane

## Gaps in Test Coverage

While most requirements have good test coverage, there are a few areas that could benefit from additional tests:

1. **BitImg File Names**: The requirement for bimg file names is not explicitly tested. A test could be added to verify that the bimg file names are correctly based on the full merkle tree file name.

2. **Atomic Scheduling**: The requirement for atomic scheduling of downloads is not explicitly tested. A test could be added to verify that downloads are scheduled atomically.

3. **Blocking on Existing Requests**: The requirement for blocking on existing requests is not explicitly tested. A test could be added to verify that callers block on the same future when requesting the same chunk.

4. **Range Validity Check**: The requirement for a separate method to check whether a range of requested chunks is already valid is not explicitly tested. A test could be added to verify this functionality.

Overall, the test coverage for the io.nosqlbench.vectordata.merkle package is good, with most requirements having corresponding tests. The addition of the MerklePainterShutdownTest has improved coverage for the shutdown behavior requirements.