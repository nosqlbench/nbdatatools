package io.nosqlbench.nbvectors.commands.merkle;

import io.nosqlbench.vectordata.download.merkle.MerkleFileUtils;
import io.nosqlbench.vectordata.download.merkle.MerkleTreeFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
    name = "merkle",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "create or verify Merkle tree files for data integrity",
    description = """
        Creates or verifies Merkle tree files for specified files. These Merkle tree files
        can be used later to efficiently verify file integrity or identify changed portions
        of files for partial downloads/updates.
        
        The Merkle tree file is created with the same name as the source file plus a .merkle extension.
        
        Examples:
        
        # Create Merkle files for multiple files
        nbvectors merkle file1.hdf5 file2.hdf5
        
        # Create with custom section sizes
        nbvectors merkle --min-section 2097152 --max-section 33554432 bigfile.hdf5
        
        # Verify files against their Merkle trees
        nbvectors merkle -v file1.hdf5 file2.hdf5
        
        # Force overwrite of existing Merkle files
        nbvectors merkle -f file1.hdf5
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: Success",
        "1: Error creating Merkle tree file",
        "2: Error verifying Merkle tree file"
    }
)
public class CMD_merkle implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle.class);

    @Parameters(
        description = "Files to process",
        arity = "1..*"
    )
    private List<Path> files = new ArrayList<>();

    @Option(
        names = {"-v", "--verify"},
        description = "Verify existing Merkle files instead of creating new ones"
    )
    private boolean verify = false;

    @Option(
        names = {"--min-section"},
        description = "Minimum section size in bytes (default: ${DEFAULT-VALUE})",
        defaultValue = "1048576"  // 1MB
    )
    private long minSection;

    @Option(
        names = {"--max-section"},
        description = "Maximum section size in bytes (default: ${DEFAULT-VALUE})",
        defaultValue = "16777216"  // 16MB
    )
    private long maxSection;

    @Option(
        names = {"-f", "--force"},
        description = "Overwrite existing Merkle files"
    )
    private boolean force = false;

    @Override
    public Integer call() {
        boolean hasErrors = false;

        for (Path file : files) {
            try {
                if (!Files.exists(file)) {
                    logger.error("File not found: {}", file);
                    hasErrors = true;
                    continue;
                }

                Path merklePath = MerkleFileUtils.getMerkleFilePath(file);

                if (verify) {
                    if (!Files.exists(merklePath)) {
                        logger.error("Merkle file not found for: {}", file);
                        hasErrors = true;
                        continue;
                    }
                    verifyFile(file, merklePath);
                } else {
                    if (Files.exists(merklePath) && !force) {
                        logger.error("Merkle file already exists for: {} (use --force to overwrite)", file);
                        hasErrors = true;
                        continue;
                    }
                    createMerkleFile(file);
                }
            } catch (Exception e) {
                logger.error("Error processing file: {}", file, e);
                hasErrors = true;
            }
        }

        return hasErrors ? 1 : 0;
    }

    private void createMerkleFile(Path file) throws Exception {
        logger.info("Creating Merkle tree file for: {}", file);
        MerkleTreeFile merkleTree = MerkleTreeFile.create(file, minSection, maxSection);
        Path merklePath = MerkleFileUtils.getMerkleFilePath(file);
        merkleTree.save(merklePath);
        logger.info("Created Merkle tree file: {}", merklePath);
    }

    private void verifyFile(Path file, Path merklePath) throws Exception {
        logger.info("Verifying file against Merkle tree: {}", file);
        MerkleTreeFile originalTree = MerkleTreeFile.load(merklePath);
        MerkleTreeFile currentTree = MerkleTreeFile.create(file, minSection, maxSection);
        
        // Compare the trees
        if (originalTree.equals(currentTree)) {
            logger.info("Verification successful: {} matches its Merkle tree", file);
        } else {
            logger.error("Verification failed: {} does not match its Merkle tree", file);
            throw new RuntimeException("File verification failed");
        }
    }
}