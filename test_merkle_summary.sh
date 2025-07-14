#!/bin/bash

# Create a test directory
mkdir -p test_merkle

# Create a test file
dd if=/dev/urandom of=test_merkle/test.dat bs=1M count=1

# Change to the commands directory
cd commands

# Create a merkle file for the test file using our CreateMerkleFileForTest program
mvn exec:java -Dexec.mainClass="io.nosqlbench.command.merkle.subcommands.CreateMerkleFileForTest" -Dexec.args="../test_merkle/test.dat" -Drat.skip=true

# Test summary command with the original file
echo "Testing summary command with original file:"
mvn exec:java -Dexec.mainClass="io.nosqlbench.command.merkle.subcommands.SummaryCommand" -Dexec.args="../test_merkle/test.dat" -Drat.skip=true

# Test summary command with the merkle file directly
echo "Testing summary command with merkle file:"
mvn exec:java -Dexec.mainClass="io.nosqlbench.command.merkle.subcommands.SummaryCommand" -Dexec.args="../test_merkle/test.dat.mrkl" -Drat.skip=true

# Test summary command with the reference file directly
echo "Testing summary command with reference file:"
mvn exec:java -Dexec.mainClass="io.nosqlbench.command.merkle.subcommands.SummaryCommand" -Dexec.args="../test_merkle/test.dat.mref" -Drat.skip=true

# Change back to the root directory
cd ..

# Clean up
rm -rf test_merkle
