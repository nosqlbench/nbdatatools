#!/bin/bash

# Create a test directory
mkdir -p test_merkle

# Create a test file
dd if=/dev/urandom of=test_merkle/test.dat bs=1M count=1

# Create a merkle file for the test file
java -cp ../target/vectordata-${revision}.jar:../nbvectors/target/nbvectors-${revision}.jar io.nosqlbench.nbvectors.commands.merkle.CMD_merkle create test_merkle/test.dat

# Create a reference file by copying the merkle file
cp test_merkle/test.dat.mrkl test_merkle/test.dat.mref

# Test summary command with the original file
echo "Testing summary command with original file:"
java -cp ../target/vectordata-${revision}.jar:../nbvectors/target/nbvectors-${revision}.jar io.nosqlbench.nbvectors.commands.merkle.CMD_merkle summary test_merkle/test.dat

# Test summary command with the merkle file directly
echo "Testing summary command with merkle file:"
java -cp ../target/vectordata-${revision}.jar:../nbvectors/target/nbvectors-${revision}.jar io.nosqlbench.nbvectors.commands.merkle.CMD_merkle summary test_merkle/test.dat.mrkl

# Test summary command with the reference file directly
echo "Testing summary command with reference file:"
java -cp ../target/vectordata-${revision}.jar:../nbvectors/target/nbvectors-${revision}.jar io.nosqlbench.nbvectors.commands.merkle.CMD_merkle summary test_merkle/test.dat.mref

# Clean up
rm -rf test_merkle
