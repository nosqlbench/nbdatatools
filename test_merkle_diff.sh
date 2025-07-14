#!/bin/bash

# Test script for the merkle diff command
# This script demonstrates how to use the merkle diff command to compare two merkle tree files

set -e  # Exit on error

# Create a temporary directory for test files
TEMP_DIR=$(mktemp -d)
echo "Using temporary directory: $TEMP_DIR"

# Create two test files with different content
echo "Creating test files..."
FILE1="$TEMP_DIR/file1.dat"
FILE2="$TEMP_DIR/file2.dat"

# Create a 1MB file with pattern data
dd if=/dev/urandom of="$FILE1" bs=1024 count=1024

# Create a copy of the file
cp "$FILE1" "$FILE2"

# Modify a portion of the second file to create differences
# This modifies bytes in the middle of the file
dd if=/dev/urandom of="$FILE2" bs=1024 count=256 seek=384 conv=notrunc

echo "Created test files:"
echo "  $FILE1 (original)"
echo "  $FILE2 (modified)"

# Create merkle tree files for both files
echo -e "\nCreating merkle tree files..."
java -jar target/nbdatatools-*-SNAPSHOT.jar merkle create "$FILE1" --chunk-size=16384
java -jar target/nbdatatools-*-SNAPSHOT.jar merkle create "$FILE2" --chunk-size=16384

echo "Created merkle tree files:"
echo "  $FILE1.mrkl"
echo "  $FILE2.mrkl"

# Display summary of the first merkle tree
echo -e "\nSummary of first merkle tree:"
java -jar target/nbdatatools-*-SNAPSHOT.jar merkle summary "$FILE1.mrkl"

# Display summary of the second merkle tree
echo -e "\nSummary of second merkle tree:"
java -jar target/nbdatatools-*-SNAPSHOT.jar merkle summary "$FILE2.mrkl"

# Compare the two merkle trees
echo -e "\nComparing the two merkle trees:"
java -jar target/nbdatatools-*-SNAPSHOT.jar merkle diff "$FILE1.mrkl" "$FILE2.mrkl"

# Create a third file with identical content to the first
echo -e "\nCreating a third file identical to the first..."
FILE3="$TEMP_DIR/file3.dat"
cp "$FILE1" "$FILE3"
java -jar target/nbdatatools-*-SNAPSHOT.jar merkle create "$FILE3" --chunk-size=16384

# Compare identical merkle trees
echo -e "\nComparing identical merkle trees:"
java -jar target/nbdatatools-*-SNAPSHOT.jar merkle diff "$FILE1.mrkl" "$FILE3.mrkl"

# Clean up
echo -e "\nCleaning up..."
rm -rf "$TEMP_DIR"
echo "Done."