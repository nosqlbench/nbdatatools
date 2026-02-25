#!/bin/bash
# scripts/generate_benchmark_cache.sh
# Standardized dataset generation via Maven

echo "Orchestrating benchmark data generation (1M -> 1B records)..."
mvn -pl datatools-predicated exec:java@predicate_test_data
