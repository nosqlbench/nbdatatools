package io.nosqlbench.vectordata.simulation.mockdriven;

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

import org.junit.jupiter.api.Test;

/// Example test showing what the tabular output from ComprehensiveSchedulerBenchmark looks like.
/// 
/// This provides a preview of the table formats without running the full benchmark.
public class TabularOutputExample {
    
    @Test
    public void showExampleTabularOutput() {
        System.out.println("=== EXAMPLE OF COMPREHENSIVE SCHEDULER BENCHMARK OUTPUT ===");
        System.out.println("\nThe ComprehensiveSchedulerBenchmark produces the following tables:");
        System.out.println();
        
        // Example of detailed results table
        System.out.println("1. DETAILED RESULTS TABLE");
        System.out.println("-".repeat(120));
        System.out.printf("%-20s | %-20s | %-15s | %8s | %10s | %10s | %12s | %10s%n",
            "Scheduler", "Network", "Workload", "Time(ms)", "Complete%", "Efficiency", "Throughput", "Score");
        System.out.println("-".repeat(120));
        
        // Sample data rows
        String[][] sampleData = {
            {"DefaultChunkScheduler", "Fiber Gigabit", "Sequential Read", "245", "100.0", "1.05", "81.63", "95.32"},
            {"DefaultChunkScheduler", "Fiber Gigabit", "Random Read", "312", "98.5", "1.12", "64.10", "88.45"},
            {"DefaultChunkScheduler", "Fiber Gigabit", "Sparse Read", "189", "100.0", "1.02", "105.82", "98.12"},
            {"DefaultChunkScheduler", "Fiber Gigabit", "Clustered Read", "267", "99.0", "1.08", "74.91", "91.28"},
            {"-".repeat(120), "", "", "", "", "", "", ""},
            {"AggressiveScheduler", "Fiber Gigabit", "Sequential Read", "198", "100.0", "1.25", "101.01", "89.76"},
            {"AggressiveScheduler", "Fiber Gigabit", "Random Read", "421", "96.8", "1.45", "47.51", "75.32"},
            {"AggressiveScheduler", "Fiber Gigabit", "Sparse Read", "156", "100.0", "1.18", "128.21", "91.85"},
            {"AggressiveScheduler", "Fiber Gigabit", "Clustered Read", "234", "99.5", "1.32", "85.47", "85.64"}
        };
        
        for (String[] row : sampleData) {
            if (row[0].startsWith("-")) {
                System.out.println(row[0]);
            } else {
                System.out.printf("%-20s | %-20s | %-15s | %8s | %9s%% | %10s | %10s MB/s | %10s%n",
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]);
            }
        }
        
        // Example of network condition summary
        System.out.println("\n\n2. BEST SCHEDULER BY NETWORK CONDITION");
        System.out.println("-".repeat(80));
        System.out.printf("%-30s | %-20s | %15s | %10s%n",
            "Network Condition", "Best Scheduler", "Avg Score", "Workloads");
        System.out.println("-".repeat(80));
        
        String[][] networkSummary = {
            {"Fiber Gigabit", "DefaultChunkScheduler", "93.29", "4"},
            {"Fast Broadband", "DefaultChunkScheduler", "87.45", "4"},
            {"Standard Broadband", "ConservativeScheduler", "82.13", "4"},
            {"Mobile LTE", "ConservativeScheduler", "78.92", "4"},
            {"Satellite", "ConservativeScheduler", "71.35", "4"}
        };
        
        for (String[] row : networkSummary) {
            System.out.printf("%-30s | %-20s | %15s | %10s%n", row[0], row[1], row[2], row[3]);
        }
        
        // Example of workload pattern summary
        System.out.println("\n\n3. BEST SCHEDULER BY WORKLOAD PATTERN");
        System.out.println("-".repeat(80));
        System.out.printf("%-20s | %-20s | %15s | %12s%n",
            "Workload Pattern", "Best Scheduler", "Avg Score", "Networks");
        System.out.println("-".repeat(80));
        
        String[][] workloadSummary = {
            {"Sequential Read", "AggressiveScheduler", "88.65", "5"},
            {"Random Read", "ConservativeScheduler", "79.23", "5"},
            {"Sparse Read", "ConservativeScheduler", "85.47", "5"},
            {"Clustered Read", "AdaptiveScheduler", "83.91", "5"}
        };
        
        for (String[] row : workloadSummary) {
            System.out.printf("%-20s | %-20s | %15s | %12s%n", row[0], row[1], row[2], row[3]);
        }
        
        // Example of overall rankings
        System.out.println("\n\n4. OVERALL SCHEDULER RANKINGS");
        System.out.println("-".repeat(100));
        System.out.printf("%4s | %-20s | %12s | %12s | %12s | %10s | %15s%n",
            "Rank", "Scheduler", "Avg Score", "Min Score", "Max Score", "Scenarios", "Recommendation");
        System.out.println("-".repeat(100));
        
        String[][] rankings = {
            {"1", "AdaptiveScheduler", "84.32", "68.45", "98.12", "20", "Best Overall"},
            {"2", "DefaultChunkScheduler", "82.76", "65.23", "95.32", "20", "Excellent"},
            {"3", "ConservativeScheduler", "79.14", "71.35", "89.76", "20", "Good"},
            {"4", "AggressiveScheduler", "76.89", "52.18", "91.85", "20", "Good"}
        };
        
        for (String[] row : rankings) {
            System.out.printf("%4s | %-20s | %12s | %12s | %12s | %10s | %15s%n",
                row[0], row[1], row[2], row[3], row[4], row[5], row[6]);
        }
        
        // Example of performance matrix
        System.out.println("\n\n5. PERFORMANCE MATRIX (Average Scores by Scheduler and Network)");
        System.out.println("-".repeat(120));
        System.out.printf("%-20s |", "Scheduler \\ Network");
        String[] networks = {"Fiber Giga...", "Fast Broad...", "Standard B...", "Mobile LTE", "Satellite"};
        for (String network : networks) {
            System.out.printf(" %12s |", network);
        }
        System.out.println();
        System.out.println("-".repeat(120));
        
        String[][] matrix = {
            {"DefaultChunkScheduler", "93.29", "87.45", "82.13", "76.89", "71.35"},
            {"AggressiveScheduler", "85.64", "82.31", "74.56", "68.92", "52.18"},
            {"ConservativeScheduler", "78.45", "81.23", "84.67", "86.12", "85.74"},
            {"AdaptiveScheduler", "91.85", "88.76", "85.34", "82.45", "73.21"}
        };
        
        for (String[] row : matrix) {
            System.out.printf("%-20s |", row[0]);
            for (int i = 1; i < row.length; i++) {
                System.out.printf(" %12s |", row[i]);
            }
            System.out.println();
        }
        
        System.out.println("-".repeat(120));
        System.out.println("Note: Higher scores indicate better performance (0-100 scale)");
        
        System.out.println("\n\nKEY INSIGHTS FROM THE TABLES:");
        System.out.println("- The Detailed Results Table shows performance metrics for every combination");
        System.out.println("- Network Condition Summary identifies which scheduler works best for each network type");
        System.out.println("- Workload Pattern Summary shows optimal schedulers for different access patterns");
        System.out.println("- Overall Rankings provide a global comparison across all scenarios");
        System.out.println("- Performance Matrix gives a quick visual comparison of scheduler performance");
        
        System.out.println("\n\nRun ComprehensiveSchedulerBenchmark#runComprehensiveBenchmark() for real results!");
    }
}