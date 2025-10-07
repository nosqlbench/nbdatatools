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

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/// Comprehensive results from a scheduler performance test.
/// 
/// This class aggregates results from all test combinations and provides
/// analysis methods to compare scheduler performance across different
/// scenarios. It includes statistical analysis and ranking capabilities
/// to help identify the best scheduler for specific conditions.
public class PerformanceResults {
    
    private final Map<SchedulerPerformanceTest.TestCombination, List<SingleTestResult>> allResults;
    
    /// Creates performance results from test data.
    /// 
    /// @param allResults Map of test combinations to their results
    public PerformanceResults(Map<SchedulerPerformanceTest.TestCombination, List<SingleTestResult>> allResults) {
        this.allResults = new HashMap<>(allResults);
    }
    
    /// Gets all test combinations that were executed.
    /// 
    /// @return Set of test combinations
    public Set<SchedulerPerformanceTest.TestCombination> getTestCombinations() {
        return allResults.keySet();
    }
    
    /// Gets results for a specific test combination.
    /// 
    /// @param combination The test combination
    /// @return List of results from all iterations
    public List<SingleTestResult> getResults(SchedulerPerformanceTest.TestCombination combination) {
        return allResults.getOrDefault(combination, Collections.emptyList());
    }
    
    /// Gets all scheduler names that were tested.
    /// 
    /// @return Set of scheduler names
    public Set<String> getSchedulerNames() {
        return allResults.keySet().stream()
            .map(SchedulerPerformanceTest.TestCombination::getSchedulerName)
            .collect(Collectors.toSet());
    }
    
    /// Gets all network conditions that were tested.
    /// 
    /// @return Set of network condition descriptions
    public Set<String> getNetworkConditions() {
        return allResults.keySet().stream()
            .map(SchedulerPerformanceTest.TestCombination::getNetworkCondition)
            .collect(Collectors.toSet());
    }
    
    /// Gets all workloads that were tested.
    /// 
    /// @return Set of workload names
    public Set<String> getWorkloadNames() {
        return allResults.keySet().stream()
            .map(SchedulerPerformanceTest.TestCombination::getWorkloadName)
            .collect(Collectors.toSet());
    }
    
    /// Gets average performance metrics for a test combination.
    /// 
    /// @param combination The test combination
    /// @return Average metrics across all iterations
    public AverageMetrics getAverageMetrics(SchedulerPerformanceTest.TestCombination combination) {
        List<SingleTestResult> results = getResults(combination);
        if (results.isEmpty()) {
            return new AverageMetrics(Duration.ZERO, 0.0, 0.0, 0.0, 0.0);
        }
        
        double avgTime = results.stream()
            .mapToLong(r -> r.getTotalTime().toMillis())
            .average()
            .orElse(0.0);
        
        double avgCompletion = results.stream()
            .mapToDouble(SingleTestResult::getCompletionPercentage)
            .average()
            .orElse(0.0);
        
        double avgEfficiency = results.stream()
            .mapToDouble(SingleTestResult::getDownloadEfficiency)
            .average()
            .orElse(0.0);
        
        double avgThroughput = results.stream()
            .mapToDouble(SingleTestResult::getOverallThroughput)
            .average()
            .orElse(0.0);
        
        double successRate = results.stream()
            .mapToDouble(r -> r.isGoodPerformance() ? 1.0 : 0.0)
            .average()
            .orElse(0.0) * 100.0;
        
        return new AverageMetrics(
            Duration.ofMillis((long) avgTime),
            avgCompletion,
            avgEfficiency,
            avgThroughput,
            successRate
        );
    }
    
    /// Ranks schedulers by overall performance across all test scenarios.
    /// 
    /// @return List of schedulers ordered by performance (best first)
    public List<SchedulerRanking> rankSchedulers() {
        Map<String, List<Double>> schedulerScores = new HashMap<>();
        
        // Collect scores for each scheduler across all combinations
        for (SchedulerPerformanceTest.TestCombination combination : allResults.keySet()) {
            String scheduler = combination.getSchedulerName();
            AverageMetrics metrics = getAverageMetrics(combination);
            
            // Calculate composite score (higher is better)
            double score = calculateCompositeScore(metrics);
            
            schedulerScores.computeIfAbsent(scheduler, k -> new ArrayList<>()).add(score);
        }
        
        // Calculate average scores and rank
        List<SchedulerRanking> rankings = new ArrayList<>();
        for (Map.Entry<String, List<Double>> entry : schedulerScores.entrySet()) {
            String scheduler = entry.getKey();
            List<Double> scores = entry.getValue();
            
            double averageScore = scores.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            double minScore = scores.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
            double maxScore = scores.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
            
            rankings.add(new SchedulerRanking(scheduler, averageScore, minScore, maxScore, scores.size()));
        }
        
        // Sort by average score (descending)
        rankings.sort((a, b) -> Double.compare(b.getAverageScore(), a.getAverageScore()));
        
        return rankings;
    }
    
    /// Finds the best scheduler for a specific network condition.
    /// 
    /// @param networkCondition The network condition to analyze
    /// @return The scheduler that performed best under this condition
    public String getBestSchedulerForCondition(String networkCondition) {
        Map<String, Double> schedulerScores = new HashMap<>();
        
        for (SchedulerPerformanceTest.TestCombination combination : allResults.keySet()) {
            if (combination.getNetworkCondition().equals(networkCondition)) {
                String scheduler = combination.getSchedulerName();
                AverageMetrics metrics = getAverageMetrics(combination);
                double score = calculateCompositeScore(metrics);
                
                schedulerScores.merge(scheduler, score, Double::sum);
            }
        }
        
        return schedulerScores.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("Unknown");
    }
    
    /// Finds the best scheduler for a specific workload pattern.
    /// 
    /// @param workloadName The workload pattern to analyze
    /// @return The scheduler that performed best with this workload
    public String getBestSchedulerForWorkload(String workloadName) {
        Map<String, Double> schedulerScores = new HashMap<>();
        
        for (SchedulerPerformanceTest.TestCombination combination : allResults.keySet()) {
            if (combination.getWorkloadName().equals(workloadName)) {
                String scheduler = combination.getSchedulerName();
                AverageMetrics metrics = getAverageMetrics(combination);
                double score = calculateCompositeScore(metrics);
                
                schedulerScores.merge(scheduler, score, Double::sum);
            }
        }
        
        return schedulerScores.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("Unknown");
    }
    
    /// Calculates a composite performance score from metrics.
    /// 
    /// Higher scores indicate better performance. The score combines
    /// completion rate, efficiency, and throughput with appropriate weighting.
    /// 
    /// @param metrics The performance metrics
    /// @return Composite score (0-100, higher is better)
    private double calculateCompositeScore(AverageMetrics metrics) {
        // Weights for different aspects of performance
        double completionWeight = 0.4; // 40% - most important
        double efficiencyWeight = 0.3; // 30% - avoid over-downloading
        double throughputWeight = 0.2;  // 20% - speed matters
        double successWeight = 0.1;    // 10% - reliability
        
        // Normalize metrics to 0-100 scale
        double completionScore = Math.min(100.0, metrics.getAverageCompletionPercentage());
        
        // Efficiency: lower is better, so invert it (1.0 = perfect, higher = worse)
        double efficiencyScore = Math.max(0.0, 100.0 - (metrics.getAverageEfficiency() - 1.0) * 50.0);
        
        // Throughput: normalize to reasonable range (assume 100 MB/s is excellent)
        double throughputScore = Math.min(100.0, (metrics.getAverageThroughput() / (100 * 1024 * 1024)) * 100.0);
        
        double successScore = metrics.getSuccessRate();
        
        return (completionScore * completionWeight) +
               (efficiencyScore * efficiencyWeight) +
               (throughputScore * throughputWeight) +
               (successScore * successWeight);
    }
    
    /// Prints a comprehensive summary of all test results.
    public void printSummary() {
        System.out.println("=== Scheduler Performance Test Results ===");
        System.out.println();
        
        // Overall rankings
        System.out.println("Overall Scheduler Rankings:");
        List<SchedulerRanking> rankings = rankSchedulers();
        for (int i = 0; i < rankings.size(); i++) {
            SchedulerRanking ranking = rankings.get(i);
            System.out.printf("%d. %s - Score: %.2f (%.2f - %.2f) across %d scenarios%n",
                            i + 1, ranking.getSchedulerName(), ranking.getAverageScore(),
                            ranking.getMinScore(), ranking.getMaxScore(), ranking.getScenarioCount());
        }
        System.out.println();
        
        // Best by network condition
        System.out.println("Best Scheduler by Network Condition:");
        for (String condition : getNetworkConditions()) {
            String best = getBestSchedulerForCondition(condition);
            System.out.printf("  %s: %s%n", condition, best);
        }
        System.out.println();
        
        // Best by workload
        System.out.println("Best Scheduler by Workload Pattern:");
        for (String workload : getWorkloadNames()) {
            String best = getBestSchedulerForWorkload(workload);
            System.out.printf("  %s: %s%n", workload, best);
        }
        System.out.println();
        
        // Detailed results
        System.out.println("Detailed Results by Combination:");
        for (SchedulerPerformanceTest.TestCombination combination : allResults.keySet()) {
            AverageMetrics metrics = getAverageMetrics(combination);
            System.out.printf("  %s:%n", combination);
            System.out.printf("    Average Time: %d ms%n", metrics.getAverageTime().toMillis());
            System.out.printf("    Completion: %.1f%%\t\tEfficiency: %.2f%n", 
                            metrics.getAverageCompletionPercentage(), metrics.getAverageEfficiency());
            System.out.printf("    Throughput: %.2f MB/s\tSuccess Rate: %.1f%%\n",
                            metrics.getAverageThroughput() / (1024 * 1024), metrics.getSuccessRate());
            System.out.printf("    Composite Score: %.2f%n", calculateCompositeScore(metrics));
            System.out.println();
        }
    }
    
    /// Average performance metrics for a test combination.
    public static class AverageMetrics {
        private final Duration averageTime;
        private final double averageCompletionPercentage;
        private final double averageEfficiency;
        private final double averageThroughput;
        private final double successRate;
        
        public AverageMetrics(Duration averageTime, double averageCompletionPercentage,
                             double averageEfficiency, double averageThroughput, double successRate) {
            this.averageTime = averageTime;
            this.averageCompletionPercentage = averageCompletionPercentage;
            this.averageEfficiency = averageEfficiency;
            this.averageThroughput = averageThroughput;
            this.successRate = successRate;
        }
        
        public Duration getAverageTime() { return averageTime; }
        public double getAverageCompletionPercentage() { return averageCompletionPercentage; }
        public double getAverageEfficiency() { return averageEfficiency; }
        public double getAverageThroughput() { return averageThroughput; }
        public double getSuccessRate() { return successRate; }
    }
    
    /// Ranking information for a scheduler.
    public static class SchedulerRanking {
        private final String schedulerName;
        private final double averageScore;
        private final double minScore;
        private final double maxScore;
        private final int scenarioCount;
        
        public SchedulerRanking(String schedulerName, double averageScore, double minScore,
                               double maxScore, int scenarioCount) {
            this.schedulerName = schedulerName;
            this.averageScore = averageScore;
            this.minScore = minScore;
            this.maxScore = maxScore;
            this.scenarioCount = scenarioCount;
        }
        
        public String getSchedulerName() { return schedulerName; }
        public double getAverageScore() { return averageScore; }
        public double getMinScore() { return minScore; }
        public double getMaxScore() { return maxScore; }
        public int getScenarioCount() { return scenarioCount; }
    }
}