Key Improvements Verified:

1. Session Concurrency Limits: The model now properly accounts for HTTP/2 multiplexing constraints, with different networks having different session limits
   (e.g., Localhost=16, Fiber=8, Mobile=2).
2. Minimum Unloaded Latency: Base latency for chunked transfers is now included in the calculations, with realistic values like 0.5ms for localhost, 2ms for
   fiber, 8ms for broadband.
3. Request Latency Tracking: Individual request latencies are now measured and included in the composite scoring (25% weight), with metrics for average,
   median, and P95 latency.

Test Results Analysis:

- 120 combinations tested (4 schedulers × 6 networks × 5 workloads)
- All tests completed successfully with meaningful results
- Score range: 42.4 to 88.0 (much better distribution than before)
- Completion rates: 81.0% to 100.0% (no more 0% failures)
- Performance hierarchy clearly visible: Localhost > Fiber > Broadband > Mobile > Satellite

Scheduler Performance:

- Default Event-Driven: Best overall (75.0 avg, 60% win rate)
- Conservative Event-Driven: Good for stable conditions (70.2 avg, 33.3% win rate)
- Adaptive/Aggressive: More specialized use cases (64.9/64.4 avg)

Network Conditions Impact:

The session concurrency limits and unloaded latency properly differentiate network performance:
- Localhost: 81.4 avg score (low latency, high concurrency)
- Fiber Gigabit: 74.2 avg score (good bandwidth, moderate concurrency)
- Mobile LTE: 60.3 avg score (limited to 2 concurrent sessions)
- Satellite: 53.3 avg score (high latency, single session limit)
