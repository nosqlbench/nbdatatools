# Output Formats

The vshapes module provides multiple output formats for analysis results to suit different use cases.

## Text Summary Format

The default human-readable format provides a comprehensive overview.

### Usage

```java
VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
String summary = report.getSummary();
System.out.println(summary);
```

### Example Output

```
Vector Space Analysis Report
===========================
Vector Space: iris-dataset
Vectors: 150, Dimensions: 4
Has Class Labels: true

Local Intrinsic Dimensionality (LID):
  Mean: 2.35 ± 0.82 (std dev)
  Range: 1.12 to 4.67

Nearest-Neighbor Margin:
  Mean: 1.847 ± 0.623 (std dev)
  Range: 0.234 to 4.112
  Valid Margins: 147/150 (98.0%)

Hubness Analysis:
  Skewness: 0.234
  Hubs: 8 (5.3%)
  Anti-hubs: 3 (2.0%)
  In-degree mean: 10.0 ± 3.2
```

## CSV Export Format

Structured format for data analysis and spreadsheet applications.

### Usage

```java
String csvReport = VectorSpaceAnalysisUtils.toCsvReport(report);
Files.writeString(Paths.get("analysis.csv"), csvReport);
```

### Example Output

```csv
Metric,Value
VectorSpaceId,iris-dataset
VectorCount,150
Dimension,4
HasClassLabels,true
LID_Mean,2.3456
LID_StdDev,0.8234
LID_Min,1.1234
LID_Max,4.6789
Margin_Mean,1.8470
Margin_StdDev,0.6234
Margin_Min,0.2340
Margin_Max,4.1120
Margin_ValidCount,147
Margin_ValidFraction,0.9800
Hubness_Skewness,0.2340
Hubness_HubCount,8
Hubness_AntiHubCount,3
Hubness_HubFraction,0.0533
Hubness_AntiHubFraction,0.0200
Hubness_InDegreeMean,10.0000
Hubness_InDegreeStdDev,3.2000
```

### Processing CSV Data

```python
import pandas as pd

# Load and analyze results
df = pd.read_csv('analysis.csv')
metrics = dict(zip(df['Metric'], df['Value']))

print(f"Dataset has {metrics['VectorCount']} vectors")
print(f"Average LID: {metrics['LID_Mean']:.2f}")
```

## JSON Export Format

Structured format for programmatic processing and API integration.

### Usage

```java
String jsonReport = VectorSpaceAnalysisUtils.toJsonReport(report);
Files.writeString(Paths.get("analysis.json"), jsonReport);
```

### Example Output

```json
{
  "vectorSpaceId": "iris-dataset",
  "vectorCount": 150,
  "dimension": 4,
  "hasClassLabels": true,
  "results": {
    "LID": {
      "mean": 2.3456,
      "stdDev": 0.8234,
      "min": 1.1234,
      "max": 4.6789,
      "k": 20
    },
    "Margin": {
      "mean": 1.8470,
      "stdDev": 0.6234,
      "min": 0.2340,
      "max": 4.1120,
      "validCount": 147,
      "validFraction": 0.9800
    },
    "Hubness": {
      "skewness": 0.2340,
      "hubCount": 8,
      "antiHubCount": 3,
      "hubFraction": 0.0533,
      "antiHubFraction": 0.0200,
      "k": 10,
      "inDegreeStats": {
        "mean": 10.0000,
        "stdDev": 3.2000,
        "min": 2.0000,
        "max": 28.0000
      }
    }
  }
}
```

### Processing JSON Data

```javascript
// JavaScript/Node.js
const fs = require('fs');
const analysis = JSON.parse(fs.readFileSync('analysis.json', 'utf8'));

console.log(`Dataset: ${analysis.vectorSpaceId}`);
console.log(`LID: ${analysis.results.LID.mean.toFixed(2)} ± ${analysis.results.LID.stdDev.toFixed(2)}`);
```

```java
// Java with Jackson
ObjectMapper mapper = new ObjectMapper();
JsonNode analysis = mapper.readTree(new File("analysis.json"));

String datasetId = analysis.get("vectorSpaceId").asText();
double lidMean = analysis.get("results").get("LID").get("mean").asDouble();
```

## Interpretation Format

Human-readable interpretation with actionable insights.

### Usage

```java
String interpretation = VectorSpaceAnalysisUtils.interpretResults(report);
System.out.println(interpretation);
```

### Example Output

```
Vector Space Analysis Interpretation
===================================

Dataset: iris-dataset (150 vectors, 4 dimensions)

Local Intrinsic Dimensionality (LID):
- Moderate intrinsic dimensionality suggests structured but complex data
- High LID variance indicates non-uniform data density

Class Separability (Margin):
- Moderate margin values indicate reasonable class separation
- Most vectors have clear class separation

Hubness Analysis:
- Low skewness suggests relatively uniform neighbor distribution
```

## Raw Data Access

For advanced analysis, access raw results directly:

### Individual Measure Results

```java
// LID per-vector results
LIDMeasure.LIDResult lidResult = analyzer.getLIDResult();
for (int i = 0; i < lidResult.getVectorCount(); i++) {
    double lid = lidResult.getLID(i);
    System.out.printf("Vector %d LID: %.2f\n", i, lid);
}

// Margin per-vector results
MarginMeasure.MarginResult marginResult = analyzer.getMarginResult();
for (int i = 0; i < marginResult.getVectorCount(); i++) {
    if (marginResult.isValidMargin(i)) {
        double margin = marginResult.getMargin(i);
        System.out.printf("Vector %d margin: %.3f\n", i, margin);
    }
}

// Hubness per-vector results
HubnessMeasure.HubnessResult hubnessResult = analyzer.getHubnessResult();
for (int i = 0; i < hubnessResult.getVectorCount(); i++) {
    int inDegree = hubnessResult.getInDegree(i);
    double hubnessScore = hubnessResult.getHubnessScore(i);
    boolean isHub = hubnessResult.isHub(i);
    System.out.printf("Vector %d: in-degree=%d, score=%.2f, hub=%s\n", 
                     i, inDegree, hubnessScore, isHub);
}
```

### Statistical Summaries

```java
VectorUtils.Statistics lidStats = lidResult.getStatistics();
System.out.printf("LID Statistics: mean=%.2f, std=%.2f, range=[%.2f, %.2f]\n",
                 lidStats.mean, lidStats.stdDev, lidStats.min, lidStats.max);
```

## Custom Output Formats

Create your own output format by accessing the raw data:

```java
public class CustomReporter {
    public static String toXmlReport(AnalysisReport report) {
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<analysis>\n");
        
        xml.append(String.format("  <dataset id=\"%s\" vectors=\"%d\" dimensions=\"%d\"/>\n",
                                report.vectorSpace.getId(),
                                report.vectorSpace.getVectorCount(),
                                report.vectorSpace.getDimension()));
        
        // Add measure results...
        LIDMeasure.LIDResult lidResult = report.getResult("LID", LIDMeasure.LIDResult.class);
        if (lidResult != null) {
            xml.append("  <lid>\n");
            xml.append(String.format("    <mean>%.4f</mean>\n", lidResult.statistics.mean));
            xml.append(String.format("    <stddev>%.4f</stddev>\n", lidResult.statistics.stdDev));
            xml.append("  </lid>\n");
        }
        
        xml.append("</analysis>");
        return xml.toString();
    }
}
```

## Batch Processing

For multiple datasets, collect results in a structured format:

```java
public class BatchReporter {
    private List<Map<String, Object>> results = new ArrayList<>();
    
    public void addDataset(String name, AnalysisReport report) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("name", name);
        entry.put("vectors", report.vectorSpace.getVectorCount());
        entry.put("dimensions", report.vectorSpace.getDimension());
        
        LIDMeasure.LIDResult lid = report.getResult("LID", LIDMeasure.LIDResult.class);
        if (lid != null) {
            entry.put("lid_mean", lid.statistics.mean);
            entry.put("lid_std", lid.statistics.stdDev);
        }
        
        results.add(entry);
    }
    
    public String toCsv() {
        StringBuilder csv = new StringBuilder();
        csv.append("Dataset,Vectors,Dimensions,LID_Mean,LID_Std\n");
        for (Map<String, Object> entry : results) {
            csv.append(String.format("%s,%d,%d,%.4f,%.4f\n",
                                    entry.get("name"),
                                    entry.get("vectors"),
                                    entry.get("dimensions"),
                                    entry.get("lid_mean"),
                                    entry.get("lid_std")));
        }
        return csv.toString();
    }
}
```

## Integration with External Tools

### R Integration

```r
# Load CSV results
analysis <- read.csv("analysis.csv")
metrics <- setNames(analysis$Value, analysis$Metric)

# Plot LID distribution (requires per-vector data)
lid_mean <- as.numeric(metrics["LID_Mean"])
lid_std <- as.numeric(metrics["LID_StdDev"])

cat("Average LID:", lid_mean, "±", lid_std, "\n")
```

### Python Integration

```python
import json
import pandas as pd
import matplotlib.pyplot as plt

# Load JSON results
with open('analysis.json') as f:
    analysis = json.load(f)

# Extract key metrics
lid_mean = analysis['results']['LID']['mean']
margin_mean = analysis['results']['Margin']['mean']
hubness_skew = analysis['results']['Hubness']['skewness']

# Visualize (conceptual - requires per-vector data for actual plots)
print(f"Dataset: {analysis['vectorSpaceId']}")
print(f"LID: {lid_mean:.2f}")
print(f"Class separability: {margin_mean:.2f}")
print(f"Hubness: {hubness_skew:.2f}")
```

## Performance Considerations

### Large Datasets

For large datasets, consider:
- Streaming output to files rather than keeping in memory
- Selective reporting (only include needed metrics)
- Compressed output formats for storage

### Real-time Applications

For real-time dashboards:
- Use JSON format for API endpoints
- Cache formatted results
- Update only changed metrics

## Next Steps

- Check out [performance guide](performance-guide.md) for optimization strategies
- See [examples](examples/) for complete integration examples
- Learn about [visualization exports](visualization.md) for graphical output