# Vector Space Visualization

The vshapes module provides 3D visualization capabilities using Jzy3D to help understand vector space characteristics visually.

## Setup

### Dependencies

Add Jzy3D dependencies to your project. The visualization dependencies are optional in vshapes to avoid forcing users to include heavy graphics libraries.

**Maven:**
```xml
<dependency>
    <groupId>org.jzy3d</groupId>
    <artifactId>jzy3d-api</artifactId>
    <version>2.2.1</version>
</dependency>
<dependency>
    <groupId>org.jzy3d</groupId>
    <artifactId>jzy3d-native-jogl-awt</artifactId>
    <version>2.2.1</version>
</dependency>
```

**Gradle:**
```gradle
implementation 'org.jzy3d:jzy3d-api:2.2.1'
implementation 'org.jzy3d:jzy3d-native-jogl-awt:2.2.1'
```

### Checking Availability

```java
import io.nosqlbench.vshapes.VectorSpaceVisualization;

if (VectorSpaceVisualization.isVisualizationAvailable()) {
    System.out.println("Visualization capabilities available");
} else {
    System.out.println("Jzy3D not found - visualization disabled");
}
```

## Visualization Types

### LID Scatter Plot

Visualizes vectors in 3D space colored by their Local Intrinsic Dimensionality values.

```java
// Perform analysis
VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);

// Get LID results
LIDMeasure.LIDResult lidResult = analyzer.getLIDResult();

// Create visualization
Object chart = VectorSpaceVisualization.createLIDScatterPlot(
    vectorSpace, 
    lidResult, 
    "LID Analysis - " + vectorSpace.getId()
);

// Display in window
if (chart != null) {
    VectorSpaceVisualization.displayChart(chart);
}
```

**Color Coding:**
- **Blue**: Low LID values (data on low-dimensional manifold)
- **Red**: High LID values (data fills ambient space)
- **Purple/Pink**: Intermediate LID values

### Class Distribution Plot

Visualizes vectors colored by their class labels (requires labeled data).

```java
// Create class-based visualization
Object classChart = VectorSpaceVisualization.createClassScatterPlot(
    vectorSpace, 
    "Class Distribution - " + vectorSpace.getId()
);

// Display the chart
VectorSpaceVisualization.displayChart(classChart);
```

**Color Coding:**
- Each class gets a distinct color (up to 10 predefined colors)
- Unknown/missing labels appear in light gray

### Hubness Analysis Plot

Visualizes vectors with colors indicating their hubness characteristics.

```java
// Get hubness results
HubnessMeasure.HubnessResult hubnessResult = analyzer.getHubnessResult();

// Create hubness visualization
Object hubnessChart = VectorSpaceVisualization.createHubnessScatterPlot(
    vectorSpace, 
    hubnessResult, 
    "Hubness Analysis - " + vectorSpace.getId()
);

VectorSpaceVisualization.displayChart(hubnessChart);
```

**Color Coding:**
- **Red**: Hub points (appear frequently in neighbor lists)
- **Blue**: Anti-hub points (rarely appear in neighbor lists)  
- **Gray**: Normal points

## Saving Visualizations

### Save Single Chart

```java
import java.nio.file.Paths;

// Create chart
Object chart = VectorSpaceVisualization.createLIDScatterPlot(vectorSpace, lidResult, "LID Analysis");

// Save as PNG image
boolean success = VectorSpaceVisualization.saveChartAsImage(
    chart, 
    Paths.get("lid_analysis.png"), 
    1024, 768  // width, height
);

if (success) {
    System.out.println("Chart saved successfully");
}
```

### Create Complete Dashboard

Generate all available visualizations for a dataset:

```java
import java.nio.file.Paths;

// Analyze vector space
VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);

// Create comprehensive visualization dashboard
boolean success = VectorSpaceVisualization.createAnalysisDashboard(
    vectorSpace, 
    report, 
    Paths.get("visualization_output/")
);

if (success) {
    System.out.println("Dashboard created with files:");
    System.out.println("- lid_visualization.png");
    System.out.println("- class_visualization.png (if labels available)");
    System.out.println("- hubness_visualization.png");
}
```

## Complete Example

```java
import io.nosqlbench.vshapes.*;
import java.nio.file.Paths;

public class VisualizationExample {
    public static void main(String[] args) {
        // Check if visualization is available
        if (!VectorSpaceVisualization.isVisualizationAvailable()) {
            System.out.println("Visualization not available - install Jzy3D dependencies");
            return;
        }
        
        // Load your data
        VectorSpace vectorSpace = loadYourVectorSpace();
        
        // Perform analysis
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        // Create and display LID visualization
        LIDMeasure.LIDResult lidResult = analyzer.getLIDResult();
        if (lidResult != null) {
            Object lidChart = VectorSpaceVisualization.createLIDScatterPlot(
                vectorSpace, lidResult, "LID Analysis");
            
            // Show in window
            VectorSpaceVisualization.displayChart(lidChart);
            
            // Save to file
            VectorSpaceVisualization.saveChartAsImage(
                lidChart, Paths.get("lid_analysis.png"), 800, 600);
        }
        
        // Create class visualization if labels available
        if (vectorSpace.hasClassLabels()) {
            Object classChart = VectorSpaceVisualization.createClassScatterPlot(
                vectorSpace, "Class Distribution");
            VectorSpaceVisualization.displayChart(classChart);
        }
        
        // Create hubness visualization
        HubnessMeasure.HubnessResult hubnessResult = analyzer.getHubnessResult();
        if (hubnessResult != null) {
            Object hubnessChart = VectorSpaceVisualization.createHubnessScatterPlot(
                vectorSpace, hubnessResult, "Hubness Analysis");
            VectorSpaceVisualization.displayChart(hubnessChart);
        }
        
        // Or create everything at once
        VectorSpaceVisualization.createAnalysisDashboard(
            vectorSpace, report, Paths.get("output/"));
    }
}
```

## Dimension Handling

### 3D Datasets
Perfect for visualization - all three dimensions are displayed.

### Higher-Dimensional Datasets
Only the first 3 dimensions are visualized. Consider:
- Applying PCA or t-SNE first for better 3D projection
- Using multiple views of different dimension triplets
- Focusing on the most important dimensions

### 2D Datasets
The Z-axis is set to 0, creating an effective 2D view in 3D space.

### 1D Datasets
Both Y and Z axes are set to 0, showing points along the X-axis.

## Interpretation Guide

### LID Visualization Patterns

**Clustered blue regions**: Areas of low intrinsic dimensionality
- Data lies on low-dimensional manifold
- Good candidates for dimensionality reduction
- May indicate natural cluster structure

**Red scattered points**: High intrinsic dimensionality
- Data fills the ambient space
- Complex relationships between features
- May be more difficult to compress

**Mixed colors**: Variable local dimensionality
- Different regions have different complexity
- May indicate multiple manifolds or transition areas

### Class Distribution Patterns

**Well-separated color clusters**: Good class separability
- Classification should be straightforward
- Clear decision boundaries likely exist

**Mixed/overlapping colors**: Poor class separability
- Classification will be challenging
- May need better features or more complex models

**Scattered single points**: Potential outliers or mislabeled data

### Hubness Patterns

**Red hub points**: Central to similarity structure
- Important for recommendation systems
- May bias similarity searches
- Natural cluster centers

**Blue anti-hub points**: Isolated in similarity space
- Potential outliers or unique cases
- Rarely similar to other points
- May indicate rare categories

**Mostly gray**: Uniform hubness distribution
- Good for similarity-based algorithms
- Less bias in nearest-neighbor methods

## Advanced Usage

### Custom Visualization Pipeline

```java
public class CustomVisualization {
    public static void analyzeDataset(VectorSpace data, String name) {
        if (!VectorSpaceVisualization.isVisualizationAvailable()) {
            return;
        }
        
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(data);
        
        // Create output directory
        Path outputDir = Paths.get("visualizations", name);
        
        // Generate all visualizations
        boolean success = VectorSpaceVisualization.createAnalysisDashboard(
            data, report, outputDir);
        
        if (success) {
            // Also save text reports
            try {
                Files.writeString(outputDir.resolve("summary.txt"), 
                                report.getSummary());
                Files.writeString(outputDir.resolve("analysis.json"), 
                                VectorSpaceAnalysisUtils.toJsonReport(report));
                Files.writeString(outputDir.resolve("interpretation.txt"), 
                                VectorSpaceAnalysisUtils.interpretResults(report));
            } catch (IOException e) {
                System.err.println("Error saving reports: " + e.getMessage());
            }
        }
    }
}
```

### Batch Visualization

```java
public void visualizeMultipleDatasets(List<VectorSpace> datasets) {
    for (VectorSpace dataset : datasets) {
        System.out.println("Processing dataset: " + dataset.getId());
        
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(dataset);
        
        Path outputDir = Paths.get("batch_visualizations", dataset.getId());
        VectorSpaceVisualization.createAnalysisDashboard(dataset, report, outputDir);
        
        System.out.println("Visualizations saved to: " + outputDir);
    }
}
```

## Troubleshooting

### Common Issues

**"Jzy3D not found"**
- Install Jzy3D dependencies
- Check classpath configuration
- Verify version compatibility

**Empty/Black Visualizations**
- Check if vector space has data
- Verify vector dimensions are valid (not all NaN/infinite)
- Ensure analysis completed successfully

**Performance Issues**
- Large datasets (>10,000 points) may be slow
- Consider sampling for visualization
- Use faster hardware or reduce image resolution

**Display Issues**
- Requires GUI environment (not headless)
- May need X11 forwarding for remote systems
- Try saving to file instead of displaying

### Platform-Specific Notes

**Linux**: May require additional OpenGL libraries
**macOS**: Should work out of the box with Java
**Windows**: Usually works with standard Java installation

### Memory Considerations

Large visualizations can use significant memory:
- Each point requires coordinate and color objects
- 3D rendering uses GPU memory
- Consider batch processing for many datasets

## Integration with Analysis Workflow

```java
public class AnalysisWithVisualization {
    public void completeAnalysis(VectorSpace data) {
        // 1. Perform numerical analysis
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(data);
        
        // 2. Generate text reports
        System.out.println("=== NUMERICAL ANALYSIS ===");
        System.out.println(report.getSummary());
        System.out.println("\n=== INTERPRETATION ===");
        System.out.println(VectorSpaceAnalysisUtils.interpretResults(report));
        
        // 3. Create visualizations if available
        if (VectorSpaceVisualization.isVisualizationAvailable()) {
            System.out.println("\n=== GENERATING VISUALIZATIONS ===");
            VectorSpaceVisualization.createAnalysisDashboard(
                data, report, Paths.get("analysis_output"));
            System.out.println("Visualizations saved to analysis_output/");
        }
        
        // 4. Export structured data
        try {
            Files.writeString(Paths.get("analysis.csv"), 
                            VectorSpaceAnalysisUtils.toCsvReport(report));
            Files.writeString(Paths.get("analysis.json"), 
                            VectorSpaceAnalysisUtils.toJsonReport(report));
        } catch (IOException e) {
            System.err.println("Error saving structured reports: " + e.getMessage());
        }
    }
}
```

## Next Steps

- See [examples](examples/) for complete visualization workflows
- Check [performance guide](performance-guide.md) for optimizing large datasets
- Learn about [core concepts](core-concepts.md) to better interpret visualizations