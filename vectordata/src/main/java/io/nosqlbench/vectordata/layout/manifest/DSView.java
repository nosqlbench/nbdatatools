package io.nosqlbench.vectordata.layout.manifest;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/// Represents a view of data with a name, source, and window.
/// A view defines how to access and filter data from a source.
public class DSView {
  /// The name of the view
  public String name;
  /// The source of the data
  public DSSource source;
  /// The window defining which portions of the data to include
  public DSWindow window;

  /// Creates an empty view with no name, source, or window.
  public DSView() {
  }

  /// Creates a view with the specified name and an empty window.
  /// @param name The name of the view
  public DSView(String name) {
    this.name = name;
    this.window = new DSWindow();
  }

  /// Creates a view with the specified name, source, and window.
  /// @param name The name of the view
  /// @param source The source of the data
  /// @param window The window defining which portions of the data to include
  public DSView(String name, DSSource source, DSWindow window) {
    this.name = name;
    this.source = source;
    this.window = window;
  }

  /// Creates a view from a map of data.
  /// @param v The map of data to create the view from
  /// @return A new DSView instance
  public static DSView fromData(Object v) {
    Map<?,?> datamap = null;
    if (v instanceof Map<?,?> m) {
      datamap = m;
    } else {
      throw new RuntimeException("invalid view format:" + v);
    }
    String name = datamap.containsKey("name") ? datamap.get("name").toString() : null;
    DSSource source = DSSource.fromData(datamap.get("source"));
    DSWindow window = DSWindow.fromData( datamap.get("window"));
    return new DSView(name, source, window);
  }

  /// Sets the source for this view.
  /// @param source The source of the data
  public void setSource(DSSource source) {
    this.source = source;
  }

  /// Sets the window for this view.
  /// @param window The window defining which portions of the data to include
  public void setWindow(DSWindow window) {
    this.window = window;
  }

//  public DSWindow setWindow(List<DSInterval> intervals) {
//    this.window = new DSWindow(intervals);
//    return window;
//  }

  /// Returns a string representation of this view.
  /// @return A string representation of this view
  @Override
  public String toString() {
    return new StringJoiner(", ", DSView.class.getSimpleName() + "[", "]").add(
        "name='" + name + "'").add("source=" + source).add("window=" + window).toString();
  }

  /// Compares this view with another object for equality.
  /// @param o The object to compare with
  /// @return True if the objects are equal, false otherwise
  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof DSView view))
      return false;

    return Objects.equals(name, view.name) && Objects.equals(source, view.source) && Objects.equals(
        window,
        view.window
    );
  }

  /// Returns a hash code for this view.
  /// @return A hash code value for this view
  @Override
  public int hashCode() {
    int result = Objects.hashCode(name);
    result = 31 * result + Objects.hashCode(source);
    result = 31 * result + Objects.hashCode(window);
    return result;
  }

  /// Gets the name of the view.
  /// @return The name of the view
  public String getName() {
    return this.name;
  }

  /// Gets the source of the data.
  /// @return The source of the data
  public DSSource getSource() {
    return this.source;
  }

  /// Gets the window defining which portions of the data to include.
  /// @return The window
  public DSWindow getWindow() {
    return this.window;
  }
}

