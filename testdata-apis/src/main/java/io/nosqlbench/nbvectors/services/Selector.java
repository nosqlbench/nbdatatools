package io.nosqlbench.nbvectors.services;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/// A selector is simply a service name to make it possible for different named service to be
/// loaded by annotated name
@Retention(RetentionPolicy.RUNTIME)
public @interface Selector {
  /**
   @return the name of the selector for the annotated type
   */
  String value();
}
