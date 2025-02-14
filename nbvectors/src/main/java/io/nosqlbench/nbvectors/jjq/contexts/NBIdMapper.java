package io.nosqlbench.nbvectors.jjq.contexts;

import io.nosqlbench.nbvectors.jjq.apis.StatefulShutdown;

public interface NBIdMapper extends StatefulShutdown {
  void addInstance(String fieldName, String string);
  long lookupId(String fieldName, String text);
}
