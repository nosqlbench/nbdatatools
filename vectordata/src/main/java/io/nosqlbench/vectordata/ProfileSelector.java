package io.nosqlbench.vectordata;

public interface ProfileSelector {
  TestDataView profile(String profileName);
  ProfileSelector setCacheDir(String cacheDir);
}
