package io.nosqlbench.vectordata.download;

import io.nosqlbench.vectordata.ProfileSelector;
import io.nosqlbench.vectordata.TestDataView;
import io.nosqlbench.vectordata.layout.manifest.DSProfile;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public class VirtualProfileSelector implements ProfileSelector {
  private final DatasetEntry datasetEntry;
private Path cacheDir = Path.of(System.getProperty("user.home"), ".cache", "jvector");


  public VirtualProfileSelector(DatasetEntry datasetEntry) {
    this.datasetEntry = datasetEntry;
  }

  public Set<String> profiles() {
    return datasetEntry.profiles().keySet();
  }

  @Override
  public TestDataView profile(String profileName) {
    DSProfile profile = datasetEntry.profiles().get(profileName);
    if (profile==null) {
      profile = datasetEntry.profiles().get(profileName.toLowerCase());
    }
    if (profile==null) {
      profile = datasetEntry.profiles().get(profileName.toUpperCase());
    }
    if (profile==null) {
      throw new RuntimeException("profile " + profileName + "' not found. Available profiles: " + profiles() + ", but not " + profileName);
    }
    VirtualTestDataView view = new VirtualTestDataView( cacheDir,datasetEntry, profile);
    return view;
  }

  @Override
  public ProfileSelector setCacheDir(String cacheDir) {
    this.cacheDir = Path.of(cacheDir.replace("~", System.getProperty("user.home")));
    return this;
  }
}
