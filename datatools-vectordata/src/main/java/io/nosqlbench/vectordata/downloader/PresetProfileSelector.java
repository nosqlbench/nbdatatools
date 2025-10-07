package io.nosqlbench.vectordata.downloader;

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

import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataView;

import java.util.Objects;
import java.util.Optional;

/// Wrapper that pins a specific profile on an underlying ProfileSelector while preserving the
/// fluent interface. This allows callers to provide combined dataset:profile specifications
/// without losing access to chainable configuration methods such as setCacheDir.
public final class PresetProfileSelector implements ProfileSelector {

  private final ProfileSelector delegate;
  private final String presetProfile;
  private volatile TestDataView cachedView;

  public PresetProfileSelector(ProfileSelector delegate, String presetProfile) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.presetProfile = Objects.requireNonNull(presetProfile, "presetProfile");
  }

  @Override
  public TestDataView profile(String profileName) {
    if (profileName == null) {
      throw new IllegalArgumentException("profileName must not be null");
    }
    String normalized = profileName.trim();
    if (!presetProfile.equalsIgnoreCase(normalized)) {
      throw new IllegalArgumentException(
          "Preset profile is '" + presetProfile + "' but caller requested '" + profileName + "'");
    }
    return resolve(normalized);
  }

  @Override
  public ProfileSelector setCacheDir(String cacheDir) {
    delegate.setCacheDir(cacheDir);
    cachedView = null;
    return this;
  }

  private TestDataView resolve() {
    return resolve(this.presetProfile);
  }

  private TestDataView resolve(String profileName) {
    TestDataView view = cachedView;
    if (view == null) {
      synchronized (this) {
        view = cachedView;
        if (view == null) {
          view = delegate.profile(profileName);
          cachedView = view;
        }
      }
    }
    return view;
  }

  @Override
  public Optional<String> presetProfile() {
    return Optional.of(presetProfile);
  }

  @Override
  public TestDataView profile() {
    return resolve();
  }
}
