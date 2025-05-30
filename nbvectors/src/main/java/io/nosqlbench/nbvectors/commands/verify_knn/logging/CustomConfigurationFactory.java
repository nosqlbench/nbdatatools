package io.nosqlbench.nbvectors.commands.verify_knn.logging;

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


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.plugins.Plugin;

/// A configuration factor to programmatically configure log4j
@Plugin(name = "CustomConfigurationFactory", category = ConfigurationFactory.CATEGORY)
@Order(100)
public class CustomConfigurationFactory extends ConfigurationFactory {

  /// create a configuration
  static Configuration createConfiguration(final String name, ConfigurationBuilder<BuiltConfiguration> builder) {
    builder.setConfigurationName(name);
    builder.setStatusLevel(Level.ERROR);
    builder.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.NEUTRAL).
        addAttribute("level", Level.TRACE));
    AppenderComponentBuilder appenderBuilder = builder.newAppender("Stdout", "CONSOLE").
        addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
    appenderBuilder.add(builder.newLayout("PatternLayout").
        addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable"));
    appenderBuilder.add(builder.newFilter("MarkerFilter", Filter.Result.DENY,
        Filter.Result.NEUTRAL).addAttribute("marker", "FLOW"));
    builder.add(appenderBuilder);
    builder.add(builder.newLogger("org.apache.logging.log4j", Level.DEBUG).
        add(builder.newAppenderRef("Stdout")).
        addAttribute("additivity", false));
    builder.add(builder.newRootLogger(Level.TRACE).add(builder.newAppenderRef("Stdout")));
    return builder.build();
  }

  @Override
  protected String[] getSupportedTypes() {
    return new String[0];
  }

  @Override
  public Configuration getConfiguration(LoggerContext loggerContext, ConfigurationSource source) {
    return null;
  }

}
