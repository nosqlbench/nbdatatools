package io.nosqlbench.nbvectors.common;

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


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Resolve a template String which contains place holders for token values.
///
/// Sections within the template string are replaced with expanded token values and accompanying
/// literal characters. Sections are indicated with square brackets like `[...]`. Each section
/// must contain a token name to be substituted with its value. By default, the token value is
/// _required_ to be resolvable. You can make a token optional by suffixing the token name with `
/// *`. When an optional token value is not found, then the enclosing section is elided from the
/// resulting string.
///
/// Token names may be strictly qualified with curly braces, to disambiguate them from
/// surrounding literal characters which are part of that token's section. For example,
/// `abc[asdf{foobar*}fdsa]xyz` is a template with one (optional) section. If the token name `
/// foobar` is not found, then the whole of `[asdf{foobar*}fdsa]` will be discarded and the
/// result will be `abcxyz`. If a value for foobar _is_ found, say "123" for example, then the
/// result will be `abcasdf123fdsaxyz`.
///
/// If the surrounding literal characters are not part of the token name, and contain no
/// underscores or alphanumeric characters, then you can omit the curly braces.
public class Templatizer {

  private final static Logger logger = LogManager.getLogger(Templatizer.class);
  private final static Pattern scanner = Pattern.compile("\\[[^\\]]+\\]");
  private final Function<String, String> lookup;

  /// @param lookup a function to resolve token names to values
  /// @see Templatizer
  public Templatizer(Function<String, String> lookup) {
    this.lookup = lookup;
  }

  /// Resolve a template String which contains place holders for token values.
  /// @param template the template string to resolve
  /// @param filenameTemplate the template string to resolve
  /// @return the resolved string
  public Optional<String> templatize(String template, String filenameTemplate) {

    Matcher matcher = scanner.matcher(filenameTemplate);
    String newName = matcher.replaceAll(new MatchResolver(template, lookup));
    if (newName.contains("[") || newName.contains("]")) {
      throw new RuntimeException(
          "unresolved tokens in outfile template '" + filenameTemplate + "'");
    }
    return Optional.of(newName);
  }

  /// Resolve a template String which contains place holders for token values.
  public static class MatchResolver implements Function<MatchResult, String> {

    private final String templateForDiagnostics;

    private final static Pattern tokenPattern = Pattern.compile(
        """
            (?<pre>[^}]*)
            \\{ (?<token>.+?) \\}
            (?<post>[^]]*)
            """, Pattern.COMMENTS
    );
    private final static Pattern barePattern = Pattern.compile(
        """
            (?<pre>[^a-zA-Z0-9_]*)
            (?<token>[a-zA-Z0-9_]+)
            (?<post>[^]]*)
            """, Pattern.COMMENTS
    );
    private final Function<String, String> lookup;

    /// This resolves substitutions for token patterns
    /// @param templateForDiagnostics the template string for which this resolver is being used
    /// @param lookup a function to resolve token names to values
    /// @see Templatizer
    public MatchResolver(String templateForDiagnostics, Function<String, String> lookup) {
      this.templateForDiagnostics = templateForDiagnostics;
      this.lookup = lookup;
    }

    @Override
    public String apply(MatchResult mr) {
      String section = mr.group();
      section = section.substring(1, section.length() - 1);

      Matcher matcher1 = tokenPattern.matcher(section);
      Matcher matcher2 = barePattern.matcher(section);
      MatchResult inner = matcher1.matches() ? matcher1 : matcher2.matches() ? matcher2 : null;
      if (inner == null) {
        throw new RuntimeException(
            "unresolved token in outfile template '" + templateForDiagnostics + "': for '" + section
            + "'");
      }
      String token = inner.group("token");
      String before = inner.group("pre");
      String after = inner.group("post");

      boolean optional = token.endsWith("*");
      token = optional ? token.substring(0, token.length() - 1) : token;
      String replacement = lookup.apply(token);
      if (replacement == null) {
        if (optional) {
          return "";
        } else {
          throw new RuntimeException(
              "WARNING: no replacement for token '" + token + "' in outfile template '"
              + templateForDiagnostics + "'");
        }
      }
      String sanitized = replacement.replaceAll("[^a-zA-Z0-9_-]", "");
      if (!sanitized.equals(replacement)) {
        logger.info(
            "sanitized replacement for token '{}' from '{}' to '{}' in outfile template " + "'{}'",
            token,
            replacement,
            sanitized,
            templateForDiagnostics
        );
      }

      return before + sanitized + after;
    }
  }


}
