package io.nosqlbench.vectordata.utils;
/*
 * Copyright (c) 2022-2023 nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.security.InvalidParameterException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Utility class for converting between different units of measurement.
///
/// This class provides methods for converting between different units of time,
/// data size, and count. It supports parsing string representations of values
/// with units and converting them to the desired unit.
public class UnitConversions {

  private static final Pattern numberFmtPattern = Pattern.compile(
      " *(?<number>(?<whole>[0-9]+)(?<fractional>\\.[0-9]+)?(?<to10power>E[0-9]+)?) *(?<unit>[^ ]+?)? *");
  private static final Pattern numberExponentPattern = Pattern.compile(
      " *(?<pre>.*?)?(?<number>([0-9]+)(\\.[0-9]+)?+)\\^(?<exponent>[0-9]+)(?<post>.*?)?");

  private static final long nanoPerSecond = 1000000000;
  private static final long bytesPerGB = 1000000000;
  private static final long BytesPerGiB = 1024 * 1024 * 1024;

  /// Converts a duration string to milliseconds.
  /// @param duration
  ///     The duration string to convert
  /// @return An Optional containing the duration in milliseconds, or empty if the string couldn't be parsed
  public static Optional<Long> msFor(String duration) {
    return durationFor(Duration.MS, duration);
  }

  /// Converts a duration string to microseconds.
  /// @param duration
  ///     The duration string to convert
  /// @return An Optional containing the duration in microseconds, or empty if the string couldn't be parsed
  public static Optional<Long> microsecondsFor(String duration) {
    return durationFor(Duration.US, duration);
  }

  /// Converts a duration string to nanoseconds.
  /// @param duration
  ///     The duration string to convert
  /// @return An Optional containing the duration in nanoseconds, or empty if the string couldn't be parsed
  public static Optional<Long> nanosecondsFor(String duration) {
    return durationFor(Duration.NS, duration);
  }

  /// Converts a duration string to seconds.
  /// @param duration
  ///     The duration string to convert
  /// @return An Optional containing the duration in seconds, or empty if the string couldn't be parsed
  public static Optional<Long> secondsFor(String duration) {
    return durationFor(Duration.SECOND, duration);
  }

  /// Converts a duration string to minutes.
  /// @param duration
  ///     The duration string to convert
  /// @return An Optional containing the duration in minutes, or empty if the string couldn't be parsed
  public static Optional<Long> minutesFor(String duration) {
    return durationFor(Duration.MINUTE, duration);
  }

  /// Converts a duration string to the specified unit.
  /// @param resultUnit
  ///     The unit to convert to
  /// @param spec
  ///     The duration string to convert
  /// @return An Optional containing the duration in the specified unit, or empty if the string
  ///  couldn't be parsed
  public static Optional<Long> durationFor(Duration resultUnit, String spec) {
    Matcher m = numberFmtPattern.matcher(spec);
    if (m.matches()) {
      String numberpart = m.group("number");
      double base = Double.parseDouble(numberpart);
      String unitpart = m.group("unit");
      if (unitpart != null) {
        Duration durationDuration = Duration.valueOfSuffix(unitpart);
        if (durationDuration == null) {
          throw new RuntimeException("Unable to recognize duration unit:" + unitpart);
        }
        long specnanos = durationDuration.getNanos();
        long resultnanos = resultUnit.getNanos();
        double multiplier = (double) specnanos / (double) resultnanos;
        base = base * multiplier;
      }
      return Optional.of((long) base);
    } else {
      return Optional.empty();

    }
  }

  /// Converts a count string to a double value.
  /// @param spec
  ///     The count string to convert
  /// @return An Optional containing the count as a double, or empty if the string couldn't be parsed
  public static Optional<Double> doubleCountFor(String spec) {
    return convertDoubleCount(Count.UNIT, spec);
  }

  /// Converts a count string to a double value in the specified unit.
  /// @param resultUnit
  ///     The unit to convert to
  /// @param spec
  ///     The count string to convert
  /// @return An Optional containing the count as a double in the specified unit, or empty if the
  ///  string couldn't be parsed
  public static Optional<Double> convertDoubleCount(Count resultUnit, String spec) {
    Matcher e = numberExponentPattern.matcher(spec);
    if (e.matches()) {
      double base = Double.parseDouble(e.group("number"));
      double exponent = Double.parseDouble(e.group("exponent"));
      double value = Math.pow(base, exponent);
      spec = e.group("pre") + value + e.group("post");
    }
    Matcher m = numberFmtPattern.matcher(spec);
    if (m.matches()) {
      String numberpart = m.group("number");
      double base = Double.parseDouble(numberpart);
      String unitpart = m.group("unit");
      if (unitpart != null) {
        Count specifierUnit = Count.valueOfSuffix(unitpart);
        if (specifierUnit == null) {
          throw new RuntimeException("Unable to recognized counts unit:" + unitpart);
        }
        double specifierScale = specifierUnit.getMultiplier();
        double resultScale = resultUnit.getMultiplier();
        double multiplier = (specifierScale / resultScale);
        base *= multiplier;
      }
      return Optional.of(base);
    } else {
      return Optional.empty();
    }

  }

  /// Converts a count string to a long value.
  /// @param spec
  ///     The count string to convert
  /// @return An Optional containing the count as a long, or empty if the string couldn't be parsed
  public static Optional<Long> longCountFor(String spec) {
    spec = longConversions(spec);
    spec = spec.replace("MAX", String.valueOf(Long.MAX_VALUE));
    try {
      long value = Long.parseLong(spec);
      return Optional.of(value);
    } catch (NumberFormatException ignored) {
    }
    return convertLongCount(Count.UNIT, spec);
  }

  /// Converts a count string to a long value in the specified unit.
  /// @param resultUnit
  ///     The unit to convert to
  /// @param spec
  ///     The count string to convert
  /// @return An Optional containing the count as a long in the specified unit, or empty if the
  ///  string couldn't be parsed
  public static Optional<Long> convertLongCount(Count resultUnit, String spec) {
    spec = longConversions(spec);
    Matcher m = numberFmtPattern.matcher(spec);
    if (m.matches()) {
      String numberpart = m.group("number");
      long base = Long.parseLong(numberpart);
      String unitpart = m.group("unit");
      if (unitpart != null) {
        Count specifierUnit = Count.valueOfSuffix(unitpart);
        if (specifierUnit == null) {
          throw new RuntimeException("Unable to recognize counts unit:" + unitpart);
        }
        double specifierScale = specifierUnit.getMultiplier();
        double resultScale = resultUnit.getMultiplier();
        double multiplier = (specifierScale / resultScale);
        base *= (long) multiplier;
      }
      return Optional.of(base);
    } else {
      return Optional.empty();
    }

  }


  /// Converts a bytes string to a double value.
  /// @param spec
  ///     The bytes string to convert
  /// @return An Optional containing the bytes as a double, or empty if the string couldn't be parsed
  public static Optional<Double> bytesFor(String spec) {
    return convertBytes(Bytes.BYTE, spec);
  }

  private static String longConversions(String spec) {
    spec = spec.replace("MAX", String.valueOf(Long.MAX_VALUE));

    Matcher scinoteMatcher = numberFmtPattern.matcher(spec);
    if (scinoteMatcher.matches() && (
        scinoteMatcher.group("to10power") != null || scinoteMatcher.group("fractional") != null))
    {
      double doubleValue = Double.parseDouble(scinoteMatcher.group("number"));
      spec = spec.replace(scinoteMatcher.group("number"), String.valueOf((long) doubleValue));
    }

    Matcher exponentMatcher = numberExponentPattern.matcher(spec);
    if (exponentMatcher.matches()) {
      long number = Long.parseLong(exponentMatcher.group("number"));
      long exponent = Long.parseLong(exponentMatcher.group("exponent"));
      if (number == 2L) {
        if (exponent > 63) {
          throw new InvalidParameterException(
              "Exponent for powers of two must be 63 or less. It is " + exponent);
        }
        long value = 1L << exponent;
        spec = exponentMatcher.group("pre") + value + exponentMatcher.group("post");
      } else {
        spec = exponentMatcher.group("pre") + (long) Math.pow(number, exponent)
               + exponentMatcher.group("post");
      }
    }
    return spec;
  }

  /// Converts a bytes string to a double value in the specified unit.
  /// @param resultUnit
  ///     The unit to convert to
  /// @param spec
  ///     The bytes string to convert
  /// @return An Optional containing the bytes as a double in the specified unit, or empty if the
  ///  string couldn't be parsed
  public static Optional<Double> convertBytes(Bytes resultUnit, String spec) {
    Matcher m = numberFmtPattern.matcher(spec);
    if (m.matches()) {
      String numberpart = m.group("number");
      double base = Double.parseDouble(numberpart);
      String unitpart = m.group("unit");
      if (unitpart != null) {
        Bytes specifierUnit = Bytes.valueOfSuffix(unitpart);
        if (specifierUnit == null) {
          throw new RuntimeException("Unable to recognized duration unit:" + unitpart);
        }
        long specifierScale = specifierUnit.getBytes();
        long resultScale = resultUnit.getBytes();
        double multiplier = (double) specifierScale / (double) resultScale;
        base *= multiplier;
      }
      return Optional.of(base);
    } else {
      return Optional.empty();
    }

  }

  /// Enumeration of count units.
  ///
  /// This enum defines the possible units for count values, such as unit, kilo, mega, etc.
  public enum Count {
    /// Base unit (1.0)
    UNIT("U", "unit", 1.0),
    /// Kilo (1,000)
    KILO("K", "kilo", 1000.0),
    /// Mega (1,000,000)
    MEGA("M", "mega", 1000000.0),
    /// Billion (1,000,000,000)
    BILLION("B", "billion", 1000000000.0),
    /// Giga (1,000,000,000)
    GIGA("G", "giga", 1000000000.0),
    /// Tera (1,000,000,000,000)
    TERA("T", "tera", 1000000000000.0),
    /// Peta (1,000,000,000,000,000)
    PETA("P", "peta", 1000000000000000.0),
    /// Exa (1,000,000,000,000,000,000)
    EXA("E", "exa", 1000000000000000000.0);

    private final String label;
    private final String name;
    private final double multiplier;

    Count(String label, String name, double multiplier) {
      this.label = label;
      this.name = name;
      this.multiplier = multiplier;
    }

    public static Count valueOfSuffix(String suffix) {
      for (Count count : Count.values()) {
        if (count.toString().equalsIgnoreCase(suffix)) {
          return count;
        }
        if (count.label.equalsIgnoreCase(suffix)) {
          return count;
        }
        if (count.name.equalsIgnoreCase(suffix)) {
          return count;
        }
      }
      return null;
    }

    public double getMultiplier() {
      return multiplier;
    }
  }

  /// Enumeration of byte units.
  ///
  /// This enum defines the possible units for byte values, such as byte, kilobyte, megabyte, etc.
  public enum Bytes {
    /// Byte (1 byte)
    BYTE("B", "byte", 1),
    /// Kilobyte (1,000 bytes)
    KB("KB", "kilobyte", 1000),
    /// Megabyte (1,000,000 bytes)
    MB("MB", "megabyte", 1000000),
    /// Gigabyte (1,000,000,000 bytes)
    GB("GB", "gigabyte", bytesPerGB),
    /// Terabyte (1,000,000,000,000 bytes)
    TB("TB", "terabyte", bytesPerGB * 1000),
    /// Petabyte (1,000,000,000,000,000 bytes)
    PB("PB", "petabyte", bytesPerGB * 1000000),
    /// Exabyte (1,000,000,000,000,000,000 bytes)
    EB("EB", "exabyte", bytesPerGB * bytesPerGB),

    /// Kibibyte (1,024 bytes)
    KIB("KiB", "kibibyte", 1024),
    /// Mebibyte (1,048,576 bytes)
    MIB("MiB", "mebibyte", 1024 * 1024),
    /// Gibibyte (1,073,741,824 bytes)
    GIB("GiB", "gibibyte", BytesPerGiB),
    /// Tebibyte (1,099,511,627,776 bytes)
    TIB("TiB", "tebibyte", BytesPerGiB * 1024),
    /// Pebibyte (1,125,899,906,842,624 bytes)
    PIB("PIB", "pebibyte", BytesPerGiB * 1024 * 1024),
    /// Exbibyte (1,152,921,504,606,846,976 bytes)
    EIB("EiB", "exbibyte", BytesPerGiB * BytesPerGiB);

    private final String name;
    private final long bytes;
    private final String label;

    Bytes(String label, String name, long bytes) {
      this.label = label;
      this.name = name;
      this.bytes = bytes;
    }

    /// get the canonical unit value of a given suffix
    /// @param unitpart - The string provided by the user
    /// @return The matching [#Bytes] enum
    ///
    public static Bytes valueOfSuffix(String unitpart) {
      for (Bytes byteUnit : Bytes.values()) {
        if (byteUnit.label.equalsIgnoreCase(unitpart)) {
          return byteUnit;
        }
        if (byteUnit.name.equalsIgnoreCase(unitpart)) {
          return byteUnit;
        }
        if ((byteUnit.name.toLowerCase() + "s").equals(unitpart.toLowerCase())) {
          return byteUnit;
        }
        if (byteUnit.toString().equalsIgnoreCase(unitpart)) {
          return byteUnit;
        }
      }
      return null;
    }

    public long getBytes() {
      return bytes;
    }
  }

  /// Enumeration of duration units.
  ///
  /// This enum defines the possible units for duration values, such as nanoseconds, microseconds,
  /// milliseconds, etc.
  public static enum Duration {
    /// Second (1,000,000,000 nanoseconds)
    SECOND("s", "seconds", nanoPerSecond),
    /// Millisecond (1,000,000 nanoseconds)
    MS("ms", "milliseconds", 1000000),
    /// Microsecond (1,000 nanoseconds)
    US("µs", "microseconds", 1000),
    /// Nanosecond (1 nanosecond)
    NS("ns", "nanoseconds", 1),
    /// Minute (60,000,000,000 nanoseconds)
    MINUTE("M", "minutes", nanoPerSecond * 60),
    /// Hour (3,600,000,000,000 nanoseconds)
    HOUR("H", "hours", nanoPerSecond * 60 * 60),
    /// Day (86,400,000,000,000 nanoseconds)
    DAY("D", "days", nanoPerSecond * 60 * 60 * 24),
    /// Week (604,800,000,000,000 nanoseconds)
    WEEK("W", "weeks", nanoPerSecond * 60 * 60 * 24 * 7),
    /// Year (31,536,000,000,000,000 nanoseconds)
    YEAR("Y", "years", nanoPerSecond * 60 * 60 * 24 * 365);


    private final String name;
    private final String label;
    private final long nanos;

    Duration(String label, String name, long nanos) {
      this.label = label;
      this.name = name;
      this.nanos = nanos;
    }

    /// get the canonical unit value of a given suffix
    /// @param spec - The string provided by the user
    /// @return a Duration object
    public static Duration valueOfSuffix(String spec) {
      for (Duration duration : Duration.values()) {
        if (duration.label.equalsIgnoreCase(spec)) {
          return duration;
        }
        if (duration.toString().equalsIgnoreCase(spec)) {
          return duration;
        }
        if (duration.name.equalsIgnoreCase(spec)) {
          return duration;
        }
      }
      return null;
    }

    public long getNanos() {
      return nanos;
    }
  }


}
