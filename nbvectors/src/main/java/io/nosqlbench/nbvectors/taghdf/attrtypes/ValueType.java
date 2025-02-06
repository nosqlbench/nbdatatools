package io.nosqlbench.nbvectors.taghdf.attrtypes;

/// This enum represents the different types of values that can be stored in an HDF5 attribute.
/// (At least which are supported by nbvectors for now)
/// These literal forms are supported for automatic type detection:
///
/// - 999999999 - int (any value up to 999999999)
/// - 1000000000 - long (any value greater than 999999999)
/// - 1234.567 - float (any value with a decimal point and fewer than 8 digits)
/// - 123456789.0123456789 - double (any value with a decimal point and more than 8 digits)
/// - 0.5 - float (any value with a decimal point)
/// - 10s - short literal
/// - 10l - long literal
/// - 10i - int literal
/// - 0.5f - float literal
/// - 234f - float literal
/// - 234d - double literal
/// - 23456789.0123456789f - float (extra precisions is discarded for floating due to literal form)
/// - 1234567891234567i - ERROR (extra precision is not discarded for other types)
/// - 0.5d - double literal
///
/// Everything which does not match one of the above forms is taken as a String value
///
/// All types can be qualified as a literal by appending a letter to the end of the value.
/// While it may seem too magical to auto-select the precision of numeric types for common cases,
/// it is a robust simplification. If they try to use a value which is too large for the type,
/// they will fail with a specific error message that explains the problem. If they try to use
/// an explicit type which doesn't parse as such, they will receive an error message about that too.
/// In all other cases, where compatible (big-enough) receiver types are provided or specified,
/// things will simply work as expected.
public enum ValueType {
  BYTE(Byte.class) {
    @Override
    public Byte parse(String value) {
      return Byte.parseByte(
          value.endsWith("B") || value.endsWith("b") ? value.substring(0, value.length() - 1) :
              value);
    }
  }, INT(Integer.class) {
    @Override
    public Integer parse(String value) {
      return Integer.parseInt(
          value.endsWith("I") || value.endsWith("i") ? value.substring(0, value.length() - 1) :
              value);
    }
  }, LONG(Long.class) {
    @Override
    public Long parse(String value) {
      return Long.parseLong(
          value.endsWith("L") || value.endsWith("l") ? value.substring(0, value.length() - 1) :
              value);
    }
  }, SHORT(Short.class) {
    @Override
    public Short parse(String value) {
      return Short.parseShort(
          value.endsWith("S") || value.endsWith("s") ? value.substring(0, value.length() - 1) :
              value);
    }
  }, FLOAT(Float.class) {
    @Override
    public Float parse(String value) {
      return Float.parseFloat(
          value.endsWith("F") || value.endsWith("f") ? value.substring(0, value.length() - 1) :
              value);
    }
  }, DOUBLE(Double.class) {
    @Override
    public Double parse(String value) {
      return Double.parseDouble(
          value.endsWith("D") || value.endsWith("d") ? value.substring(0, value.length() - 1) :
              value);
    }
  }, STRING(String.class) {
    @Override
    public String parse(String value) {
      return value;
    }
  };

  public final Class<?> type;

  ValueType(Class<?> type) {
    this.type = type;
  }

  // Abstract method for parsing the value
  public abstract <T> T parse(String value);

  // Factory method to determine the enum type from the string
  public static ValueType fromLiteral(String value) {
    if (value.matches("[+-]?\\d+[bB]")) {
      return BYTE;
    } else if (value.matches("[+-]?\\d{10,}")) {
      return LONG;
    } else if (value.matches("[+-]?\\d+[lL]")) {
      return LONG;
    } else if (value.matches("[+-]?\\d+[sS]")) {
      return SHORT;
    } else if (value.matches("[+-]?\\d+[iI]?")) {
      return INT;
    } else if (value.matches("[+-]?\\d+\\.\\d+")
               & value.chars().filter(Character::isDigit).count() <= 7)
    {
      return FLOAT;
    } else if (value.matches("[+-]?\\d+\\.\\d+")
               & value.chars().filter(Character::isDigit).count() > 7)
    {
      return DOUBLE;
    } else if (value.matches("[+-]?\\d+(\\.\\d+)?[fF]"))
    {
      return FLOAT;
    } else if (value.matches("[+-]?\\d+(\\.\\d+)?[dD]")) {
      return DOUBLE;
    } else {
      return STRING;
    }
  }

  private int countDigits(String value) {
    return (int) value.chars().filter(Character::isDigit).count();
  }
}
