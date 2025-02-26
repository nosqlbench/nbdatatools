package io.nosqlbench.taghdf;

import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrValue;
import io.nosqlbench.nbvectors.taghdf.attrtypes.ValueType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AttrValueTest {

  @Test
  public void testStringValues() {
    assertThat(AttrValue.parse("values")).isEqualTo(new AttrValue<String>(
        ValueType.STRING,
        "values",
        "values"
    ));
    assertThat(AttrValue.parse("(String)values")).isEqualTo(new AttrValue<String>(
        ValueType.STRING,
        "values",
        "values"
    ));
    assertThrows(NumberFormatException.class, () -> AttrValue.parse("(int)values"));
  }

  @Test
  public void testWholeNumberAutoPrecision() {
    assertThat(AttrValue.parse("123")).isEqualTo(new AttrValue<Integer>(ValueType.INT, "123", 123));
    assertThat(AttrValue.parse("(int)123")).isEqualTo(new AttrValue<Integer>(
        ValueType.INT,
        "123",
        123
    ));
    assertThat(AttrValue.parse("123456789")).isEqualTo(new AttrValue<Integer>(
        ValueType.INT,
        "123456789",
        123456789
    ));
    assertThat(AttrValue.parse("1234567890123456789")).isEqualTo(new AttrValue<Long>(
        ValueType.LONG,
        "1234567890123456789",
        1234567890123456789L
    ));
    assertThrows(NumberFormatException.class, () -> AttrValue.parse("12345678901234567890i"));
    assertThrows(NumberFormatException.class, () -> AttrValue.parse("(int)12345678901234567890"));
  }

  @Test
  public void testFloatingPointExplicitPrecision() {
    assertThat(AttrValue.parse("123.456")).isEqualTo(new AttrValue<Float>(
        ValueType.FLOAT,
        "123.456",
        123.456f
    ));
    assertThat(AttrValue.parse("(float)123.456")).isEqualTo(new AttrValue<Float>(
        ValueType.FLOAT,
        "123.456",
        123.456f
    ));
    assertThat(AttrValue.parse("123.45678901234567890")).isEqualTo(new AttrValue<Double>(
        ValueType.DOUBLE,
        "123.45678901234567890",
        123.45678901234567890d
    ));
    assertThat(AttrValue.parse("(double)123.45678901234567890")).isEqualTo(new AttrValue<Double>(ValueType.DOUBLE,
        "123.45678901234567890",
        123.45678901234567890d
    ));
    assertThat(AttrValue.parse("123.45678901234567890d")).isEqualTo(new AttrValue<Double>(
        ValueType.DOUBLE,
        "123.45678901234567890d",
        123.45678901234567890d
    ));
    assertThat(AttrValue.parse("123.45678901234567890f")).isEqualTo(new AttrValue<Float>(
        ValueType.FLOAT,
        "123.45678901234567890f",
        123.45679f
    ));
  }
}