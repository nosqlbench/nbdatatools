package io.nosqlbench.taghdf;

import io.nosqlbench.nbvectors.taghdf.attrtypes.ValueType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValueTypeTest {
  @Test
  public void testBasicTypes() {
    assertEquals(ValueType.INT,ValueType.fromLiteral("123"));
    assertEquals(ValueType.FLOAT,ValueType.fromLiteral("123.456"));
    assertEquals(ValueType.DOUBLE,ValueType.fromLiteral("123.45678901234567890"));
    assertEquals(ValueType.LONG,ValueType.fromLiteral("12345678901234567890"));
    assertEquals(ValueType.FLOAT,ValueType.fromLiteral("123.456"));
    assertEquals(ValueType.DOUBLE,ValueType.fromLiteral("123.456d"));
    assertEquals(ValueType.SHORT,ValueType.fromLiteral("123s"));
    assertEquals(ValueType.LONG,ValueType.fromLiteral("123l"));
    assertEquals(ValueType.LONG,ValueType.fromLiteral("123L"));
    assertEquals(ValueType.SHORT,ValueType.fromLiteral("123S"));
    assertEquals(ValueType.FLOAT,ValueType.fromLiteral("123F"));
    assertEquals(ValueType.DOUBLE,ValueType.fromLiteral("123D"));
    assertEquals(ValueType.INT,ValueType.fromLiteral("123I"));
    assertEquals(ValueType.INT,ValueType.fromLiteral("123i"));
    assertEquals(ValueType.BYTE,ValueType.fromLiteral("123B"));
    assertEquals(ValueType.BYTE,ValueType.fromLiteral("123b"));
    assertEquals(ValueType.INT,ValueType.fromLiteral("123"));
  }

}