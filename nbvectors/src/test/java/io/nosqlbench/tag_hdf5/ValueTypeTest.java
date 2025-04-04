package io.nosqlbench.tag_hdf5;

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


import io.nosqlbench.vectordata.internalapi.attributes.spec.ValueType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValueTypeTest {
  @Test
  public void testBasicTypes() {
    assertEquals(ValueType.INT,ValueType.fromLiteral("-123"));
    assertEquals(ValueType.FLOAT,ValueType.fromLiteral("123.456"));
    assertEquals(ValueType.DOUBLE,ValueType.fromLiteral("123.45678901234567890"));
    assertEquals(ValueType.LONG,ValueType.fromLiteral("12345678901234567890"));
    assertEquals(ValueType.FLOAT,ValueType.fromLiteral("123.456"));
    assertEquals(ValueType.DOUBLE,ValueType.fromLiteral("+123.456d"));
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
