package io.nosqlbench.command.tag_hdf5;

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


import io.nosqlbench.vectordata.spec.attributes.syntax.AttrValue;
import io.nosqlbench.vectordata.spec.attributes.syntax.AttrSet;
import io.nosqlbench.vectordata.spec.attributes.syntax.AttrSpec;
import io.nosqlbench.vectordata.spec.attributes.syntax.ValueType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfAttrEntryTest {

  @Test
  public void testFormats() {
    AttrSet e1 = AttrSet.parse("varname=values");
    assertThat(e1).isEqualTo(new AttrSet(
        new AttrSpec("/", "varname"),
        new AttrValue<>(ValueType.STRING, "values", "values")
    ));
    AttrSet e2 = AttrSet.parse("varname=(String)values");
    assertThat(e2).isEqualTo(new AttrSet(
        new AttrSpec("/", "varname"),
        new AttrValue<>(ValueType.STRING, "values", "values")
    ));
    AttrSet e3 = AttrSet.parse(":varname=values");
    assertThat(e3).isEqualTo(new AttrSet(
        new AttrSpec("/", "varname"),
        new AttrValue<>(ValueType.STRING, "values", "values")
    ));
  }

}
