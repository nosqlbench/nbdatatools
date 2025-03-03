package io.nosqlbench.taghdf;

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


import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrValue;
import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrSet;
import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrSpec;
import io.nosqlbench.nbvectors.taghdf.attrtypes.ValueType;
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




//
//    HdfAttributeAssignment e2 = HdfAttributeAssignment.parse("varname=(String)values");
//    assertThat(e2).isEqualTo(new HdfAttributeAssignment("/", "varname", "String", "values"));
//
//    HdfAttributeAssignment e3 = HdfAttributeAssignment.parse(":varname=values");
//    assertThat(e3).isEqualTo(new HdfAttributeAssignment("/", "varname", "", "values"));
//
//    HdfAttributeAssignment e4 = HdfAttributeAssignment.parse(".varname=values");
//    assertThat(e4).isEqualTo(new HdfAttributeAssignment("/", "varname", "", "values"));
//
//    HdfAttributeAssignment e5 = HdfAttributeAssignment.parse("/:varname=values");
//    assertThat(e5).isEqualTo(new HdfAttributeAssignment("/", "varname", "", "values"));
//
//    HdfAttributeAssignment e6 = HdfAttributeAssignment.parse("/.varname=values");
//    assertThat(e6).isEqualTo(new HdfAttributeAssignment("/", "varname", "", "values"));
//
//    HdfAttributeAssignment e7 = HdfAttributeAssignment.parse("/group1:varname=values");
//    assertThat(e7).isEqualTo(new HdfAttributeAssignment("/group1", "varname", "", "values"));
//
//    HdfAttributeAssignment e8 = HdfAttributeAssignment.parse("/group1.varname=values");
//    assertThat(e8).isEqualTo(new HdfAttributeAssignment("/group1", "varname", "", "values"));
//
//    HdfAttributeAssignment e9 = HdfAttributeAssignment.parse("/group1/group2:varname=values");
//    assertThat(e9).isEqualTo(new HdfAttributeAssignment("/group1/group2", "varname", "", "values"));
//
//    HdfAttributeAssignment ea = HdfAttributeAssignment.parse("/group1/group2.varname=values");
//    assertThat(ea).isEqualTo(new HdfAttributeAssignment("/group1/group2", "varname", "", "values"));
  }

}
