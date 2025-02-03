package io.nosqlbench.taghdf;

import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrValue;
import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrSet;
import io.nosqlbench.nbvectors.taghdf.attrtypes.AttrSpec;
import io.nosqlbench.nbvectors.taghdf.attrtypes.ValueType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfAttrEntryTest {

  @Test
  public void testFormats() {
    AttrSet e1 = AttrSet.parse("varname=value");
    assertThat(e1).isEqualTo(new AttrSet(
        new AttrSpec("/", "varname"),
        new AttrValue<>(ValueType.STRING, "value", "value")
    ));
    AttrSet e2 = AttrSet.parse("varname=(String)value");
    assertThat(e2).isEqualTo(new AttrSet(
        new AttrSpec("/", "varname"),
        new AttrValue<>(ValueType.STRING, "value", "value")
    ));
    AttrSet e3 = AttrSet.parse(":varname=value");
    assertThat(e3).isEqualTo(new AttrSet(
        new AttrSpec("/", "varname"),
        new AttrValue<>(ValueType.STRING, "value", "value")
    ));




//
//    HdfAttributeAssignment e2 = HdfAttributeAssignment.parse("varname=(String)value");
//    assertThat(e2).isEqualTo(new HdfAttributeAssignment("/", "varname", "String", "value"));
//
//    HdfAttributeAssignment e3 = HdfAttributeAssignment.parse(":varname=value");
//    assertThat(e3).isEqualTo(new HdfAttributeAssignment("/", "varname", "", "value"));
//
//    HdfAttributeAssignment e4 = HdfAttributeAssignment.parse(".varname=value");
//    assertThat(e4).isEqualTo(new HdfAttributeAssignment("/", "varname", "", "value"));
//
//    HdfAttributeAssignment e5 = HdfAttributeAssignment.parse("/:varname=value");
//    assertThat(e5).isEqualTo(new HdfAttributeAssignment("/", "varname", "", "value"));
//
//    HdfAttributeAssignment e6 = HdfAttributeAssignment.parse("/.varname=value");
//    assertThat(e6).isEqualTo(new HdfAttributeAssignment("/", "varname", "", "value"));
//
//    HdfAttributeAssignment e7 = HdfAttributeAssignment.parse("/group1:varname=value");
//    assertThat(e7).isEqualTo(new HdfAttributeAssignment("/group1", "varname", "", "value"));
//
//    HdfAttributeAssignment e8 = HdfAttributeAssignment.parse("/group1.varname=value");
//    assertThat(e8).isEqualTo(new HdfAttributeAssignment("/group1", "varname", "", "value"));
//
//    HdfAttributeAssignment e9 = HdfAttributeAssignment.parse("/group1/group2:varname=value");
//    assertThat(e9).isEqualTo(new HdfAttributeAssignment("/group1/group2", "varname", "", "value"));
//
//    HdfAttributeAssignment ea = HdfAttributeAssignment.parse("/group1/group2.varname=value");
//    assertThat(ea).isEqualTo(new HdfAttributeAssignment("/group1/group2", "varname", "", "value"));
  }

}