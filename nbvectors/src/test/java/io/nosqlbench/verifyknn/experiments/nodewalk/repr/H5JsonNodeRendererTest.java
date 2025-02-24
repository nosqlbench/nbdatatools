package io.nosqlbench.verifyknn.experiments.nodewalk.repr;

import io.nosqlbench.verifyknn.experiments.nodewalk.types.OpType;
import io.nosqlbench.verifyknn.experiments.nodewalk.types.PredicateNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class H5JsonNodeRendererTest {

  public static final String test1 = """
            {
                "conjunction": "none",
                "terms": [
                    {
                        "field": {"name": "firstname"},
                        "operator": "eq",
                        "comparator": {"value": 123}
                    }
                ]
            }
            """;

  @Test
  public void testEx1() {
    PredicateNode p = new PredicateNode(0, OpType.EQ,123);
    H5JsonNodeRenderer h5r = new H5JsonNodeRenderer();
    String result = h5r.apply(p);
    assertThat(result).isEqualTo(test1);
  }

  public static final String test2 = """
        {
            "conjunction": "and",
            "terms": [
                {
                    "field": {"name": "firstname"},
                    "operator": "eq",
                    "comparator": {"value": "Mark"}
                },
                {
                    "field": {"name": "lastname"},
                    "operator": "eq",
                    "comparator": {"value": "Wolters"}
                }
            ]
        }
        """;

  public static final String testExample = """
        {
            "conjunction": "none",
            "terms": [
                {
                    "field": {"name": "firstname"},
                    "operator": "in",
                    "comparator": {"value":
                        ["Mark","Mark's friend Joe"]
                        }
                }
            ]
        }
        """;
  public static final String test3 = """
        {
            "conjunction": "or",
            "terms": [
                {
                    "field": {"name": "highprice"},
                    "operator": "gt",
                    "comparator": {"value": 1000}
                },
                {
                    "field": {"name": "lowprice"},
                    "operator": "lt",
                    "comparator": {"value": 1}
                }
            ]
        }
        """;

}