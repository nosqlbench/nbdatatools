package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import org.apache.parquet.example.data.Group;

import java.util.Arrays;
import java.util.function.Function;

public class HFEmbedToLongIndexedFloatVector implements Function<Group, LongIndexedFloatVector> {

  @Override
  public LongIndexedFloatVector apply(Group group) {
    Group emb = group.getGroup("emb", 0);
    int repetition = emb.getFieldRepetitionCount("list");
    float[] floats = new float[repetition];

    for (int i = 0; i < repetition; i++) {
      Group listGroup = emb.getGroup("list", i);
      float itemFloat = listGroup.getFloat("item", 0);
      floats[i]=itemFloat;
    }
    System.out.println(Arrays.toString(floats));
    return null;
  }
}
