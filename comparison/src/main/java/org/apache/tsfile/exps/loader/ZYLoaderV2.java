package org.apache.tsfile.exps.loader;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.loader.legacy.StreamingLoader;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

public class ZYLoaderV2 extends StreamingLoader<ZYLoaderV2> {

  private String ent, dev;

  public ZYLoaderV2(MergedDataSets mds) throws FileNotFoundException {
    super(mds);
  }

  public ZYLoaderV2(File file) throws FileNotFoundException {
    super(file);
  }

  @Override
  protected void updateDeviceIDComponents(String fulDev) {
    String[] nodes = fulDev.split("\\.");
    ent = nodes[0];
    dev = nodes[1];
  }

  @Override
  protected void appendIDFieldsToGroup(Group g) {
    g.append("ent", ent);
    g.append("dev", dev);
  }

  @Override
  protected void appendIDFieldsToParquetFields(List<Type> fields) {
    Types.PrimitiveBuilder<?> builder =
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
    fields.add((Type) builder.named("ent"));
    fields.add((Type) builder.named("dev"));
  }
}
