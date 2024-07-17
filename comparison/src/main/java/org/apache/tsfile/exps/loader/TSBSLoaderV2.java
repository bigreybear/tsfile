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

public class TSBSLoaderV2 extends StreamingLoader<TSBSLoaderV2> {

  private String name, fleet, driver;

  public TSBSLoaderV2(MergedDataSets mds) throws FileNotFoundException {
    super(mds);
  }

  public TSBSLoaderV2(File file) throws FileNotFoundException {
    super(file);
  }

  @Override
  protected void updateDeviceIDComponents(String fulDev) {
    String[] nodes = fulDev.split("\\.");
    name = nodes[0];
    fleet = nodes[1];
    driver = nodes[2];
  }

  @Override
  protected void appendIDFieldsToGroup(Group g) {
    g.append("name", name);
    g.append("fleet", fleet);
    g.append("driver", driver);
  }

  @Override
  protected void appendIDFieldsToParquetFields(List<Type> fields) {
    Types.PrimitiveBuilder<?> builder =
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
    fields.add((Type) builder.named("name"));
    fields.add((Type) builder.named("fleet"));
    fields.add((Type) builder.named("driver"));
  }
}
