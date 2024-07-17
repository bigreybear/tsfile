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

public class REDDLoaderV2 extends StreamingLoader<REDDLoaderV2> {

  private String building, meter;

  public REDDLoaderV2(MergedDataSets mds) throws FileNotFoundException {
    super(mds);
  }

  public REDDLoaderV2(File file) throws FileNotFoundException {
    super(file);
  }

  @Override
  protected void updateDeviceIDComponents(String fulDev) {
    String[] nodes = fulDev.split("\\.");
    building = nodes[0];
    meter = nodes[1];
  }

  @Override
  protected void appendIDFieldsToGroup(Group g) {
    g.append("building", building);
    g.append("meter", meter);
  }

  @Override
  protected void appendIDFieldsToParquetFields(List<Type> fields) {
    Types.PrimitiveBuilder<?> builder =
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
    fields.add((Type) builder.named("building"));
    fields.add((Type) builder.named("meter"));
  }
}
