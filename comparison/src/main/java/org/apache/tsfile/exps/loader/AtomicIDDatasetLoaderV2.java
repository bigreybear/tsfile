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

public class AtomicIDDatasetLoaderV2 extends StreamingLoader<AtomicIDDatasetLoaderV2> {

  public AtomicIDDatasetLoaderV2(MergedDataSets mds) throws FileNotFoundException {
    super(mds);
  }

  public AtomicIDDatasetLoaderV2(File file) throws FileNotFoundException {
    super(file);
  }

  @Override
  protected void updateDeviceIDComponents(String fulDev) {
  }

  @Override
  protected void appendIDFieldsToGroup(Group g) {
    g.append("deviceID", deviceID);
  }

  @Override
  protected void appendIDFieldsToParquetFields(List<Type> fields) {
    Types.PrimitiveBuilder<?> builder =
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY);
    fields.add((Type) builder.named("deviceID"));
  }
}