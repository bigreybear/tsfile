package org.apache.tsfile.exps.loader.legacy;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.tsfile.exps.conf.MergedDataSets;
import org.apache.tsfile.exps.updated.BenchWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

// ccs datasets from Timecho's nas
public class CCSLoader extends ZYLoader{
  public CCSLoader() throws FileNotFoundException {
  }

  public CCSLoader(MergedDataSets mds) throws FileNotFoundException {
    super(mds);
  }

  public CCSLoader(File file) throws FileNotFoundException {
    super(file);
  }

  /**
   * Only used to test correctness, share the support file with the source.
   */
  @Deprecated
  public static CCSLoader deserFromFile(File file) throws IOException {
    CCSLoader loader = new CCSLoader(file);
    try {
      loader.preprocess(MergedDataSets.ZY);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return loader;
  }

  public static CCSLoader deser(MergedDataSets mds) throws IOException {
    CCSLoader loader = new CCSLoader(mds);
    try {
      loader.preprocess(mds);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return loader;
  }

  @Override
  public void updateDeviceID(String fulDev) {
    if (deviceID != null && deviceID.equals(fulDev)) {
      return;
    }

    if (BenchWriter.currentScheme != null && BenchWriter.currentScheme.toSplitDeviceID()) {
      String[] nodes = fulDev.split("\\.");
      ent = nodes[0];
      dev = nodes[1];
    }
    updateWorkingVectors(fulDev);
    deviceID = fulDev;
  }

  private String ent, dev;

  @Override
  public Group fillGroup(SimpleGroupFactory factory) {
    Group g = factory.newGroup();
    for (Map.Entry<String, ValueVector> entry : workingVectors.entrySet()) {
      if (entry.getValue().isNull(curIdx)) {
        continue;
      }

      Object res = entry.getValue().getObject(curIdx);
      switch (entry.getValue().getField().getFieldType().getType().getTypeID()) {
        case Bool:
          g.append(entry.getKey(), (boolean) res);
          break;
        case Int:
          if (((ArrowType.Int) entry.getValue().getField().getFieldType().getType()).getBitWidth() == 32) {
            g.append(entry.getKey(), (int) res);
            break;
          } else if (((ArrowType.Int) entry.getValue().getField().getFieldType().getType()).getBitWidth() == 64) {
            g.append(entry.getKey(), (long) res);
            break;
          } else {
            throw new RuntimeException("Illegal int width during group filling.");
          }
        case FloatingPoint:
          g.append(entry.getKey(), (float) res);
          break;
        default:
          throw new RuntimeException("Illegal type during group filling.");
      }
    }

    g.append("timestamp", timestampVector.get(curIdx));
    if (BenchWriter.currentScheme.toSplitDeviceID()) {
      g.append("ent", ent);
      g.append("dev", dev);
    } else {
      g.append("deviceID", deviceID);
    }
    return g;
  }
}
