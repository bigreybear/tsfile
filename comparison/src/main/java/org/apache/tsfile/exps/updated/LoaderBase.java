package org.apache.tsfile.exps.updated;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.tsfile.exps.GeoLifeLoader;
import org.apache.tsfile.exps.REDDLoader;
import org.apache.tsfile.exps.TDriveLoader;
import org.apache.tsfile.exps.TSBSLoader;
import org.apache.tsfile.exps.conf.MergedDataSets;

import java.io.IOException;

public abstract class LoaderBase {
  public LargeVarCharVector idVector;
  public BigIntVector timestampVector;

  public static LoaderBase getLoader(MergedDataSets mds) throws IOException {
    // following constructor with integer parameter is to create an empty object
    switch (mds) {
      case TSBS:
        return TSBSLoader.deser(mds);
      case REDD:
        return REDDLoader.deser(mds);
      case TDrive:
        return TDriveLoader.deser(mds);
      case GeoLife:
        return GeoLifeLoader.deser(mds);
      default:
        return null;
    }
  }
}
