package org.apache.tsfile.exps.updated;

import org.apache.arrow.vector.Float8Vector;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exps.loader.legacy.GeoLifeLoader;
import org.apache.tsfile.exps.loader.legacy.REDDLoader;
import org.apache.tsfile.exps.loader.legacy.TDriveLoader;
import org.apache.tsfile.exps.loader.legacy.TSBSLoader;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.exps.updated.BenchWriter.encodingTsFile;
import static org.apache.tsfile.exps.updated.BenchWriter.compressorTsFile;
import static org.apache.tsfile.exps.updated.BenchWriter.mergedDataSets;

public class TsFileTabletFiller {
  public static double[] lats, lons, alts, vels, elecs, eles;
  public static GeoLifeLoader geoLifeLoader;
  public static TSBSLoader tsbsLoader;
  public static REDDLoader reddLoader;
  public static TDriveLoader tDriveLoader;
  public static Tablet _tablet;

  public static void initFiller(Tablet tablet, Object loader) {
    _tablet = tablet;
    switch (BenchWriter.mergedDataSets) {
      case TSBS:
        tsbsLoader = (TSBSLoader) loader;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        eles = (double[]) tablet.values[2];
        vels = (double[]) tablet.values[3];
        return;
      case REDD:
        reddLoader = (REDDLoader) loader;
        elecs = (double[]) tablet.values[0];
        return;
      case GeoLife:
        geoLifeLoader = (GeoLifeLoader) loader;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        alts = (double[]) tablet.values[2];
        return;
      case TDrive:
        tDriveLoader = (TDriveLoader) loader;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        return;
      default:
    }
  }

  public static void refreshArray(Tablet tablet) {
    switch (mergedDataSets) {
      case TSBS:
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        eles = (double[]) tablet.values[2];
        vels = (double[]) tablet.values[3];
        return;
      case REDD:
        elecs = (double[]) tablet.values[0];
        return;
      case GeoLife:
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        alts = (double[]) tablet.values[2];
        return;
      case TDrive:
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        return;
      default:
    }
  }

  public static void fill(int rowInTablet, int cnt) {
    switch (mergedDataSets) {
      case TDrive:
        lats[rowInTablet] = tDriveLoader.latitudeVector.get(cnt);
        lons[rowInTablet] = tDriveLoader.longitudeVector.get(cnt);
        return;
      case GeoLife:
        lats[rowInTablet] = geoLifeLoader.latitudeVector.get(cnt);
        lons[rowInTablet] = geoLifeLoader.longitudeVector.get(cnt);
        alts[rowInTablet] = geoLifeLoader.altitudeVector.get(cnt);
        return;
      case REDD:
        elecs[rowInTablet] = reddLoader.elecVector.get(cnt);
        return;
      case TSBS:
        setValueWithNull(lats, _tablet.bitMaps[0], cnt, rowInTablet, tsbsLoader.latVec);
        setValueWithNull(lons, _tablet.bitMaps[1], cnt, rowInTablet, tsbsLoader.lonVec);
        setValueWithNull(eles, _tablet.bitMaps[2], cnt, rowInTablet, tsbsLoader.eleVec);
        setValueWithNull(vels, _tablet.bitMaps[3], cnt, rowInTablet, tsbsLoader.velVec);
        return;
      default:
    }
  }

  private static void setValueWithNull(double[] dvs, BitMap bm, int vecIdx, int tabIdx, Float8Vector vec) {
    if (vec.isNull(vecIdx)) {
      bm.mark(tabIdx);
    } else {
      dvs[tabIdx] = vec.get(vecIdx);
    }
  }

  public static List<MeasurementSchema> getSchema() {
    List<MeasurementSchema> schemas = new ArrayList<>();
    switch (BenchWriter.mergedDataSets) {
      case TSBS:
        schemas.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("ele", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("vel", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        return schemas;
      case TDrive:
        schemas.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        return schemas;
      case GeoLife:
        schemas.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        schemas.add(new MeasurementSchema("alt", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        return schemas;
      case REDD:
        schemas.add(new MeasurementSchema("elec", TSDataType.DOUBLE, encodingTsFile, compressorTsFile));
        return schemas;
      default:
        return null;
    }
  }
}
