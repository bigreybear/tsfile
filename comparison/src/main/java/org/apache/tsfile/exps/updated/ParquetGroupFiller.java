package org.apache.tsfile.exps.updated;

import org.apache.arrow.vector.Float8Vector;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.tsfile.exps.GeoLifeLoader;
import org.apache.tsfile.exps.REDDLoader;
import org.apache.tsfile.exps.TDriveLoader;
import org.apache.tsfile.exps.TSBSLoader;

/**
 * Serves as a helper to init and update group, which is the unit to write parquet
 */
public class ParquetGroupFiller {
  // id components cache
  public static String deviceID;
  public static String meter;
  public static String building;
  public static String fleet;
  public static String driver;
  public static String name;
  public static GeoLifeLoader geoLifeLoader;
  public static TSBSLoader tsbsLoader;
  public static REDDLoader reddLoader;
  public static TDriveLoader tDriveLoader;

  public static void clear() {
    deviceID = meter = building = fleet = driver = name = null;
  }

  public static void setLoader(LoaderBase loader) {
    switch (BenchWriter.mergedDataSets) {
      case TSBS:
      case TSBS_A:
        tsbsLoader = (TSBSLoader) loader;
        return;
      case REDD:
      case REDD_A:
        reddLoader = (REDDLoader) loader;
        return;
      case TDrive:
        tDriveLoader = (TDriveLoader) loader;
        return;
      case GeoLife:
        geoLifeLoader = (GeoLifeLoader) loader;
        return;
      default:
        return;
    }
  }

  public static void updateDeviceID(String idFromVector) {
    switch (BenchWriter.mergedDataSets) {
      case REDD_A:
      case TSBS_A:
      case GeoLife:
      case TDrive:
        deviceID = idFromVector;
        return;
      case TSBS:
        String[] nodes = idFromVector.split("\\.");
        fleet = nodes[0];
        name = nodes[1];
        driver = nodes[2];
        return;
      case REDD:
        String[] nodes2 = idFromVector.split("\\.");
        building = nodes2[0];
        meter = nodes2[1];
        return;
      default:
    }
  }

  public static Group fill(SimpleGroupFactory factory, Object loader, int cnt) {
    switch (BenchWriter.mergedDataSets) {
      case GeoLife:
        return factory.newGroup()
            .append("deviceID", deviceID)
            .append("timestamp", geoLifeLoader.timestampVector.get(cnt))
            .append("lon", geoLifeLoader.longitudeVector.get(cnt))
            .append("lat", geoLifeLoader.latitudeVector.get(cnt))
            .append("alt", geoLifeLoader.altitudeVector.get(cnt));
      case TDrive:
        return factory.newGroup()
            .append("deviceID", deviceID)
            .append("timestamp", tDriveLoader.timestampVector.get(cnt))
            .append("lon", tDriveLoader.longitudeVector.get(cnt))
            .append("lat", tDriveLoader.latitudeVector.get(cnt));
      case REDD:
        return factory.newGroup()
            .append("building", building)
            .append("meter", meter)
            .append("timestamp", reddLoader.timestampVector.get(cnt))
            .append("elec", reddLoader.elecVector.get(cnt));
      case REDD_A:
        return factory.newGroup()
            .append("deviceID", deviceID)
            .append("timestamp", reddLoader.timestampVector.get(cnt))
            .append("elec", reddLoader.elecVector.get(cnt));
      case TSBS:
        return appendIfNotNull(
            factory.newGroup()
                .append("fleet", fleet)
                .append("name", name)
                .append("driver", driver)
                .append("timestamp", tsbsLoader.timestampVector.get(cnt)),
            tsbsLoader,
            cnt
        );
      case TSBS_A:
        return appendIfNotNull(
            factory.newGroup()
                .append("deviceID", deviceID)
                .append("timestamp", tsbsLoader.timestampVector.get(cnt)),
            tsbsLoader,
            cnt
        );
    }
    return null;
  }

  public static Group appendIfNotNull(Group g, TSBSLoader loader, int cnt) {
    g = appendIfNotNull(g, "lat", loader.latVec, cnt);
    g = appendIfNotNull(g, "lon", loader.lonVec, cnt);
    g = appendIfNotNull(g, "ele", loader.eleVec, cnt);
    g = appendIfNotNull(g, "vel", loader.velVec, cnt);
    return g;
  }

  public static Group appendIfNotNull(Group group, String fn, Float8Vector vec, int idx) {
    if (!vec.isNull(idx)) {
      group.append(fn, vec.get(idx));
    }
    return group;
  }
}