package org.apache.tsfile.bmtool;


import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5ReaderConfigurator;
import ch.systemsx.cisd.hdf5.IHDF5SimpleReader;
import org.apache.tsfile.fileSystem.fsFactory.HDFSFactory;

import java.io.File;
import java.util.List;

/**
 * Load various datasets and transform into objects could available for TsFile.
 */
public class DataLoader {
  public static String DATA_FILE = "D:\\0006DataSets\\redd.h5";
  public static File DATA_SOURCE;

  DataLoader() {
  }

  public static void setDataFile(String path) {
    DATA_FILE = path;
  }

  public void loadData() {}

  public void close() {}

  private void loadHDF5() {
    System.out.println(HDF5Factory.isHDF5File(DATA_FILE));
    IHDF5ReaderConfigurator configurator = HDF5Factory.configureForReading(DATA_FILE);
    IHDF5Reader reader = HDF5Factory.openForReading(DATA_FILE);
    HDF5CompoundType<MyHDF5Point> da = reader.getNamedCompoundType("/building6/elec/meter9/table", MyHDF5Point.class);
    printDatasets("/", reader);
    reader.close();
  }

  private static void printDatasets(String path, IHDF5Reader reader) {
    List<String> memberNames = reader.object().getGroupMemberPaths(path);
    for (String fullPath : memberNames) {
      if (reader.object().isDataSet(fullPath)) {
        System.out.println(fullPath);
      } else if (reader.object().isGroup(fullPath)) {
        printDatasets(fullPath, reader);
      }
    }
  }

  public class MyHDF5Point {
    public long index;
    public float[] valuesBlock0;

    MyHDF5Point() {}
  }

  public static void main(String[] args) {
    DataLoader loader = new DataLoader();
    loader.loadData();
    loader.loadHDF5();
    loader.close();
  }

  public enum DataSetType {
    HDF5;
  }
}
