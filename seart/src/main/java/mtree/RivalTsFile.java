package mtree;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RivalTsFile {

  public static void main(String[] args) throws Exception {
    File tsFile = new File("tsfileIndex_vs_mtree");
    tsFile.delete();
    TsFileWriter writer = new TsFileWriter(tsFile);

    PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH);

    List<String> bat;

    // build dev-sen map
    String[] pathNodes;
    Map<String, Set<String>> devSen = new HashMap<>();
    while (!(bat = loader.getLines()).isEmpty()) {
      String dev, sen;
      for (String path : bat) {
        pathNodes = PathTxtLoader.getNodes(PathTxtLoader.removeBacktick(path));

        sen = pathNodes[pathNodes.length - 1];
        dev = String.join(".", Arrays.copyOfRange(pathNodes, 0, pathNodes.length - 1));

        if (!devSen.containsKey(dev)) {
          devSen.put(dev, new HashSet<>());
        }
        devSen.get(dev).add(sen);
      }
    }

    Tablet tablet;
    Set<String> sens;
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (String dev : devSen.keySet()) {
      sens = devSen.get(dev);

      schemaList.clear();
      for (String s : sens) {
        schemaList.add(new MeasurementSchema(s, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      }

      tablet = new Tablet(dev, schemaList, 100);
      writer.registerTimeseries(Path.wrapDevPath(dev), schemaList);

      for (String s : sens) {
        tablet.addTimestamp(0, 1100L);
        tablet.addValue(s, 0, 1);
      }

      tablet.rowSize = 1;
      writer.write(tablet);
    }

    writer.close();
    System.out.println(writer.verboseReport());
  }
}
