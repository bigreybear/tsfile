package optimize.resolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import loader.PathTxtLoader;
import seart.utils.PathUtils;

public class XYZCResolver {

  public static void transFile(String dev, String sen, String res) throws Exception {
    PathTxtLoader devLoader = new PathTxtLoader(dev);
    PathTxtLoader senLoader = new PathTxtLoader(sen);

    List<String> allDev = devLoader.getAllLines().stream().distinct().collect(Collectors.toList());
    List<String> allSenLines = senLoader.getAllLines();
    List<String> allSen = new ArrayList<>();
    for (String line : allSenLines) {
      allSen.addAll(Arrays.asList(line.split("\t")));
    }

    if (allSen.size() > 50) allSen = allSen.subList(0, 10);

    List<String> paths = PathUtils.joinStringLists(allDev, allSen);
    PathUtils.dumpStringCollection(res, paths);

    devLoader.close();
    senLoader.close();
  }

  public static void main(String[] args) throws Exception {
    transFile(
        "mtreedata/xyzc/boxmeas_dev.txt",
        "mtreedata/xyzc/boxmeas_sen.txt",
        "mtreedata/xyzc/boxmeas_path.txt");
  }
}
