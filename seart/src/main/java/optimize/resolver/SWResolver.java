package optimize.resolver;

import loader.PathTxtLoader;
import seart.utils.PathUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SWResolver {

  public static void transFile(String dev, String sen, String res) throws Exception {
    PathTxtLoader devLoader = new PathTxtLoader(dev);
    PathTxtLoader senLoader = new PathTxtLoader(sen);

    List<String> allDev = devLoader.getAllLines().stream().distinct().collect(Collectors.toList());
    List<String> allSen = senLoader.getAllLines();

    allDev = allDev.subList(0, 50000);
    if (allSen.size() > 50) allSen = allSen.subList(0, 10);

    List<String> paths = PathUtils.joinStringLists(allDev, allSen);
    PathUtils.dumpStringCollection(res, paths);

    devLoader.close();
    senLoader.close();
  }

  public static void main(String[] args) throws Exception {
    transFile(
        "mtreedata/sw/devid.txt",
        "mtreedata/sw/sen.txt",
        "mtreedata/sw/path.txt");
  }
}
