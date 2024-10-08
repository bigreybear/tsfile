package mtree;

import java.util.List;

public class MTreeMeasure {
  MNode root = new MNode("root");

  // path statistics
  int ttlPathLen = 0; // total char except dots
  int ttlPathSeg = 0; // seg is separated by dots
  int maxPathSeg = 0; // maximal path seg

  public void buildPaths(List<String> paths) {
    for (String p : paths) {
      String pp = PathTxtLoader.removeBacktick(p);
      ttlPathLen += pp.length();

      String[] path = PathTxtLoader.getNodes(pp);
      ttlPathLen -= path.length - 1;
      ttlPathSeg += path.length;
      maxPathSeg = path.length > maxPathSeg ? path.length : maxPathSeg;

      if (!path[0].equals("root")) {
        throw new RuntimeException("No heading root.");
      }

      MNode cur = root, child;
      for (int i = 1; i < path.length; i++) {
        child = cur.getChild(path[i]);
        if (child == null) {
          child = new MNode(path[i]);
          cur.addChild(child);
        }
        cur = child;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    MTreeMeasure measure = new MTreeMeasure();
    PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH);

    List<String> bat;
    while (!(bat = loader.getLines()).isEmpty()) {
      measure.buildPaths(bat);
    }

    System.out.println(measure.root.getMetric());
    System.out.println(String.format("total path length : %d, total string segments: %d", measure.ttlPathLen, measure.ttlPathSeg));
    System.out.println(String.format("tree depth : %d", measure.maxPathSeg));
  }
}
