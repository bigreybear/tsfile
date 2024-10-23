package mtree;

import seart.SeriesIndexTree;

import java.io.Serializable;
import java.util.List;

public class MTreeMeasure implements SeriesIndexTree, Serializable {
  public IMNode root = new MNode();

  @Override
  public void insert(String p, long value) {
    String[] path = p.split("\\.");
    if (!path[0].equals("root")) {
      throw new RuntimeException("No heading root in :" + p);
    }

    IMNode cur = root, child;
    for (int i = 1; i < path.length; i++) {
      child = cur.getChild(path[i]);
      if (child == null) {
        child = (i == path.length -1 ? new MLeaf(value) : new MNode());
        cur.addChild(path[i], child);
      }
      cur = child;
    }
  }

  public void buildPaths(List<String> paths) {
    String[] path;
    for (String p : paths) {

      path = p.split("\\.");
      if (!path[0].equals("root")) {
        throw new RuntimeException("No heading root in :" + p);
      }

      IMNode cur = root, child;
      for (int i = 1; i < path.length; i++) {
        child = cur.getChild(path[i]);
        if (child == null) {
          child = (i == path.length -1 ? new MLeaf() : new MNode());
          cur.addChild(path[i], child);
        }
        cur = child;
      }
    }
  }

  @Override
  public long search(String path) {
    String[] nodes = path.split("\\.");
    IMNode curNode = root;
    for (int i = 1; i < nodes.length; i++) {
      curNode = curNode.getChild(nodes[i]);
    }
    return curNode.getValue();
  }

  public static void main(String[] args) throws Exception {
    MTreeMeasure measure = new MTreeMeasure();
    PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH);

    List<String> bat;
    while (!(bat = loader.getLines()).isEmpty()) {
      measure.buildPaths(bat);
    }
  }
}
