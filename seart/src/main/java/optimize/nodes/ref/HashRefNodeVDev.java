package optimize.nodes.ref;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.hash.HNodeVDev;
import optimize.nodes.logic.LLeafVDev;
import optimize.util.ByteArray;

public class HashRefNodeVDev implements IMicroNode {
  public byte[] pk;
  public HNodeVDev template;
  public long[] values;

  public static IMicroNode buildHashTemplate(HNodeVDev node) {
    List<byte[]> sortedKeys =
        node.children.keySet().stream().map(ByteArray::getVal).collect(Collectors.toList());
    sortedKeys.sort(Arrays::compare);
    LLeafVDev leaf;
    HNodeVDev tr = new HNodeVDev(sortedKeys.size());
    for (int i = 0; i < sortedKeys.size(); i++) {
      leaf = new LLeafVDev(i);
      tr.setChild(sortedKeys.get(i), leaf);
    }
    return tr;
  }

  public void embedTemplate(HNodeVDev ori, HNodeVDev t) {
    pk = ori.getParKey();
    List<byte[]> keys = ori.getKeyBytes();
    values = new long[keys.size()];
    template = t;
    for (byte[] k : keys) {
      long oriValue = ori.getChild(k).getValue();
      int order = (int) t.getChild(k).getValue();
      values[order] = oriValue;
    }
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return null;
  }

  @Override
  public List<IMicroNode> getChildren() {
    return null;
  }

  @Override
  public IMicroNode getChild(byte[] key) {
    return null;
  }

  @Override
  public void setChild(byte[] k, IMicroNode n) {}

  @Override
  public IMicroNode replace(byte[] key, IMicroNode node) {
    return null;
  }

  @Override
  public ITSNode getLogicalChild(String pathSeg) {
    return null;
  }

  @Override
  public byte[] getParKey() {
    return new byte[0];
  }

  @Override
  public void setParKey(byte[] _pk) {}

  @Override
  public long getValue() {
    return 0;
  }
}
