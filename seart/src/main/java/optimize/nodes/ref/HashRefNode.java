package optimize.nodes.ref;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import optimize.nodes.INode;
import optimize.nodes.hash.HNodeV2;
import optimize.nodes.logic.LLeaf;
import optimize.util.ByteArray;

public class HashRefNode implements INode {
  public byte[] pk;
  public HNodeV2 template;
  public long[] values;

  public static INode buildHashTemplate(HNodeV2 node) {
    List<byte[]> sortedKeys =
        node.children.keySet().stream().map(ByteArray::getVal).collect(Collectors.toList());
    sortedKeys.sort(Arrays::compare);
    LLeaf leaf;
    HNodeV2 tr = new HNodeV2(sortedKeys.size());
    for (int i = 0; i < sortedKeys.size(); i++) {
      leaf = new LLeaf(i);
      tr.add(sortedKeys.get(i), leaf);
    }
    return tr;
  }

  public void embedTemplate(HNodeV2 ori, HNodeV2 t) {
    pk = ori.getPartialKey();
    List<byte[]> keys = ori.getKeyBytes();
    values = new long[keys.size()];
    template = t;
    for (byte[] k : keys) {
      long oriValue = ori.getChildByBytes(k).getValue();
      int order = (int) t.getChildByBytes(k).getValue();
      values[order] = oriValue;
    }
  }

  @Override
  public long getValue() {
    return 0;
  }

  @Override
  public INode getChild(String name) {
    return null;
  }

  @Override
  public List<INode> getChildren() {
    return null;
  }

  @Override
  public List<String> getKeys() {
    return null;
  }

  @Override
  public byte[] getPartialKey() {
    return new byte[0];
  }

  @Override
  public INode addChild(String name, INode child) {
    return null;
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }
}
