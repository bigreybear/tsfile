package mtree;

import java.util.HashMap;
import java.util.Map;

public class MNode implements IMNode {
  Map<String, IMNode> children;

  public MNode() {}

  public boolean hasChild(String name) {
    return children != null && children.containsKey(name);
  }

  @Override
  public long getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public IMNode getChild(String name) {
    return children == null ? null : children.getOrDefault(name, null);
  }

  @Override
  public void addChild(String name, IMNode child) {
    if (children == null) {
      this.children = new HashMap<>();
    }

    if (hasChild(name)) {
      throw new RuntimeException("Duplicated children: " + name);
    }
    children.put(name, child);
  }
}
