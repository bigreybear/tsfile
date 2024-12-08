package optimize.nodes.logic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import optimize.nodes.INode;

/** L for Logical Nodes */
public class LNode implements INode {

  // todo shall be private
  public Map<String, INode> children;

  public LNode() {}

  public boolean hasChild(String name) {
    return children != null && children.containsKey(name);
  }



  @Override
  public long getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public INode getChild(String name) {
    return children == null ? null : children.getOrDefault(name, null);
  }

  @Override
  public List<INode> getChildren() {
    return new ArrayList<>(children.values());
  }

  @Override
  public List<String> getKeys() {
    return new ArrayList<>(children.keySet());
  }

  @Override
  public byte[] getPartialKey() {
    return null;
  }

  @Override
  public INode replace(String key, INode nNode) {
    return children.put(key, nNode);
  }

  @Override
  public INode addChild(String name, INode child) {
    if (children == null) {
      this.children = new HashMap<>(1, 1.0f);
    }

    if (hasChild(name)) {
      throw new RuntimeException("Duplicated children: " + name);
    }
    return children.put(name, child);
  }
}
