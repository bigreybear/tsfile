package optimize.nodes.logic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import optimize.nodes.ITSNode;

/**
 * L for Logical Nodes <br>
 * Both {@linkplain LNodeV3} and {@linkplain LLeaf} are adaption for better encapsulation.
 */
public class LNodeV3 implements ITSNode {

  // todo shall be private
  private Map<String, ITSNode> children;

  public LNodeV3() {}

  private boolean hasChild(String name) {
    return children != null && children.containsKey(name);
  }

  @Override
  public long getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ITSNode getLogicalChild(String name) {
    return children == null ? null : children.getOrDefault(name, null);
  }

  @Override
  public List<ITSNode> getLogicalChildren() {
    return new ArrayList<>(children.values());
  }

  @Override
  public List<String> getStringKeys() {
    return new ArrayList<>(children.keySet());
  }

  @Override
  public byte[] getParKey() {
    return null;
  }

  @Override
  public void setParKey(byte[] _pk) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replace(String key, ITSNode nNode) {
    children.put(key, nNode);
  }

  @Override
  public ITSNode addChild(String name, ITSNode child) {
    if (children == null) {
      this.children = new HashMap<>(1, 1.0f);
    }

    if (hasChild(name)) {
      throw new RuntimeException("Duplicated children: " + name);
    }
    return children.put(name, child);
  }
}
