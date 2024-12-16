package optimize.nodes.logic;

import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** L for Logical Nodes <br/>
 * Both {@linkplain LNodeVDev} and {@linkplain LLeafVDev} are adaption for better encapsulation.*/
public class LNodeVDev implements ITSNode {

  // todo shall be private
  private Map<String, ITSNode> children;

  public LNodeVDev() {}

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
