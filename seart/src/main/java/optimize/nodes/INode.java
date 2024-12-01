package optimize.nodes;

import java.util.List;

public interface INode {

  long getValue(); // only for leaves

  /**
   * @param name is a segment of the series identifier, may across multiple nodes
   * @return the result corresponds to the whole nodes
   */
  INode getChild(String name);

  List<INode> getChildren();

  List<String> getKeys();

  byte[] getPartialKey();

  INode addChild(String name, INode child);

  INode replace(String key, INode nNode);
}
