package optimize.nodes;

import java.util.List;

public interface INode {

  long getValue(); // only for leaves

  INode getChild(String name);

  List<INode> getChildren();

  List<String> getKeys();

  byte[] getPartialKey();

  INode addChild(String name, INode child);

  INode replace(String key, INode nNode);
}
