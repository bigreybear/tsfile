package optimize.nodes;

import java.util.List;

public interface ILeaf extends INode{
  @Override
  default INode getChild(String name) {
    throw new UnsupportedOperationException();
  };

  @Override
  default List<INode> getChildren() {
    return null;
  }

  @Override
  default List<String> getKeys() {
    return null;
  }

  @Override
  default INode replace(String key, INode nNode) {
    throw new UnsupportedOperationException();
  }

  @Override
  default INode addChild(String name, INode child) {
    throw new UnsupportedOperationException();
  }
}
