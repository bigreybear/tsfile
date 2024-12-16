package optimize.nodes;

import java.util.List;

public interface ILeaf extends INode {
  @Override
  default INode getChild(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  default List<INode> getChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  default List<String> getKeys() {
    throw new UnsupportedOperationException();
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
