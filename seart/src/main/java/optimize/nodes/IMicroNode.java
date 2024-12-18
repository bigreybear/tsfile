package optimize.nodes;

import java.util.ArrayList;
import java.util.List;

public interface IMicroNode extends ITSNode {

  List<byte[]> getKeyBytes();

  List<IMicroNode> getChildren();

  IMicroNode getChild(byte[] key);

  void setChild(byte[] k, IMicroNode n);

  IMicroNode replace(byte[] key, IMicroNode node);

  @Override
  default List<String> getStringKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  default List<ITSNode> getLogicalChildren() {
    return new ArrayList<>(getChildren());
  }

  @Override
  default ITSNode addChild(String key, ITSNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void replace(String key, ITSNode node) {
    throw new UnsupportedOperationException();
  }
}
