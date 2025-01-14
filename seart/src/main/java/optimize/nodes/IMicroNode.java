package optimize.nodes;

import java.util.ArrayList;
import java.util.List;
import optimize.SearchStatus;

public interface IMicroNode extends ITSNode {

  List<byte[]> getKeyBytes();

  List<IMicroNode> getChildren();

  IMicroNode getChild(byte[] key);

  void setChild(byte[] k, IMicroNode n);

  void replace(byte[] key, IMicroNode node);

  default IMicroNode getHashChild(final byte[] key, final SearchStatus sts) {
    throw new UnsupportedOperationException();
  }

  @Override
  default List<String> getStringKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  default List<ITSNode> getPhysicalChildren() {
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
