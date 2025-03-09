package optimize.nodes;

import optimize.annotation.DebugOnly;
import optimize.merge.skeleton.PartitionInfo;

import java.util.List;

// to evaluate various implementations
public interface ITSNode {

  ITSNode getLogicalChild(String pathSeg);

  List<ITSNode> getPhysicalChildren();

  List<String> getStringKeys();

  ITSNode addChild(String key, ITSNode n);

  void replace(String key, ITSNode node);

  byte[] getParKey();

  default int getParKeyLen() {
    if (getParKey() == null) return 0;
    return getParKey().length;
  }

  void setParKey(byte[] _pk);

  long getValue();

  default boolean isLogicalLeaf() {
    return false;
  }

  default void acceptInspector(NodeInspector noi) {
    throw new UnsupportedOperationException();
  }

  @DebugOnly
  default PartitionInfo getInfoObj() {throw new UnsupportedOperationException();}
}
