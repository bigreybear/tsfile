package optimize.nodes.logic;

import java.util.List;
import java.util.function.Function;

import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.NodeWithPartialKey;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.IFNode;
import optimize.util.InfixGroup;

public class LLeafVDev extends NodeWithPartialKey implements IMicroNode, IFNode, ICNode {
  long value;

  public LLeafVDev(long val) {
    value = val;
  }

  @Override
  public long getValue() {
    return value;
  }

  @Override
  public boolean isLogicalLeaf() {
    return true;
  }

  @Override
  public void replace(byte k, IFNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(byte k, IFNode v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IFNode get(byte k) {
    return null;
  }

  @Override
  public byte[] getKeysFromFDM() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ITSNode getLogicalChild(String pathSeg) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<byte[]> getKeyBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<IMicroNode> getChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setContent(InfixGroup group, Function<byte[], IMicroNode> getLChild, PrefixMergeStrategy mergeStrategy, MapType mapType, int height, boolean EFCoded) {
    throw new UnsupportedOperationException();
  }
}
