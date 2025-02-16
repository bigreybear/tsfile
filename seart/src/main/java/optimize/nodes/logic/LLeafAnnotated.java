package optimize.nodes.logic;

import optimize.SearchStatus;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.merge.skeleton.IBNode;
import optimize.merge.skeleton.PartitionInfo;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.IFNode;
import optimize.util.InfixGroup;

import java.util.List;
import java.util.function.Function;

public class LLeafAnnotated extends NodeWithPartialKey implements IMicroNode, IFNode, ICNode, IBNode {
  PartitionInfo info = new PartitionInfo(this);
  long value;

  public LLeafAnnotated(long val) {
    value = val;
  }

  @Override
  public PartitionInfo getInfoObj() {
    return info;
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
  public List<String> getStringKeys() {
    return null; // shall not be removed as it works as mark in recursive traversal
  }

  @Override
  public IFNode get(byte k) {
    return null;
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return null;
  }

  @Override
  public ICNode proceedQueryCDM(byte[] key, SearchStatus sts) {
    if ((pk == null && sts.getCurLen() != key.length) || key.length != checkPartialKey(key, sts.getCurLen())) {
      throw new RuntimeException("Key Search Failed for unknown reason.");
    }
    sts.setFinished(true);
    return this;
  }

  @Override
  public IFNode getFDMChild(byte[] key, SearchStatus sts) {
    if (sts.getCurLen() != key.length) {
      throw new RuntimeException("Key Search Failed for unknown reason.");
    }
    sts.setFinished(true);
    return this;
  }

  @Override
  public IMicroNode getChild(byte[] k) {
    return null;
  }

  // Note(zx) follows are unsupported

  @Override
  public void replace(byte k, IFNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replace(byte[] key, IMicroNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(byte k, IFNode v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChild(byte[] k, IMicroNode n) {
    throw new UnsupportedOperationException();
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
  public List<IMicroNode> getChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getBranchingPos() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][] getBranchingKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setContent(
      InfixGroup group,
      Function<byte[], IMicroNode> getLChild,
      MapType mapType, PrefixMergeStrategy mergeStrategy,
      int height) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] assembleKeyAt(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    if (pk != null) noi.appendEntry("LLeaf_pk_len", pk.length);
  }
}
