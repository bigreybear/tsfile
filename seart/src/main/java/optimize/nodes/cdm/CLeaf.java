package optimize.nodes.cdm;

import optimize.nodes.ILeaf;
import optimize.nodes.INode;

import java.util.Arrays;
import java.util.List;

public class CLeaf implements ICNode {
  byte[] pk;
  INode ptr;

  public CLeaf(byte[][] pk, int preLen, INode ptr) {
    if (pk.length > 1) throw new UnsupportedOperationException("More than 1 key in CLeaf.");
    if (pk[0].length < preLen) this.pk = null;
    else this.pk = Arrays.copyOfRange(pk[0], preLen, pk[0].length);
    this.ptr = ptr;
  }

  @Override
  public void setPartialKey(byte[] b) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getValue() {
    return 0;
  }

  @Override
  public INode getChild(String name) {
    return null;
  }

  @Override
  public List<INode> getChildren() {
    return null;
  }

  @Override
  public List<String> getKeys() {
    return null;
  }

  @Override
  public byte[] getPartialKey() {
    return new byte[0];
  }

  @Override
  public INode addChild(String name, INode child) {
    return null;
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }

  @Override
  public void setBranchingKeys(List<Integer> collect) {
    throw new UnsupportedOperationException();
  }
}
