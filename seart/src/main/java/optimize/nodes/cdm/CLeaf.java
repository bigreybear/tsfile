package optimize.nodes.cdm;

import optimize.nodes.ILeaf;
import optimize.nodes.INode;

import java.util.List;

public class CLeaf implements INode, ICNode {
  byte[] pk;
  INode ptr;

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
