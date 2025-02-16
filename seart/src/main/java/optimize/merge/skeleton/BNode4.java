package optimize.merge.skeleton;

import optimize.nodes.fdm.FNode4;

public class BNode4 extends FNode4 implements IBNode{
  public PartitionInfo info = new PartitionInfo(this);

  @Override
  public PartitionInfo getInfoObj() {
    return info;
  }

  @Override
  public String toString() {
    return info.toString();
  }
}
