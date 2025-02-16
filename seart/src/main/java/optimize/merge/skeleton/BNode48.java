package optimize.merge.skeleton;

import optimize.nodes.fdm.FNode48;

public class BNode48 extends FNode48 implements IBNode {
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
