package optimize.merge.skeleton;

import optimize.nodes.fdm.FNode256;

public class BNode256 extends FNode256 implements IBNode {
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
