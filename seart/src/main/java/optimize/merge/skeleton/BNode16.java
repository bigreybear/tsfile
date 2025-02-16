package optimize.merge.skeleton;

import optimize.nodes.fdm.FNode16;

public class BNode16 extends FNode16 implements IBNode{
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
