package optimize.nodes.logic;

import java.util.List;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;

public class LLeafVDev implements IMicroNode {
  public byte[] pk;
  long value;

  public LLeafVDev() {}

  public LLeafVDev(long val) {
    value = val;
  }

  @Override
  public long getValue() {
    return value;
  }

  @Override
  public byte[] getParKey() {
    return pk;
  }

  @Override
  public void setParKey(byte[] _pk) {
    pk = _pk;
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
  public LLeafVDev getChild(byte[] key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChild(byte[] k, IMicroNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IMicroNode replace(byte[] key, IMicroNode node) {
    throw new UnsupportedOperationException();
  }
}
