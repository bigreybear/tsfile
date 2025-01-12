package optimize.nodes.cdm;

import optimize.SearchStatus;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.util.InfixGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class CNode2 extends CNodeBase implements ICNode {
  byte p1, p2;
  short[] bks;

  @Override
  public List<byte[]> getKeyBytes() {
    List<byte[]> r = new ArrayList<>();
    for (short s : bks) {
      r.add(ByteEncode.short2BytesNoTrailing(s));
    }
    return r;
  }

  @Override
  public IMicroNode getChild(byte[] key) {
    return null;
  }

  @Override
  public void setChild(byte[] k, IMicroNode n) {

  }

  @Override
  public void replace(byte[] key, IMicroNode node) {

  }

  @Override
  public ITSNode getLogicalChild(String pathSeg) {
    return null;
  }

  @Override
  public long getValue() {
    return 0;
  }

  @Override
  protected byte[] getBrKeyAt(int channel) {
    return new byte[0];
  }

  @Override
  public int[] getBranchingPos() {
    if (p2 == 0) return new int[] {p1};
    return new int[] {p1, p2};
  }

  @Override
  public byte[][] getBranchingKeys() {
    return new byte[0][];
  }

  @Override
  public void setContent(InfixGroup group, Function<byte[], IMicroNode> getLChild, PrefixMergeStrategy mergeStrategy, int height) {

  }

  @Override
  public ICNode getCDMChild(byte[] key, SearchStatus sts) {
    return null;
  }
}
