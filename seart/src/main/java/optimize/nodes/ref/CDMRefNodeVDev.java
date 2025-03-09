package optimize.nodes.ref;

import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.util.List;
import java.util.function.Function;
import optimize.SearchStatus;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.NodeWithPartialKey;
import optimize.nodes.cdm.frame.LegacyCNode;
import optimize.nodes.cdm.ICNode;
import optimize.util.InfixGroup;

public class CDMRefNodeVDev extends NodeWithPartialKey implements ICNode {
  public int[] pos;
  public LegacyCNode template;
  public long[] values;

  public void embedTemplate(ICNode ori, LegacyCNode t) {
    pk = ori.getParKey();
    byte[][] keys = ori.getBranchingKeys();
    values = new long[keys.length];
    template = t;
    pos = ori.getBranchingPos();
    for (byte[] k : keys) {
      // fixme finish this
      // long ov = ori.getChildByBytes(k).getValue();
      // int order = (int) t.getChildByBytes(k).getValue();
      // values[order] = ov;
    }
  }

  public long getValFrom(byte k) {
    throw new UnsupportedOperationException();
  }

  public long getValFrom(byte[] k, int _i) {
    // todo check interleaved bytes in further days

    // return values[getChildByBytes(k)]
    byte[] tar = extractBytes(k, pos);
    tar = removeTrailingZeros(tar);
    int vid = (int) template.getChild(tar).getValue();
    return values[vid];

    //
    // int idx = _i;
    // if (pk != null) {
    //   for (int i = 0; i<pk.length && idx < k.length; i++, idx++){
    //     if (pk[i] != k[idx]) break;
    //   }
    // }
    //
    // byte tar = (idx >= k.length) ? 0 : k[idx];
    // return values[(int) template.get(tar).getValue()];
  }

  @Override
  public ITSNode getLogicalChild(String pathSeg) {
    return null;
  }

  @Override
  public long getValue() {
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
  public IMicroNode getChild(byte[] key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChild(byte[] k, IMicroNode n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replace(byte[] key, IMicroNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ICNode[] getPtrArr() {
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
  public ICNode proceedQueryCDM(byte[] key, SearchStatus sts) {
    throw new UnsupportedOperationException();
  }
}
