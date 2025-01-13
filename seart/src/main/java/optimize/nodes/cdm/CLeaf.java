package optimize.nodes.cdm;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import optimize.SearchStatus;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;
import optimize.util.InfixGroup;

// todo eliminate this class
public class CLeaf extends NodeWithPartialKey implements ICNode {
  public ICNode ptr;

  public CLeaf(byte[][] pk, int preLen, ICNode ptr) {
    if (pk.length > 1) throw new UnsupportedOperationException("More than 1 key in CLeaf.");
    if (pk[0].length < preLen) this.pk = null;
    else this.pk = Arrays.copyOfRange(pk[0], preLen, pk[0].length);
    this.ptr = ptr;
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    noi.incEntry("CLeaf_Count", 1);
  }

  @Override
  public ITSNode getLogicalChild(String pathSeg) {
    return null;
  }

  @Override
  public long getValue() {
    return ptr.getValue();
  }

  public ICNode getChild(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return null;
  }

  @Override
  public List<IMicroNode> getChildren() {
    return Collections.singletonList(ptr);
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
      PrefixMergeStrategy mergeStrategy,
      int height) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ICNode proceedQueryCDM(byte[] key, SearchStatus sts) {
    // the parent did not set finished since an orphan leaf has been eliminated
    if (sts.getCurLen() == key.length) {
      sts.setFinished(true);
      return this;
    }

    // check partial key
    int len = checkPartialKey(key, sts.getCurLen(), -1);
    if (len == key.length) {
      // set finished
      sts.setFinished(true);
      // return next node
      return ptr;
    }
    throw new RuntimeException();
  }
}
