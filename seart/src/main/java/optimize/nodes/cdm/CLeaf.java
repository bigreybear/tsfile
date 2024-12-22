package optimize.nodes.cdm;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.INode;
import optimize.nodes.ITSNode;
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
  public ITSNode getLogicalChild(String pathSeg) {
    return null;
  }

  @Override
  public long getValue() {
    return ptr.getValue();
  }

  public ICNode getChild(String name) {
    // fixme todo remove this fast hack
    byte[] bk = name.getBytes(StandardCharsets.UTF_8);
    for (int i = 0; i < bk.length && i < pk.length; i++) {
      if (bk[i] != pk[i]) return null;
    }
    return ptr;
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
  public void setContent(InfixGroup group, Function<byte[], IMicroNode> getLChild, PrefixMergeStrategy mergeStrategy, MapType mapType, int height, boolean EFCoded) {
    throw new UnsupportedOperationException();
  }
}
