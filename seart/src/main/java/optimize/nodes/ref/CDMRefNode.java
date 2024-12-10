package optimize.nodes.ref;

import optimize.nodes.INode;
import optimize.nodes.cdm.CNode;
import optimize.nodes.cdm.CNode4;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.FNode256;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.logic.LLeaf;

import java.util.Arrays;
import java.util.List;

import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.ICNode.unsignedByteArr2IntArr;
import static optimize.util.ArrayHelper.removeTrailingZeros;

public class CDMRefNode implements ICNode {
  public byte[] pk;
  public int[] pos;
  public CNode template;
  public long[] values;

  public void embedTemplate(ICNode ori, CNode t) {
    pk = ori.getPartialKey();
    byte[][] keys = ori.getKeysFromCDM();
    values = new long[keys.length];
    template = t;
    pos = ori.getBranchingPos();
    for (byte[] k : keys) {
      long ov = ori.getChildByBytes(k).getValue();
      int order = (int) t.getChildByBytes(k).getValue();
      values[order] = ov;
    }
  }

  public static INode buildCDMTemplate(ICNode node) {
    byte[][] keys = node.getKeysFromCDM();
    int[] fakePos = new int[keys[0].length];

    // why to sort: CNode4 is sorted by int and could be different from byte[]
    Arrays.sort(keys, Arrays::compare);
    LLeaf leaf;
    CNode tr = new CNode(fakePos);
    tr.setBranchingKeysExtended(keys);

    for (int i = 0; i < keys.length; i++) {
      leaf = new LLeaf(i);
      leaf.pk = node.getChildByBytes(keys[i]).getPartialKey();
      tr.ptrs[i] = leaf;
    }
    return tr;
  }

  public long getValFrom(byte k) {
    throw new UnsupportedOperationException();
  }

  public long getValFrom(byte[] k, int _i) {
    // todo check interleaved bytes in further days

    // return values[getChildByBytes(k)]
    byte[] tar = extractBytes(k, pos);
    tar = removeTrailingZeros(tar);
    int vid = (int) template.getChildByBytes(tar).getValue();
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
  public long getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public INode getChild(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<INode> getChildren() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getPartialKey() {
    return pk;
  }

  @Override
  public INode addChild(String name, INode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public INode replace(String key, INode nNode) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBranchingKeys(List<Integer> collect) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPartialKey(byte[] b) {
    pk = b;
  }
}
