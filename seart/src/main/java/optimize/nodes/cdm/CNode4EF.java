package optimize.nodes.cdm;

import optimize.eliasfano.EliasFano;
import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// enhanced with Elias-Fano coding
public class CNode4EF implements INode, IInternal, IStaticNode {
  // for only 4 positions
  int posInt;// an int concatenated by 4 unsigned bytes: byte p1, p2, p3, p4;
  byte[] pks; // partial keys
  byte[] pbk, nbk; // positive/negative compressed array; by negative, it uses bitwise opposite
  int plen, nlen; // length of the original pos
  int plb, nlb; // lower-bits of related array
  byte[][] interBytes; // bytes interleaves br keys
  INode[] ptrs;


  // raw keys might with prefix
  public CNode4EF(int[] pos) {
    if (pos.length > 4) throw new UnsupportedOperationException("No more than 4 bytes branching key yet.");

    // pos int init.
    byte[] posBytes = new byte[4];
    for (int i = 0; i < pos.length; i++) {
      if ((pos[i] & 0xffffff00) != 0) throw new UnsupportedOperationException("Longer than 255 not supported in CDM yet.");
      posBytes[i] = (byte) (pos[i] & 0x000000ff);
    }
    posInt = CNodeHelper.bytes2Int(posBytes);

    // rem key init
    int remNum = pos[pos.length-1] - pos[0] - pos.length + 1;
    interBytes = remNum == 0 ? null : new byte[remNum][];
  }

  public void setBranchingKeys(List<Integer> branchingBytes) {
    ptrs = new INode[branchingBytes.size()];

    List<Integer> positiveNumbers = branchingBytes.stream()
        .filter(num -> num >= 0)
        .collect(Collectors.toList());
    int[] arr = positiveNumbers.stream().mapToInt(i->i).toArray();
    plen = arr.length;
    plb = EliasFano.getL(arr[plen - 1], plen);
    pbk = EliasFano.compress(arr, 0, plen);

    List<Integer> negativeNumbers = branchingBytes.stream()
        .filter(num -> num < 0)
        .map(num -> num & 0x7fffffff)  /* turn negative to positive while retaining the order */
        .sorted()
        .collect(Collectors.toList());
    arr = negativeNumbers.stream().mapToInt(i->i).toArray();
    nlen = arr.length;
    nlb = EliasFano.getL(arr[nlen-1], nlen);
    nbk = EliasFano.compress(arr, 0, nlen);
  }

  public int getBrKeyIdx(int val) {
    if (val < 0) {
      val &= 0x7fffffff;
      return EliasFano.select(nbk, 0, nlen, nlb, val);
    }

    return nlen + EliasFano.select(pbk, 0, plen, plb, val);
  }

  public void setBranchingPtr(int idx, List<byte[]> cptKeys, INode ptr) {
    if (interBytes != null) {
      // todo extract rmk from cptKeys
    }
    // brKeys already set.
    ptrs[idx] = ptr;
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }

  @Override
  public INode getChild(String name) {
    return null;
  }

  @Override
  public List<INode> getChildren() {
    return Arrays.asList(ptrs);
  }

  @Override
  public List<String> getKeys() {
    return null;
  }

  @Override
  public byte[] getPartialKey() {
    return pks;
  }
}

