package optimize.nodes.cdm;

import static optimize.nodes.cdm.CNodeHelper.bytes2Int;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.nodes.cdm.CNodeHelper.int2BytesFixedLen;
import static optimize.nodes.cdm.CNodeHelper.int2BytesVarLen;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import optimize.eliasfano.EliasFano;
import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;

// enhanced with Elias-Fano coding
public class CNode4EF implements INode, IInternal, IStaticNode, ICNode {
  // for only 4 positions
  int posInt; // an int concatenated by 4 unsigned bytes: byte p1, p2, p3, p4;
  byte[] pks; // partial keys
  byte[] pbk, nbk; // positive/negative compressed array; by negative, it uses bitwise opposite
  int plen, nlen; // length of the original pos
  int plb, nlb; // lower-bits of related array
  byte[][] interBytes; // bytes interleaves br keys, same number as branching keys
  ICNode[] ptrs;

  // raw keys might with prefix
  public CNode4EF(int[] pos) {
    if (pos.length > 4)
      throw new UnsupportedOperationException("No more than 4 bytes branching key yet.");

    // pos int init.
    byte[] posBytes = new byte[4];
    for (int i = 0; i < pos.length; i++) {
      if ((pos[i] & 0xffffff00) != 0)
        throw new UnsupportedOperationException("Longer than 255 not supported in CDM yet.");
      posBytes[i] = (byte) (pos[i] & 0x000000ff);
    }
    posInt = CNodeHelper.bytes2Int(posBytes);
  }

  @Override
  public int[] getBranchingPos() {
    return ICNode.unsignedByteArr2IntArr(int2BytesVarLen(posInt));
  }

  @Override
  public void setBranchingKeys(List<Integer> branchingBytes) {
    ptrs = new ICNode[branchingBytes.size()];

    // init interleaved bytes array
    int[] itvPos = findIntervals(int2BytesVarLen(posInt));
    if (itvPos.length > 0) interBytes = new byte[branchingBytes.size()][];

    List<Integer> positiveNumbers =
        branchingBytes.stream().filter(num -> num >= 0).collect(Collectors.toList());
    int[] arr = positiveNumbers.stream().mapToInt(i -> i).toArray();
    plen = arr.length;
    plb = plen == 0 ? -1 : EliasFano.getL(arr[plen - 1], plen);
    pbk = plen == 0 ? null : EliasFano.compress(arr, 0, plen);

    List<Integer> negativeNumbers =
        branchingBytes.stream()
            .filter(num -> num < 0)
            .map(num -> num & 0x7fffffff) /* turn negative to positive while retaining the order */
            .sorted()
            .collect(Collectors.toList());
    arr = negativeNumbers.stream().mapToInt(i -> i).toArray();
    nlen = arr.length;
    nlb = nlen == 0 ? -1 : EliasFano.getL(arr[nlen - 1], nlen);
    nbk = nlen == 0 ? null : EliasFano.compress(arr, 0, nlen);
  }

  // get index of the target key
  @Override
  public int getBrKeyIdx(int val) {
    if (val < 0) {
      val &= 0x7fffffff;
      return EliasFano.select(nbk, 0, nlen, nlb, val);
    }

    return nlen + EliasFano.select(pbk, 0, plen, plb, val);
  }

  @Override
  public void setBranchingPtr(int idx, INode ptr) {
    ptrs[idx] = (ICNode) ptr;
  }

  @Override
  public void setInterleavedBytes(int idx, byte[] ilb) {
    ilb = removeTrailingZeros(ilb);
    if (ilb.length > 0 && interBytes == null)
      throw new RuntimeException("Initial Interleave Bytes Error.");
    if (ilb.length == 0) return;

    interBytes[idx] = ilb;
  }

  @Override
  public void setPartialKey(byte[] b) {
    pks = b;
  }

  @Override
  public byte[] assembleKeyAt(int pos) {
    // fixme todo align with CNode4
    byte[] res;
    int[] brPosInt = ICNode.unsignedByteArr2IntArr(int2BytesVarLen(posInt));
    int[] itvPosInt = findIntervals(brPosInt);
    byte[] brKey = getBrKeyAt(pos);

    int keyLen = brPosInt[brPosInt.length - 1] - brPosInt[0] + 1;

    int[] brRltPos = ICNode.shiftIntArr(brPosInt, -1 * brPosInt[0]);
    int[] itvRltPos = ICNode.shiftIntArr(itvPosInt, -1 * brPosInt[0]);

    byte[] asmkey = new byte[keyLen];
    ICNode.setBytesByPos(asmkey, brKey, brRltPos);
    if (interBytes != null) ICNode.setBytesByPos(asmkey, interBytes[pos], itvRltPos);

    return asmkey;
  }

  private byte[] getBrKeyAt(int pos) {
    if (pos < nlen) {
      int i = EliasFano.get(nbk, 0, nlen, nlb, pos);
      i |= 0x80000000;
      return int2BytesFixedLen(i, 4);
    }

    pos -= nlen;
    return int2BytesFixedLen(EliasFano.get(pbk, 0, plen, plb, pos), 4);
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }

  @Override
  public INode getChild(String name) {
    byte[] kb = name.getBytes(StandardCharsets.UTF_8), cpk, curBrKeys, checkBrKeys;

    ICNode curNode = this;
    int idx = 0; /* idx to read the key */
    int channel = -1; // which ptr to route
    int[] brPos;
    while (idx < kb.length) {
      // check on partial key
      if ((cpk = curNode.getPartialKey()) != null) {
        for (int i = 0; i < cpk.length && idx < kb.length; i++) {
          if (kb[idx] != cpk[i]) throw new RuntimeException("Key not exists: " + name);
          idx++;
        }

        if (idx == kb.length) {
          if (curNode instanceof CLeaf) return ((CLeaf) curNode).ptr;
          // search key is exhausted on partial key, the branching key must be 0000
          channel = curNode.getBrKeyIdx(0);
          curNode = curNode.getPtrByPos(channel);
          break;
        }
      }

      // locate and retrieve brn and itv bytes and verify
      brPos = curNode.getBranchingPos();
      curBrKeys = extractBytes(kb, brPos);
      channel = curNode.getBrKeyIdx(bytes2Int(curBrKeys));
      if (channel < 0) throw new RuntimeException("Key not found: " + name);
      checkBrKeys = curNode.assembleKeyAt(channel);
      for (int i = 0; i < checkBrKeys.length && idx < kb.length; i++) {
        if (checkBrKeys[i] != kb[idx]) throw new RuntimeException();
        idx++;
      }

      curNode = curNode.getPtrByPos(channel);
      if (curNode instanceof CNode) {
        return ((CNode) curNode).getChild(kb, idx);
      }
    }

    // todo fixme IMPROVE
    if (!(curNode instanceof CLeaf)) {
      curNode = curNode.getPtrByPos(curNode.getBrKeyIdx(0));
    }
    return ((CLeaf) curNode).ptr;
  }

  @Override
  public ICNode getPtrByPos(int pos) {
    return ptrs[pos];
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
