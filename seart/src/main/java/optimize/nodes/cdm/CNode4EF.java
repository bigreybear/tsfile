package optimize.nodes.cdm;

import optimize.eliasfano.EliasFano;
import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.nodes.cdm.CNodeHelper.int2BytesFixedLen;
import static optimize.nodes.cdm.CNodeHelper.int2BytesVarLen;

// enhanced with Elias-Fano coding
public class CNode4EF implements INode, IInternal, IStaticNode, ICNode {
  // for only 4 positions
  int posInt;// an int concatenated by 4 unsigned bytes: byte p1, p2, p3, p4;
  byte[] pks; // partial keys
  byte[] pbk, nbk; // positive/negative compressed array; by negative, it uses bitwise opposite
  int plen, nlen; // length of the original pos
  int plb, nlb; // lower-bits of related array
  byte[][] interBytes; // bytes interleaves br keys, same number as branching keys
  ICNode[] ptrs;


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

    int[] itvPos = findIntervals(pos);
  }

  public void setBranchingKeys(List<Integer> branchingBytes) {
    ptrs = new ICNode[branchingBytes.size()];

    // init interleaved bytes array
    int[] itvPos = findIntervals(int2BytesVarLen(posInt));
    if (itvPos.length > 0) interBytes = new byte[branchingBytes.size()][];

    List<Integer> positiveNumbers = branchingBytes.stream()
        .filter(num -> num >= 0)
        .collect(Collectors.toList());
    int[] arr = positiveNumbers.stream().mapToInt(i->i).toArray();
    plen = arr.length;
    plb = plen == 0 ? -1 : EliasFano.getL(arr[plen - 1], plen);
    pbk = plen == 0 ? null : EliasFano.compress(arr, 0, plen);

    List<Integer> negativeNumbers = branchingBytes.stream()
        .filter(num -> num < 0)
        .map(num -> num & 0x7fffffff)  /* turn negative to positive while retaining the order */
        .sorted()
        .collect(Collectors.toList());
    arr = negativeNumbers.stream().mapToInt(i->i).toArray();
    nlen = arr.length;
    nlb = nlen == 0 ? -1 : EliasFano.getL(arr[nlen-1], nlen);
    nbk = nlen == 0 ? null : EliasFano.compress(arr, 0, nlen);
  }

  // get index of the target key
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
    if (ilb.length > 0 && interBytes == null) throw new RuntimeException("Initial Interleave Bytes Error.");
    if (ilb.length == 0) return;

    interBytes[idx] = ilb;
  }

  @Override
  public void setPartialKey(byte[] b) {pks = b;}

  private int[] unsignedByteArr2IntArr(byte[] b) {
    int [] intArr = new int[b.length];
    for (int i = 0; i < intArr.length; i++) {
      intArr[i] = 0xff & b[i];
    }
    return intArr;
  }

  // pos is the target index within the res[]
  private byte[] setBytesByPos(byte[] res, byte[] src, int[] pos) {
    if (res == null
        || src == null
        || pos == null
        || src.length < pos[pos.length-1])
        // || res.length < src.length) /* no need to equal as branching keys may have trailing 0s */
      throw new RuntimeException("Input Error");

    for (int i = 0; i < pos.length; i++) {
      res[pos[i]] = src[i];
    }

    return res;
  }

  private int[] shiftIntArr(int[] b, int shift) {
    int[] res = new int[b.length];
    for (int i = 0; i < b.length; i++) {
      res[i] = b[i] + shift;
    }
    return res;
  }

  @Override
  public byte[] assembleKeyAt(int pos) {
    byte[] res;
    int[] brPosInt = unsignedByteArr2IntArr(int2BytesVarLen(posInt));
    int[] itvPosInt = findIntervals(brPosInt);
    byte[] brKey = getBrKeyAt(pos);

    // todo debug
    if (brKey[brKey.length-1] == 0) {
      System.out.println("HHH");
    }

    int keyLen = brPosInt[brPosInt.length-1] - brPosInt[0] + 1;

    int[] brRltPos = shiftIntArr(brPosInt, -1 * brPosInt[0]);
    int[] itvRltPos = shiftIntArr(itvPosInt, -1 * brPosInt[0]);

    byte[] asmkey = new byte[keyLen];
    setBytesByPos(asmkey, brKey, brRltPos);
    if (interBytes != null)
      setBytesByPos(asmkey, interBytes[pos], itvRltPos);

    if (pks != null) {
      res = new byte[pks.length + keyLen];
      System.arraycopy(pks, 0, res, 0, pks.length);
      System.arraycopy(asmkey, 0, res, pks.length, asmkey.length);
      asmkey = res;
    }
    return asmkey;
  }

  private byte[] getBrKeyAt(int pos) {
    if (pos < nlen) {
      int i = EliasFano.get(nbk, 0, nlen, nlb, pos);
      i |= 70000000;
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

