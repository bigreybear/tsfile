package optimize.nodes.cdm.frame;

import static optimize.nodes.cdm.ByteEncode.bytes2Int;
import static optimize.nodes.cdm.ByteEncode.int2Bytes;
import static optimize.nodes.cdm.ByteEncode.int2BytesNoTrailing;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.util.ArrayHelper.findIntervals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import optimize.SearchStatus;
import optimize.eliasfano.EliasFano;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.INode;
import optimize.nodes.cdm.ByteEncode;
import optimize.nodes.cdm.ICNode;
import optimize.util.ByteArray;
import optimize.util.InfixGroup;

@Deprecated
// enhanced with Elias-Fano coding
public class LegacyCNode4EF extends CNode4 implements ICNode {
  // for only 4 positions
  int posInt; // an int concatenated by 4 unsigned bytes: byte p1, p2, p3, p4;
  byte[] pbk, nbk; // positive/negative compressed array; by negative, it uses bitwise opposite
  int plen, nlen; // length of the original pos
  int plb, nlb; // lower-bits of related array
  ICNode[] ptrs;

  // raw keys might with prefix
  public LegacyCNode4EF(int[] pos) {
    super(pos);
    if (pos.length > 4)
      throw new UnsupportedOperationException("No more than 4 bytes branching key yet.");

    // pos int init.
    byte[] posBytes = new byte[4];
    for (int i = 0; i < pos.length; i++) {
      if ((pos[i] & 0xffffff00) != 0)
        throw new UnsupportedOperationException("Longer than 255 not supported in CDM yet.");
      posBytes[i] = (byte) (pos[i] & 0x000000ff);
    }
    posInt = ByteEncode.bytes2Int(posBytes);
  }

  @Override
  public int[] getBranchingPos() {
    return ICNode.unsignedByteArr2IntArr(int2BytesNoTrailing(posInt));
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
    int[] bps = getBranchingPos();
    if (bps.length == 0) throw new RuntimeException();
    byte[] pk = getParKey();
    int curLen = sts.getCurLen();
    curLen = checkPartialKey(key, curLen, bps[0]);

    // finish searching and is PREFIXED
    if (curLen == key.length) {
      sts.setFinished(true);
      return ptrs[getBrKeyIdx(EMPTY_BYTE_ARR)];
    }

    byte[] tar = extractBytes(key, bps);
    int channel = getBrKeyIdx(bytes2Int(tar));

    sts.setCurLen(checkKeyBytes(key, channel, bps));
    sts.setFinished(sts.getCurLen() == key.length);
    return ptrs[channel];
  }

  @Override
  protected String codeName() {
    return null;
  }

  public void setBranchingKeys(Integer[] branchingBytes) {
    setBranchingKeys(Arrays.asList(branchingBytes));
  }

  public void setBranchingKeys(List<Integer> branchingBytes) {
    ptrs = new ICNode[branchingBytes.size()];

    // init interleaved bytes array
    int[] itvPos = findIntervals(int2BytesNoTrailing(posInt));
    if (itvPos.length > 0) rmk = new byte[branchingBytes.size()][];

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

  private int getBrKeyIdx(byte[] v) {
    throw new UnsupportedOperationException();
  }

  // get index of the target key
  private int getBrKeyIdx(int val) {
    if (val < 0) {
      val &= 0x7fffffff;
      return EliasFano.select(nbk, 0, nlen, nlb, val);
    }

    return nlen + EliasFano.select(pbk, 0, plen, plb, val);
  }

  public void setBranchingPtr(int idx, INode ptr) {
    ptrs[idx] = (ICNode) ptr;
  }

  // @Override
  // public byte[] assembleKeyAt(int pos) {
  //   // fixme todo align with CNode4
  //   byte[] res;
  //   int[] brPosInt = ICNode.unsignedByteArr2IntArr(int2BytesNoTrailing(posInt));
  //   int[] itvPosInt = findIntervals(brPosInt);
  //   byte[] brKey = getBrKeyAt(pos);
  //
  //   int keyLen = brPosInt[brPosInt.length - 1] - brPosInt[0] + 1;
  //
  //   int[] brRltPos = ICNode.shiftIntArr(brPosInt, -1 * brPosInt[0]);
  //   int[] itvRltPos = ICNode.shiftIntArr(itvPosInt, -1 * brPosInt[0]);
  //
  //   byte[] asmkey = new byte[keyLen];
  //   ICNode.setBytesByPos(asmkey, brKey, brRltPos);
  //   if (rmk != null) ICNode.setBytesByPos(asmkey, rmk[pos], itvRltPos);
  //
  //   return asmkey;
  // }

  @Override
  protected void setBranchKeyValRmk(Map<ByteArray, ICNode> m, Map<ByteArray, byte[]> k2r, int actualSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected byte[] getBrKeyAt(int pos) {
    if (pos < nlen) {
      int i = EliasFano.get(nbk, 0, nlen, nlb, pos);
      i |= 0x80000000;
      return int2Bytes(i);
    }

    pos -= nlen;
    return int2Bytes(EliasFano.get(pbk, 0, plen, plb, pos));
  }

  @Override
  protected Object[] setBrKeysAndGenSupFunctions(InfixGroup group) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IMicroNode getLogicalChild(String name) {
    throw new UnsupportedOperationException();
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
  public byte[][] getBranchingKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getEmptyKeyIdx() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getBrKeyIdx(byte[] key, int[] brPos) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int getKeyPos(int k) {
    throw new UnsupportedOperationException();
  }
}
