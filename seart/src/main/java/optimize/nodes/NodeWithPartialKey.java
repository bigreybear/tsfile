package optimize.nodes;

import optimize.exception.PartialKeyCheckException;

public abstract class NodeWithPartialKey {
  protected byte[] pk;

  public static int ubyte(byte b) {
    return b & 0xff;
  }

  public static int ubyte(int i) {
    return i & 0xff;
  }

  public byte[] getParKey() {
    return pk;
  }

  public void setParKey(byte[] _pk) {
    pk = (_pk == null || _pk.length == 0) ? null : _pk;
  }

  protected int checkPartialKey(byte[] key, int curLen, int firstBrPos) {
    // for FDM or Hash types
    if (firstBrPos < 0) {
      for (int i = 0, len = pk.length; i < len; i++) {
        if (pk[i] != key[curLen + i]) throw new PartialKeyCheckException();
      }
      return curLen + pk.length;
    }

    // for CNode/CNode4, only when a gap between branch and curLen should check the partial key
    if (pk != null && firstBrPos > curLen) {
      if (pk.length != firstBrPos - curLen) throw new PartialKeyCheckException();
      for (int i = 0, len = pk.length; i < len; i++) {
        if (pk[i] != key[curLen + i]) throw new PartialKeyCheckException();
      }
      return curLen + pk.length;
    }
    return curLen;
  }

  protected int checkPartialKey(byte[] key, int ofs) {
    if (pk == null) return ofs;
    for (int i = 0; i < pk.length; i++) {
      if (i + ofs >= key.length || key[i+ofs] != pk[i]) throw new PartialKeyCheckException();
    }
    return ofs + pk.length;
  }
}
