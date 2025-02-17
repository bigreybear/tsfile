package optimize.nodes.cdm.frame;

import optimize.nodes.cdm.ICNode;
import optimize.util.ByteArray;

import java.nio.charset.StandardCharsets;

public class CNodeEntry {
  byte[] brk, rmk;
  ICNode ptr;

  public CNodeEntry(byte[] b, byte[] r, ICNode p) {brk = b; rmk = r; ptr = p;}

  @Override
  public String toString() {
    return String.format("key: %s, rmk %s",
        new String(brk, StandardCharsets.UTF_8),
        new String(rmk, StandardCharsets.UTF_8));
  }
}
