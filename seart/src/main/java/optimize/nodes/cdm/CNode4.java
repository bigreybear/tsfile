package optimize.nodes.cdm;

import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class CNode4 implements INode, IInternal, IStaticNode {
  // for only 4 positions
  int posInt;// an int concatenated by 4 bytes: byte p1, p2, p3, p4;
  byte[] pks; // partial keys
  int[] bks;// indeed a byte[][4] bks; // branching keys
  byte[][] rmk; // remaining keys
  INode[] ptrs;

  // exactly no padding on 64-jvm, jdk-17, Compressed OOPs

  // raw keys might with prefix
  public CNode4() {
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

