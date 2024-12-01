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
  int posInt;// an int concatenated by 4 bytes: byte p1, p2, p3, p4;
  byte[] pks; // partial keys
  byte[] pbk, nbk; // positive/negative compressed array; by negative, it uses bitwise opposite
  int plen, nlen; // length of the original pos
  int plb, nlb; // lower-bits of related array
  byte[][] rmk; // remaining keys
  INode[] ptrs;

  // exactly no padding on 64-jvm, jdk-17, Compressed OOPs

  // raw keys might with prefix
  public CNode4EF() {
  }

  public void setBranchingKeys(List<Integer> branchingBytes) {
    List<Integer> positiveNumbers = branchingBytes.stream()
        .filter(num -> num >= 0)
        .collect(Collectors.toList());
    int[] arr = positiveNumbers.stream().mapToInt(i->i).toArray();
    plen = arr.length;
    plb = EliasFano.getL(arr[plen - 1], plen);
    pbk = EliasFano.compress(arr, 0, plen);

    List<Integer> negativeNumbers = branchingBytes.stream()
        .filter(num -> num < 0)
        .map(num -> num & 0x7fffffff)
        .sorted()
        .collect(Collectors.toList());
    arr = negativeNumbers.stream().mapToInt(i->i).toArray();
    nlen = arr.length;
    nlb = EliasFano.getL(arr[nlen-1], nlen);
    nbk = EliasFano.compress(arr, 0, nlen);
  }

  public int getBrKeyIdx(int brKey) {
    if (brKey < 0) {
      brKey &= 0x7fffffff;
      return EliasFano.select(nbk, 0, nlen, nlb, brKey);
    }

    return nlen + EliasFano.select(pbk, 0, plen, plb, brKey);
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

