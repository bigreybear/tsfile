package optimize.merge;

import static optimize.nodes.cdm.CNodeHelper.findLCPLength;
import static optimize.nodes.cdm.CNodeHelper.groupPrefixes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import optimize.nodes.INode;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.FNode16;
import optimize.nodes.fdm.FNode256;
import optimize.nodes.fdm.FNode4;
import optimize.nodes.fdm.FNode48;
import optimize.nodes.fdm.IFNode;

public class FDMPrefixMerge {
  public static IFNode generateFNode(int need) {
    if (need <= 4) return new FNode4();
    else if (need <= 16) return new FNode16();
    else if (need <= 48) return new FNode48();
    else return new FNode256();
  }

  public static INode recNextMergeOnFDM(
      INode oriNode, byte[][] keys, int preLen, PrefixMergeStrategy ms, MapType mt, int height) {
    if (ms.equals(PrefixMergeStrategy.SIMPLE) || ms.equals(PrefixMergeStrategy.PARTIAL)) {
      throw new UnsupportedOperationException();
    }

    if (keys.length == 1) {
      IFNode leaf = new FLeaf();
      leaf.setValue(oriNode.getChild(new String(keys[0], StandardCharsets.UTF_8)));
      leaf.setPartialKey(Arrays.copyOfRange(keys[0], preLen, keys[0].length));
      return leaf;
    }

    // get the ptr of 0
    final int len = findLCPLength(keys, preLen);
    // find the key exactly IS the common prefix
    INode prefixedPtr = null;
    List<byte[]> a =
        Arrays.stream(keys).filter(e -> e.length == len + preLen).collect(Collectors.toList());
    if (!a.isEmpty()) {
      prefixedPtr = oriNode.getChild(new String(a.get(0), StandardCharsets.UTF_8));
    }

    // the prefixed key not EXCLUDED
    List<CNodeHelper.ValuedPrefixArray> groupedPrefix = groupPrefixes(keys, len + preLen, 1);
    IFNode repNode = generateFNode(groupedPrefix.size() + (prefixedPtr != null ? 1 : 0));
    if (prefixedPtr != null) {
      FLeaf leaf = new FLeaf();
      leaf.value = prefixedPtr;
      leaf.setPartialKey(Arrays.copyOfRange(a.get(0), preLen + len, a.get(0).length));
      repNode.add((byte) 0, leaf);
    }
    if (len != 0) {
      repNode.setPartialKey(Arrays.copyOfRange(keys[0], preLen, preLen + len));
    }

    for (CNodeHelper.ValuedPrefixArray vpa : groupedPrefix) {
      repNode.add(
          vpa.bytes[0][len + preLen],
          recNextMergeOnFDM(oriNode, vpa.bytes, len + preLen + 1, ms, mt, height));
    }
    return repNode;
  }
}
