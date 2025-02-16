package optimize.merge;

import optimize.MainSupport;
import optimize.MyDataSet;
import optimize.TSTree;
import optimize.merge.bum.BottomUpMergeStrategy;
import optimize.merge.bum.PullUpSharedPosition;
import optimize.merge.skeleton.BNode16;
import optimize.merge.skeleton.BNode256;
import optimize.merge.skeleton.BNode4;
import optimize.merge.skeleton.BNode48;
import optimize.merge.skeleton.MiniTreeRep;
import optimize.merge.skeleton.PartitionInfo;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static optimize.nodes.cdm.ByteEncode.strings2ByteArrays;
import static optimize.nodes.cdm.CNodeHelper.findLCPLength;
import static optimize.nodes.cdm.CNodeHelper.groupPrefixes;

/**
 * P1: Transform to FDM/ART, and merge bottom-up, making each mini-tree height less than 8; <br>
 * P2: Transform to CDM, and build each CDM node with the most suitable span according to fanout. <br>
 */
public class TwoPhasePrefixMerge {

  private static final BottomUpMergeStrategy mergeStrategy = new PullUpSharedPosition();

  public static void main(String[] args) {
    TSTree tree = MainSupport.buildLogicalTree(MyDataSet.BW, true);
    startMerge(tree);
    System.out.println("STOP HERE");
  }

  public static void startMerge(TSTree tree) {
    transformToART(tree);
    // first phase
    traverseAndMarkRecursive(tree.root, 0, 0);
    // todo 0216
    // phase-m: transform to mini-hash map
    MiniTreeRep rep = MiniTreeRep.transform(tree.root);
    System.out.println("HERE");
    // second phase: bottom-up merge mini-trees while keep the merged of height less than 8
    // third phase: transform each mini tree to one or more CNodes
  }

  private static void combineToCNodes(TSTree tree) {
    // if hyperLevel == 0: transform immediately
    // otherwise search each miniRoot and recur
    // finally transform the current node
  }

  private static void traverseAndMarkRecursive(ITSNode node, int depth, int offset) {
    int parKeyLen = node.getParKey() == null ? 0 : node.getParKey().length;
    List<ITSNode> children = node.getPhysicalChildren();

    PartitionInfo info = node.getInfoObj();
    info.brPos = offset + parKeyLen;
    info.dep = depth;
    info.brPosSet.add(info.brPos);

    ITSNode chd;
    for (int i = 0, len = children.size(); i < len; i++) {
      if ((chd = children.get(i)).isLogicalLeaf()) { // skip all leaves
        continue;
      }

      traverseAndMarkRecursive(
          chd,
          depth + 1,
          offset + 1 + parKeyLen);
    }

    // all children had been visited
    // switch between different strategies.
    mergeStrategy.mergeAndUpdateInfo(node, info, children);
  }

  private static Set<Integer> mergeConnectedBrPosSet(List<PartitionInfo> chdInfoLst) {
    Set<Integer> res = new TreeSet<>();
    for (PartitionInfo info : chdInfoLst) {
      if (!info.isMiniRoot) {
        res.addAll(info.brPosSet);
      }
    }
    return res;
  }


  private static void transformToART(TSTree tree) {
    tree.traversePostOrderRec(
        (par, key, cur, stk) -> {
          List<String> keyList = null;
          if ((keyList = cur.getStringKeys()) == null) {
            return;
          }
          byte[][] keyBytes = strings2ByteArrays(keyList);

          IFNode n2 = recNextMergeOnFDM(cur, keyBytes, 0);

          if (n2 != cur) {
            if (par == null) {
              tree.root = n2;
            } else {
              par.replace(key, n2);
            }
          }
        });
  }

  private static IFNode generateBNode(int need) {
    if (need <= 4) return new BNode4();
    else if (need <= 16) return new BNode16();
    else if (need <= 48) return new BNode48();
    else return new BNode256();
  }

  private static IFNode recNextMergeOnFDM(
      ITSNode oriNode, byte[][] keys, int preLen) {

    if (keys.length == 1) {
      return FLeaf.constructAnnotatedLLeaf(
          (IFNode) oriNode.getLogicalChild(new String(keys[0], StandardCharsets.UTF_8)),
          Arrays.copyOfRange(keys[0], preLen, keys[0].length),
          keys[0]);
    }

    // get the ptr of 0
    final int len = findLCPLength(keys, preLen);
    // find the key exactly IS the common prefix
    IFNode prefixedPtr = null;
    List<byte[]> a =
        Arrays.stream(keys).filter(e -> e.length == len + preLen).toList();
    if (!a.isEmpty()) {
      prefixedPtr = (IFNode) oriNode.getLogicalChild(new String(a.get(0), StandardCharsets.UTF_8));
    }

    // the prefixed key not EXCLUDED
    List<CNodeHelper.ValuedPrefixArray> groupedPrefix = groupPrefixes(keys, len + preLen, 1);
    IFNode repNode = generateBNode(groupedPrefix.size() + (prefixedPtr != null ? 1 : 0));
    if (prefixedPtr != null) {
      repNode.add(
          (byte) 0,
          FLeaf.constructAnnotatedLLeaf(
              prefixedPtr,
              Arrays.copyOfRange(a.get(0), preLen + len, a.get(0).length),
              a.get(0)));
    }
    if (len != 0) {
      repNode.setParKey(Arrays.copyOfRange(keys[0], preLen, preLen + len));
    }

    for (CNodeHelper.ValuedPrefixArray vpa : groupedPrefix) {
      repNode.add(
          vpa.bytes[0][len + preLen],
          recNextMergeOnFDM(oriNode, vpa.bytes, len + preLen + 1));
    }
    return repNode;
  }

}
