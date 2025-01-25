package optimize.merge;

import static optimize.Main.REPORT_CHANNEL;
import static optimize.MyDataSet.BW;
import static optimize.merge.MergePrefixVDev.partialNotMerge;
import static optimize.merge.MergePrefixVDev.partialToMerge;
import static optimize.util.InfixGroup.groupByInfix;

import java.util.List;
import java.util.function.Function;

import optimize.MainSupport;
import optimize.TSTree;
import optimize.annotation.DebugOnly;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.CNode1F256;
import optimize.nodes.cdm.CNode1F48;
import optimize.nodes.cdm.CNode1FBS;
import optimize.nodes.cdm.SCNode2;
import optimize.nodes.cdm.SCNode8;
import optimize.nodes.cdm.legacyCNode;
import optimize.nodes.cdm.SCNode4;
import optimize.nodes.cdm.ICNode;
import optimize.util.InfixGroup;

public class CDMPrefixMerge {

  @DebugOnly("to inspect merge in procedure")
  public static NodeInspector procInspect = new NodeInspector();

  public static void reportMergeStatus() {
    REPORT_CHANNEL.append(procInspect.toString());
  }

  // Alternating whether CNode1Fx/2/4/8 or only CNode4
  public static IMicroNode recMergeCDM(
      Function<byte[], IMicroNode> getLChild,
      List<byte[]> keys,
      int preLen,
      MapType mapType,
      PrefixMergeStrategy ms,
      int height) {
    if (mapType == MapType.CDM) {
      // legacy impl.
      return recNextMergeOnCDM(
          getLChild,
          keys,
          preLen,
          mapType,
          ms,
          height
      );
    } else if (mapType == MapType.NCDM){
      return recNextMergeOnCDMV2(
          getLChild,
          keys,
          preLen,
          mapType,
          ms,
          height
      );
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private static IMicroNode recNextMergeOnCDMV2(
      Function<byte[], IMicroNode> getLChild,
      List<byte[]> keys,
      int preLen,
      MapType mapType,
      PrefixMergeStrategy ms,
      int height) {

    ICNode node;
    int brcLen, brcNum;
    int[] brcPos;
    InfixGroup group = groupByInfix(keys, preLen);

    // just to simplify code
    Function<ICNode, ICNode> filler = (c -> {
      c.setContent(group, getLChild, mapType, ms, height);
      return c;
    });

    boolean tentative = true;
    while (true) {
      brcPos = group.getBranchingPos();
      brcLen = brcPos.length;
      brcNum = group.countBranches();

      // for decision on brcNum:
      //   too much: revert until CNode1FX
      //   middle: build node
      //   too fewer: find next until CNode8
      if (brcLen == 1) {

        if (brcNum > 48) {
          return filler.apply(new CNode1F256());
        } else if (brcNum > 32) {
          return filler.apply(new CNode1F48());
        } else {
          if (!(tentative && group.findNextBranch())) {
            return filler.apply(new CNode1FBS());
          }
        }

      } else if (brcLen == 2) {

        if (brcNum > 128) {
          tentative = !group.revertSplit();
        } else if (brcNum > 32) {
          return filler.apply(new SCNode2(brcPos));
        } else {
          if (!(tentative && group.findNextBranch())) return filler.apply(new SCNode2(brcPos));
        }

      } else if (brcLen <= 4) {

        if (brcNum > 512) {
          tentative = !group.revertSplit();
        } else if (brcNum > 16) {
          return filler.apply(new SCNode4(brcPos));
        } else {
          if (!(tentative && group.findNextBranch())) return filler.apply(new SCNode4(brcPos));
        }

      } else if (brcLen <= 8) {

        if (brcNum > 512) {
          tentative = !group.revertSplit();
        } else if (brcNum > 32 || brcLen == 8 || !group.findNextBranch()){
          return filler.apply(new SCNode8(brcPos));
        }

      } else {
        System.out.println("SHALL NOT REACH HERE");
        group.revertSplit();
        tentative = false;
      }
    }
  }

  private static IMicroNode recNextMergeOnCDM(
      Function<byte[], IMicroNode> getLChild,
      List<byte[]> keys,
      int preLen,
      MapType mp,
      PrefixMergeStrategy ms,
      int height) {
    if (keys.size() == 1) {
      // actually this will never be executed
      System.out.println("SHALL NOT be executed.");
      return new CLeaf(keys, preLen, (ICNode) getLChild.apply(keys.get(0)));
    }

    // for standard edition set CDM width = 4
    // key in group.map is ByteArray, shall align with bytes2Int method
    InfixGroup group = groupByInfix(keys, 4, preLen);

    if (group.getInfixMap().size() <= 1) {
      throw new RuntimeException("Should not have duplicate keys.");
    }

    if (ms == null) {
      // todo transform to CNode, serving as full
      return null;
    }

    boolean useNode4;
    if (ms.equals(PrefixMergeStrategy.FULL)) {
      useNode4 = true;
    } else if (ms.equals(PrefixMergeStrategy.PARTIAL)) {
      useNode4 = evaluateForNode4(group, preLen, keys, height);

      if (useNode4) partialToMerge.incrementAndGet();
      else partialNotMerge.incrementAndGet();
    } else if (ms.equals(PrefixMergeStrategy.SIMPLE)) {
      useNode4 = false;
    } else {
      throw new RuntimeException("MERGE STRATEGY ERROR");
    }

    if (useNode4) {
      List<byte[]> completeKeys;
      evaTrueTime++;
      ICNode curNode = new SCNode4(group.getBranchingPos());
//      if (preLen < group.getBranchingPos()[0]) {
//        curNode.setParKey(Arrays.copyOfRange(keys.get(0), preLen, group.getBranchingPos()[0]));
//      }

      curNode.setContent(group, getLChild, mp, ms, height);
      return curNode;
    } else {
      InfixGroup group1;
      evaFalseTime++;
      // not use final-mapping CNode
      group1 = groupByInfix(keys, 256, preLen);
      legacyCNode curNode = new legacyCNode(group1.getBranchingPos());
//      if (preLen < group1.getBranchingPos()[0]) {
//        curNode.setParKey(Arrays.copyOfRange(keys.get(0), preLen, group1.getBranchingPos()[0]));
//      }

      curNode.setContent(group1, getLChild, mp, ms, height);
      return curNode;
    }
  }

  public static int evaTrueTime = 0, evaFalseTime = 0;

  public static boolean evaluateForNode4(InfixGroup g, int preLen, List<byte[]> keys, int height) {
    float gamma = 0.5f;
    int keyNum = g.getInfixMap().values().stream().mapToInt(List::size).sum();
    int[] posArr = g.getBranchingPos();
    int shareLen = posArr[posArr.length - 1] - posArr[0] + 1;
    int savingBySharing = shareLen * keyNum;

    final int pkLen = posArr[0];
    int ptrSpace = 8 * g.getInfixMap().size(), desKeyLen = 0;
    double detTime = 0.0d;
    for (List<byte[]> lb : g.getInfixMap().values()) {
      // desKeyLen += lb.stream().mapToInt(i -> i.length - pkLen).sum();
      detTime += (lb.size() * 1.0d / keyNum) * (Math.log(lb.size())) / (Math.log(2));
    }

    int deltaSpace = ptrSpace + desKeyLen - savingBySharing;
    // return false; // fixme debug CNode
    return gamma * deltaSpace + (1 - gamma) * detTime * 13 < 0;
  }

  public static void main(String[] args) {
    // Main.main("-mt cdm -ms partial -ds bw -merge -latency".split(" "));

    TSTree tree = MainSupport.buildLogicalTree(BW, true);
    // root.bw.baoshan.441233M03.`01`.速度.I.hz
    // ITSNode t = tree.root
    //     .getLogicalChild("bw")
    //     .getLogicalChild("baoshan")
    //     .getLogicalChild("441233M03");
    // byte[][] keys = CNodeHelper.strings2ByteArrays(t.getStringKeys());
    // ITSNode resEF =
    //     recNextMergeOnCDM(getLogicalChildVDev(t), keys, 0, PrefixMergeStrategy.PARTIAL, null, 2,
    // true);
    // ITSNode res =
    //     recNextMergeOnCDM(getLogicalChildVDev(t), keys, 0, PrefixMergeStrategy.PARTIAL, null, 2,
    // false);
    //
    // LLeafVDev lt = (LLeafVDev) t;
    //
    // int num = 0;
    // for (Map.Entry<String, INode> entry : lt.entrySet()) {
    //   ITSNode r2 = resEF.getLogicalChild(entry.getKey());
    //   ITSNode r3 = res.getLogicalChild(entry.getKey());
    //
    //   if (r2 == entry.getValue() && r3 == r2) {
    //     System.out.println("PASS");
    //     num++;
    //   } else {
    //     System.out.println("WRONG");
    //   }
    // }
    //
    // long sizeEF = GraphLayout.parseInstance(resEF).totalSize();
    // long size = GraphLayout.parseInstance(res).totalSize();
    // System.out.println("Space gap: EF-noEF = " + (sizeEF - size));
    // System.out.println("FINISH: " + num);
  }
}
