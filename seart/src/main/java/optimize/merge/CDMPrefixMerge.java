package optimize.merge;

import static optimize.MainVDev.REPORT_CHANNEL;
import static optimize.MyDataSet.BW;
import static optimize.merge.MergePrefixVDev.getLogicalChildVDev;
import static optimize.merge.MergePrefixVDev.partialNotMerge;
import static optimize.merge.MergePrefixVDev.partialToMerge;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.nodes.cdm.CNodeHelper.int2BytesFixedLen;
import static optimize.util.InfixGroup.groupByInfix;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import optimize.MainVDev;
import optimize.TSTreeVDev;
import optimize.nodes.IMicroNode;
import optimize.nodes.INode;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.CNode;
import optimize.nodes.cdm.CNode4;
import optimize.nodes.cdm.CNode4EF;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.logic.LLeafVDev;
import optimize.util.InfixGroup;
import org.openjdk.jol.info.GraphLayout;

public class CDMPrefixMerge {

  public static void reportMergeStatus() {
    REPORT_CHANNEL.append(
        String.format("CDM node 4 num: %d, CDM-final num: %d \n", evaTrueTime, evaFalseTime));
    REPORT_CHANNEL.append(
        String.format(
            "Partial merge: %d, not merge: %d \n", partialToMerge.get(), partialNotMerge.get()));
  }

  public static IMicroNode recNextMergeOnCDM(
      Function<byte[], IMicroNode> getLChild,
      byte[][] keys,
      int preLen,
      PrefixMergeStrategy ms,
      MapType mt,
      int height,
      boolean withEFCode) {
    if (keys.length == 1) {
      return new CLeaf(keys, preLen, (ICNode) getLChild.apply(keys[0]));
    }

    List<byte[]> byteList = Arrays.asList(keys);

    // for standard edition set CDM width = 4
    // key in group.map is ByteArray, shall align with bytes2Int method
    InfixGroup group = groupByInfix(byteList, 4, preLen);

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
      // CNode4EF curNode = new CNode4EF(group.getBranchingPos());
      ICNode curNode =
          withEFCode ? new CNode4EF(group.getBranchingPos()) : new CNode4(group.getBranchingPos());
      if (preLen < group.getBranchingPos()[0]) {
        curNode.setParKey(Arrays.copyOfRange(keys[0], preLen, group.getBranchingPos()[0]));
      }

      // int[] itvPos = findIntervals(group.getBranchingPos());
      // int[] sortedBrKeys = group.sortedBrKeys();
      curNode.setContent(group, getLChild, ms, mt, height, withEFCode);
      // int validBrKeyLen = group.getBranchingPos().length;
      // for (int i = 0; i < sortedBrKeys.length; i++) {
      //   // do not worry about prefixed key: handled by 0x00 key byte
      //   completeKeys = group.getCompleteKeys(int2BytesFixedLen(sortedBrKeys[i], validBrKeyLen));
      //
      //   curNode.setInterleavedBytes(i, extractBytes(completeKeys.get(0), itvPos));
      //   curNode.setBranchingPtr(
      //       i,
      //       recNextMergeOnCDM(
      //           getLChild,
      //           completeKeys.toArray(new byte[0][0]),
      //           group.getBranchingPos()[group.getBranchingPos().length - 1] + 1,
      //           ms,
      //           mt,
      //           height,
      //           withEFCode));
      // }
      return curNode;
    } else {
      InfixGroup group1;
      evaFalseTime++;
      // not use final-mapping CNode
      group1 = groupByInfix(byteList, 256, preLen);
      CNode curNode = new CNode(group1.getBranchingPos());
      if (preLen < group1.getBranchingPos()[0]) {
        curNode.setParKey(Arrays.copyOfRange(keys[0], preLen, group1.getBranchingPos()[0]));
      }
      curNode.setContent(group1, getLChild, ms, mt, height, withEFCode);

      // byte[][] sortedBrKeys = group1.sortedBrKeyBytes();
      // curNode.setBranchingKeysExtended(sortedBrKeys);
      // fixme for CNode, not interleaved but complementary, because no succeeding nodes
      // int [] itvPos = findIntervals(group.getBranchingPos()), curItvPos;
      // int prolongItvPos = 0;
      // byte[] sk, ck;
      // List<byte[]> ckl;
      // for (int i = 0; i < sortedBrKeys.length; i++) {
      //   sk = sortedBrKeys[i];
      //   ckl = group1.getInfixMap().get(new ByteArray(sk));
      //   if (ckl.size() > 1) {
      //     throw new UnsupportedOperationException(
      //         "Too long key: " + new String(ckl.get(0), StandardCharsets.UTF_8));
      //   }
      //   ck = ckl.get(0);
      //
      //   int[] cmpPos =
      //       CNodeHelper.complementaryBytePos(
      //           group1.getBranchingPos()[0], ck.length, group1.getBranchingPos());
      //   curNode.setInterleavedBytes(i, extractBytes(ck, cmpPos));
      //   curNode.setBranchingPtr(i, getLChild.apply(ck));
      // }
      return curNode;
    }
  }

  public static int evaTrueTime = 0, evaFalseTime = 0;

  public static boolean evaluateForNode4(InfixGroup g, int preLen, byte[][] keys, int height) {
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

    TSTreeVDev tree = MainVDev.buildLogicalTree(BW);
    // root.bw.baoshan.441233M03.`01`.速度.I.hz
    // ITSNode t = tree.root
    //     .getLogicalChild("bw")
    //     .getLogicalChild("baoshan")
    //     .getLogicalChild("441233M03");
    // byte[][] keys = CNodeHelper.strings2ByteArrays(t.getStringKeys());
    // ITSNode resEF =
    //     recNextMergeOnCDM(getLogicalChildVDev(t), keys, 0, PrefixMergeStrategy.PARTIAL, null, 2, true);
    // ITSNode res =
    //     recNextMergeOnCDM(getLogicalChildVDev(t), keys, 0, PrefixMergeStrategy.PARTIAL, null, 2, false);
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
