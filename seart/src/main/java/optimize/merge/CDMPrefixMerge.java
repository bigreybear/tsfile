package optimize.merge;

import static optimize.Main.DataSet.BW;
import static optimize.Main.REPORT_CHANNEL;
import static optimize.MergePrefix.getLogicalChild;

import optimize.util.InfixGroup;

import static optimize.MergePrefix.partialNotMerge;
import static optimize.MergePrefix.partialToMerge;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.util.InfixGroup.groupByInfix;
import static optimize.nodes.cdm.CNodeHelper.int2BytesFixedLen;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import optimize.Evaluator;
import optimize.Main;
import optimize.TSTree;
import optimize.nodes.INode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.CNode;
import optimize.nodes.cdm.CNode4;
import optimize.nodes.cdm.CNode4EF;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.logic.LNode;
import optimize.util.ByteArray;
import org.openjdk.jol.info.GraphLayout;

public class CDMPrefixMerge {

  public static void reportMergeStatus() {
    REPORT_CHANNEL.append(
        String.format(
            "CDM node 4 num: %d, CDM-final num: %d \n", evaTrueTime, evaFalseTime));
    REPORT_CHANNEL.append(String.format("Partial merge: %d, not merge: %d \n",
        partialToMerge.get(), partialNotMerge.get()));
  }

  public static INode recNextMergeOnCDM(
      Function<byte[], INode> getLChild,
      byte[][] keys,
      int preLen,
      Evaluator.MergeStrategy ms,
      Evaluator.MapType mt,
      int height,
      boolean withEFCode) {
    if (keys.length == 1) {
      return new CLeaf(keys, preLen, getLChild.apply(keys[0]));
    }

    List<byte[]> byteList = Arrays.asList(keys);

    // for standard edition set CDM width = 4
    // key in group.map is ByteArray, shall align with bytes2Int method
    InfixGroup group = groupByInfix(byteList, 4, preLen);

    if (group.getInfixMap().size() <= 1) {
      throw new RuntimeException("Suffixes should not be identical.");
    }

    boolean useNode4;
    if (ms.equals(Evaluator.MergeStrategy.FULL)) {
      useNode4 = true;
    } else if (ms.equals(Evaluator.MergeStrategy.PARTIAL)) {
      useNode4 = evaluateForNode4(group, preLen, keys, height);

      if (useNode4) partialToMerge.incrementAndGet();
      else partialNotMerge.incrementAndGet();
    } else if (ms.equals(Evaluator.MergeStrategy.SIMPLE)) {
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
        curNode.setPartialKey(Arrays.copyOfRange(keys[0], preLen, group.getBranchingPos()[0]));
      }

      int[] itvPos = findIntervals(group.getBranchingPos());
      int[] sortedBrKeys = group.sortedBrKeys();
      curNode.setBranchingKeys(Arrays.stream(sortedBrKeys).boxed().collect(Collectors.toList()));
      int validBrKeyLen = group.getBranchingPos().length;
      for (int i = 0; i < sortedBrKeys.length; i++) {
        // do not worry about prefixed key: handled by 0x00 key byte
        completeKeys = group.getCompleteKeys(int2BytesFixedLen(sortedBrKeys[i], validBrKeyLen));

        curNode.setInterleavedBytes(i, extractBytes(completeKeys.get(0), itvPos));
        curNode.setBranchingPtr(
            i,
            recNextMergeOnCDM(
                getLChild,
                completeKeys.toArray(new byte[0][0]),
                group.getBranchingPos()[group.getBranchingPos().length - 1] + 1,
                ms,
                mt,
                height,
                withEFCode));
      }
      return curNode;
    } else {
      InfixGroup group1;
      evaFalseTime++;
      // not use final-mapping CNode
      group1 = groupByInfix(byteList, 256, preLen);
      CNode curNode = new CNode(group1.getBranchingPos());
      if (preLen < group1.getBranchingPos()[0]) {
        curNode.setPartialKey(Arrays.copyOfRange(keys[0], preLen, group1.getBranchingPos()[0]));
      }

      byte[][] sortedBrKeys = group1.sortedBrKeyBytes();
      curNode.setBranchingKeysExtended(sortedBrKeys);
      // fixme for CNode, not interleaved but complementary, because no succeeding nodes
      // int [] itvPos = findIntervals(group.getBranchingPos()), curItvPos;
      // int prolongItvPos = 0;
      byte[] sk, ck;
      List<byte[]> ckl;
      for (int i = 0; i < sortedBrKeys.length; i++) {
        sk = sortedBrKeys[i];
        ckl = group1.getInfixMap().get(new ByteArray(sk));
        if (ckl.size() > 1) {
          throw new UnsupportedOperationException(
              "Too long key: " + new String(ckl.get(0), StandardCharsets.UTF_8));
        }
        ck = ckl.get(0);

        int[] cmpPos =
            CNodeHelper.complementaryBytePos(
                group1.getBranchingPos()[0], ck.length, group1.getBranchingPos());
        curNode.setInterleavedBytes(i, extractBytes(ck, cmpPos));
        curNode.setBranchingPtr(i, getLChild.apply(ck));
      }
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

    TSTree tree = Main.buildLogicalTree(BW);
    // root.bw.baoshan.441233M03.`01`.速度.I.hz
    INode t = tree.root.getChild("bw").getChild("baoshan").getChild("441233M03");
    byte[][] keys = CNodeHelper.strings2ByteArrays(t.getKeys());
    INode resEF =
        recNextMergeOnCDM(
            getLogicalChild(t), keys, 0, Evaluator.MergeStrategy.PARTIAL, null, 2, true);
    INode res =
        recNextMergeOnCDM(
            getLogicalChild(t), keys, 0, Evaluator.MergeStrategy.PARTIAL, null, 2, false);

    LNode lt = (LNode) t;

    int num = 0;
    for (Map.Entry<String, INode> entry : lt.children.entrySet()) {
      INode r2 = resEF.getChild(entry.getKey());
      INode r3 = res.getChild(entry.getKey());

      if (r2 == entry.getValue() && r3 == r2) {
        System.out.println("PASS");
        num++;
      } else {
        System.out.println("WRONG");
      }
    }

    long sizeEF = GraphLayout.parseInstance(resEF).totalSize();
    long size = GraphLayout.parseInstance(res).totalSize();
    System.out.println("Space gap: EF-noEF = " + (sizeEF - size));
    System.out.println("FINISH: " + num);
  }
}
