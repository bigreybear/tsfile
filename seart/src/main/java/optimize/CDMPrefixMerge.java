package optimize;

import optimize.nodes.INode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.CNode4EF;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.cdm.ICNode;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static optimize.Main.DataSet.BW;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.findIntervals;
import static optimize.nodes.cdm.CNodeHelper.getBranchingPosParallel;
import static optimize.nodes.cdm.CNodeHelper.groupByInfix;
import static optimize.nodes.cdm.CNodeHelper.int2BytesFixedLen;
import static optimize.nodes.cdm.CNodeHelper.parallelGetBranchingPositions;
import static optimize.MergePrefix.getLogicalChild;
import static optimize.nodes.cdm.CNodeHelper.InfixGroup;

public class CDMPrefixMerge {

  public static INode recNextMergeOnCDM(Function<byte[], INode> getLChild, byte[][] keys, int preLen,
                                        Evaluator.MergeStrategy ms, Evaluator.MapType mt, int height) {
    if (keys.length == 1) {
      return new CLeaf(keys, preLen, getLChild.apply(keys[0]));
    }

    List<byte[]> byteList = Arrays.asList(keys);
    // for standard edition set CDM width = 4

    // key in group.map is ByteArray, shall align with bytes2Int method
    InfixGroup group = groupByInfix(byteList, 4, preLen);
    CNode4EF curNode = new CNode4EF(group.getBranchingPos());
    if (preLen < group.getBranchingPos()[0]) {
      curNode.setPartialKey(Arrays.copyOfRange(keys[0], preLen, group.getBranchingPos()[0]));
    }

    // System.out.println(ClassLayout.parseInstance(curNode).toPrintable());

    int[] itvPos = findIntervals(group.getBranchingPos());
    int[] sortedBrKeys = group.sortedBrKeys();
    List<byte[]> completeKeys;
    if (sortedBrKeys.length > 1 /* && evaluate()*/ ) {
      curNode.setBranchingKeys(Arrays.stream(sortedBrKeys).boxed().collect(Collectors.toList()));
      int validBrKeyLen = group.getBranchingPos().length;
      for (int i = 0; i < sortedBrKeys.length; i++) {
        // do not worry about prefixed key: handled by 0x00 key byte
        completeKeys = group.getCompleteKeys(int2BytesFixedLen(sortedBrKeys[i], validBrKeyLen));

        // todo debug
        if ((sortedBrKeys[i] & 0xff000000) == 0) {
          System.out.println("HHH");
        }


        curNode.setInterleavedBytes(i, extractBytes(completeKeys.get(0), itvPos));
        curNode.setBranchingPtr(
            i,
            recNextMergeOnCDM(
                getLChild,
                completeKeys.toArray(new byte[0][0]),
                group.getBranchingPos()[group.getBranchingPos().length-1] + 1,
                ms, mt, height
            ));
      }
    } else {
      throw new RuntimeException("Suffixes should not be identical.");
    }

    curNode.assembleKeyAt(0);
    return curNode;
    // replaced with new func
    // final Set<Integer> pos = getBranchingPosParallel(byteList, 4);
    // final int[] posArray = pos.stream().mapToInt(i -> i).toArray();
    //
    // Map<Integer, List<byte[]>> groups = byteList
    //     .parallelStream()
    //     .collect(Collectors.groupingByConcurrent(
    //     ba -> bytes2Int(extractBytes(ba, posArray))));
    // List<Integer> sortedBrnBytes = groups.keySet().stream().sorted().collect(Collectors.toList());
    //
    // CNode4EF cNode4EF = new CNode4EF();
    // cNode4EF.setBranchingKeys(sortedBrnBytes);

    // if any suffix of extracted bytes are 0, it must point to a leaf/LNode

  }

  public static void main(String[] args) {
    // Main.main("-mt cdm -ms partial -ds bw -merge -latency".split(" "));

    TSTree tree = Main.buildLogicalTree(BW);
    INode t = tree.root.getChild("bw").getChild("baoshan");
    byte[][] keys = CNodeHelper.strings2ByteArrays(t.getKeys());
    INode res = recNextMergeOnCDM(getLogicalChild(t), keys, 0, null, null, 2);

    System.out.println("FINISH");
    System.out.println(new String(((ICNode)res).assembleKeyAt(6), StandardCharsets.UTF_8));
  }
}
