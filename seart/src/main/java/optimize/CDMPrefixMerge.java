package optimize;

import optimize.eliasfano.EliasFano;
import optimize.nodes.INode;
import optimize.nodes.cdm.CNode;
import optimize.nodes.cdm.CNode4;
import optimize.nodes.cdm.CNode4EF;
import optimize.nodes.cdm.CNodeHelper;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static optimize.Main.DataSet.BW;
import static optimize.nodes.cdm.CNodeHelper.bytes2Int;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.nodes.cdm.CNodeHelper.getBranchingPosParallel;
import static optimize.nodes.cdm.CNodeHelper.parallelExtBytes;
import static optimize.nodes.cdm.CNodeHelper.parallelGetBranchingPositions;

public class CDMPrefixMerge {

  public static INode recNextMergeOnCDM(INode oriNode, byte[][] keys, int preLen,
                                        Evaluator.MergeStrategy ms, Evaluator.MapType mt, int height) {
    System.out.println("CDM");
    // 单个的为直接 partial key
    // 取 4 个 pos, value 分别每个递归
    //

    List<byte[]> byteList = Arrays.asList(keys);
    final Set<Integer> pos = getBranchingPosParallel(byteList, 4);
    final int[] posArray = pos.stream().mapToInt(i -> i).toArray();

    Map<Integer, List<byte[]>> groups = byteList
        .parallelStream()
        .collect(Collectors.groupingByConcurrent(
        ba -> bytes2Int(extractBytes(ba, posArray))));
    List<Integer> sortedBrnBytes = groups.keySet().stream().sorted().collect(Collectors.toList());

    CNode4EF cNode4EF = new CNode4EF();
    cNode4EF.setBranchingKeys(sortedBrnBytes);

    return null;
  }

  public static void main(String[] args) {
    // Main.main("-mt cdm -ms partial -ds bw -merge -latency".split(" "));

    TSTree tree = Main.buildLogicalTree(BW);
    INode t = tree.root.getChild("bw").getChild("baoshan");
    byte[][] keys = CNodeHelper.strings2ByteArrays(t.getKeys());
    INode res = recNextMergeOnCDM(t, keys, 0, null, null, 2);

    System.out.println("FINISH");
  }
}
