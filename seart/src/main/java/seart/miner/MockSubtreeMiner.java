package seart.miner;

import seart.ISEARTNode;
import seart.Leaf;
import seart.RefNode;
import seart.SEARTree;
import seart.traversal.DFSTraversal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class MockSubtreeMiner {

  // condition affiliations
  static Set<String> replacedPaths = new HashSet<>();
  static int templateBranches = 0;
  static Map<String, Integer> templatePaths = new HashMap<>();

  private static byte[] concatenateByteArrays(byte[] ...arrays) {
    int totalLength = 0;
    for (byte[] array : arrays) {
      totalLength += array.length;
    }
    byte[] result = new byte[totalLength];
    int currentIndex = 0;
    for (byte[] array : arrays) {
      System.arraycopy(array, 0, result, currentIndex, array.length);
      currentIndex += array.length;
    }
    return result;
  }

  /**
   * construct RefNode for target leaves
   * ref.pk = oLeaf.pk + tr.root.pk; REMOVE pk from tr.root at the end of replacement
   * @return @NotNull if curNode is leaf AND ought to be replaced
   **/
  private static ISEARTNode recursionV1(
      ISEARTNode curNode,
      ISEARTNode tr,
      Deque<byte[]> pstack,
      BiFunction<ISEARTNode, byte[], Boolean> condition) {
    Deque<byte[]> stack = pstack == null ? new ArrayDeque<>() : pstack;

    if (curNode.isLeaf()) {
      if (curNode instanceof RefNode) {
        throw new RuntimeException("Shall not replace twice");
      }
      // Note(zx) only transform to String when bytes are all collected AS IS before insertion
      // if the result of concatenation is incomplete (comparing to the original), the string must be wrong
      byte[] curPathBytes = concatenateByteArrays(
          concatenateByteArrays(pstack.toArray(new byte[0][0])),
          curNode.getPartialKey());

      // embedded condition to decide whether to replace
      if (condition.apply(curNode, curPathBytes)) {
        // System.out.println("Enable template on:" + curPath);
        RefNode rn = new RefNode();

        // the separating dot is added here
        byte[] pk = new byte[curNode.getPartialKey().length + tr.getPartialKey().length + 1];
        System.arraycopy(curNode.getPartialKey(), 0, pk, 0, curNode.getPartialKey().length);
        pk[curNode.getPartialKey().length] = (byte) '.';
        System.arraycopy(tr.getPartialKey(), 0, pk, curNode.getPartialKey().length + 1, tr.getPartialKey().length);
        rn.reassignPartialKey(pk);

        // todo not robust here since curPathBytes may not be character-complete
        long[] vals = new long[templateBranches];
        for (Map.Entry<String, Integer> entry : templatePaths.entrySet()) {
          vals[entry.getValue()] = new String(
              concatenateByteArrays(curPathBytes, new byte[] {46}, entry.getKey().getBytes(StandardCharsets.UTF_8)),
              StandardCharsets.UTF_8).hashCode();
        }
        rn.setValues(vals);
        rn.setTemplateRoot(tr);
        return rn;
      }
      return null;
    }


    stack.addLast(curNode.getPartialKey() == null ? new byte[0] : curNode.getPartialKey());
    for (byte b : curNode.getKeys()) {
      stack.addLast(new byte[] {b});
      ISEARTNode res = recursionV1(curNode.getChildByKeyByte(b), tr, stack, condition);
      if (res != null) {
        curNode.setChildPtrByIndex(curNode.getPtrIdxByByte(b), res);
      }
      stack.removeLast();
    }
    stack.removeLast();
    return null;
  }

  // iterate template and count branches
  public static void replaceV1(ISEARTNode context, ISEARTNode template,
                               BiFunction<ISEARTNode, byte[], Boolean> condition) {

    DFSTraversal dfsTraversal = new DFSTraversal(template);
    while (dfsTraversal.hasNext()) {
      ISEARTNode node = dfsTraversal.next();
      if (node.isLeaf()) {
        ((Leaf)node).setValue(templateBranches);
        templatePaths.put(dfsTraversal.getCurrentPath(), templateBranches);
        templateBranches++;
      }
    }

    recursionV1(context, template, null, condition);
    template.reassignPartialKey(null);
  }

  public static void main(String[] args) {
    String a = "root.bw.baoshan.九位码待补充001lt.01.外接电源电压";
    byte[] b = a.getBytes(StandardCharsets.UTF_8);
    String c = new String(b, StandardCharsets.UTF_8);
    System.out.println(a.equals(c));
  }

  public static void main2(String[] args) {
    SEARTree context = new SEARTree();
    SEARTree template = new SEARTree();

    template.insert("speed", 0L);
    template.insert("temperature", 0L);
    template.insert("I.serve", 0L);
    template.insert("current", 0L);
    template.insert("humidity", 0L);

    String[] keys = {
        "root.bw.baoshan.072029E51.05.低频加速度有效值",
        "root.bw.baoshan.323536M03009.01.高频加速度有效值",
        "root.bw.baoshan.840643M02D10.02.轴向冲击平均值",
        "root.bw.baoshan.640456M01.01.高频加速度峭度",
        "root.bw.baoshan.九位码待补充001lt.01.外接电源电压"
    };

    for (String s : keys) {
      context.insert(s, s.hashCode());
    }

    replaceV1(context.root, template.root, (a, b) -> {
      System.out.println(b);
      return true;
    });
    DFSTraversal dfsTraversal = new DFSTraversal(context.root);
    dfsTraversal.printAllPaths();

    DFSTraversal.consumeNodes(context.root, (node, path) -> {
      Map<Integer, List<String>> tpltPath = new HashMap<>();
      if (node.isLeaf()) {
        if (node instanceof RefNode) {
          List<String> sl = tpltPath.computeIfAbsent(
              ((RefNode)node).templateRoot.hashCode(),
              (a) -> DFSTraversal.getAllPathsWithoutTemplate(((RefNode)node).templateRoot));
          for (int i = 0; i < sl.size(); i++) {
            System.out.println(String.format("<%s, %d>, %d",
                path + sl.get(i), ((RefNode)node).values[i], (path+sl.get(i)).hashCode()));
          }
        }
      }
    });

    System.out.println(context.search("root.sg1.v1.d2.speed"));
  }

}
