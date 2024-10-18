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


  /**
   * construct RefNode for target leaves
   * ref.pk = oLeaf.pk + tr.root.pk; REMOVE pk from tr.root at the end of replacement
   * @return @NotNull if curNode is leaf AND ought to be replaced
   **/
  private static ISEARTNode recursionV1(
      ISEARTNode curNode,
      ISEARTNode tr,
      Deque<String> pstack,
      BiFunction<ISEARTNode, Deque<String>, Boolean> condition) {
    Deque<String> stack = pstack == null ? new ArrayDeque<>() : pstack;

    if (curNode.isLeaf()) {
      String curPath = String.join("", pstack) + (curNode.getPartialKey() == null ? "" : new String(curNode.getPartialKey(), StandardCharsets.UTF_8));
      if (condition.apply(curNode, stack)) {
        System.out.println("Enable template on:" + curPath);
        RefNode rn = new RefNode();

        // the separating dot is added here
        byte[] pk = new byte[curNode.getPartialKey().length + tr.getPartialKey().length + 1];
        System.arraycopy(curNode.getPartialKey(), 0, pk, 0, curNode.getPartialKey().length);
        pk[curNode.getPartialKey().length] = (byte) '.';
        System.arraycopy(tr.getPartialKey(), 0, pk, curNode.getPartialKey().length + 1, tr.getPartialKey().length);
        rn.reassignPartialKey(pk);

        long[] vals = new long[templateBranches];
        for (Map.Entry<String, Integer> entry : templatePaths.entrySet()) {
          vals[entry.getValue()] = (curPath + '.' + entry.getKey()).hashCode();
        }
        rn.setValues(vals);
        rn.setTemplateRoot(tr);
        return rn;
      }
      return null;
    }


    stack.addLast(new String(curNode.getPartialKey(), StandardCharsets.UTF_8));
    for (byte b : curNode.getKeys()) {
      stack.addLast(String.valueOf((char) b));

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
                               BiFunction<ISEARTNode, Deque<String>, Boolean> condition) {

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
    SEARTree context = new SEARTree();
    SEARTree template = new SEARTree();

    template.insert("speed", 0L);
    template.insert("temperature", 0L);
    template.insert("I.serve", 0L);
    template.insert("current", 0L);
    template.insert("humidity", 0L);

    String[] keys = {
        "root.sg1.v1.d1",
        "root.sg1.v1.d2",
        "root.sg1.v1.d3"
    };

    for (String s : keys) {
      context.insert(s, s.hashCode());
    }

    replaceV1(context.root, template.root, (a, b) -> (String.join("", b).hashCode() & 0x01) == 0);
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
