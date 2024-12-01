package optimize;

import optimize.nodes.INode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.FNode16;
import optimize.nodes.fdm.FNode256;
import optimize.nodes.fdm.FNode48;
import optimize.nodes.fdm.FNode4;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.fdm.VirtualFNode;
import optimize.nodes.hash.HNode;
import optimize.nodes.hash.PrefixedHNode;
import optimize.nodes.logic.LLeaf;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static optimize.CDMPrefixMerge.recNextMergeOnCDM;
import static optimize.Evaluator.MergeStrategy;
import static optimize.Evaluator.MapType;
import static optimize.Evaluator.calcSpace;
import static optimize.nodes.cdm.CNodeHelper.findLCPLength;
import static optimize.nodes.cdm.CNodeHelper.groupPrefixes;
import static optimize.nodes.cdm.CNodeHelper.strings2ByteArrays;

import static optimize.nodes.cdm.CNodeHelper.ValuedPrefixArray;

public class MergePrefix {
  public static AtomicInteger
      occ = new AtomicInteger(),
      ttlLen = new AtomicInteger(),
      inc = new AtomicInteger();
  public static void reportMergeStatus() {
    System.out.println(String.format("Occ: %d, total len: %d, inc: %d",
        occ.get(), ttlLen.get(), inc.get()));
  }

  public static INode initNodeWithPartialKey(byte[] key, int preLen, int len, MapType mapType,
                                             INode prefixedChild,
                                             int branchingNum) {
    switch (mapType) {
      case HASH: {
        if (prefixedChild == null) {
          HNode res = new HNode();
          res.pk = len == 0 ? null : Arrays.copyOfRange(key, preLen, len + preLen);
          return res;
        }
        PrefixedHNode res = new PrefixedHNode();
        res.pk = Arrays.copyOfRange(key, preLen, len + preLen);
        res.prePtr = prefixedChild;
        return res;
      }
      case FDM: {
        return null;
      }
    }

    return null;
  }

  public static IFNode generateFNode(int need) {
    if (need <=4) return new FNode4();
    else if (need <=16) return new FNode16();
    else if (need <=48) return new FNode48();
    else return new FNode256();
  }

  // todo merge with hash ones
  public static INode recNextMergeOnFDM(INode oriNode, byte[][] keys, int preLen,
                                   MergeStrategy ms, MapType mt, int height) {
    if (ms.equals(MergeStrategy.SIMPLE) || ms.equals(MergeStrategy.PARTIAL)) {
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
    List<byte[]> a = Arrays.stream(keys).filter(e -> e.length == len + preLen)
        .collect(Collectors.toList());
    if (!a.isEmpty()) {
      prefixedPtr = oriNode.getChild(new String(a.get(0), StandardCharsets.UTF_8));
    }

    // the prefixed key not EXCLUDED
    List<ValuedPrefixArray> groupedPrefix = groupPrefixes(keys, len + preLen, 1);
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

    for (ValuedPrefixArray vpa : groupedPrefix) {
      repNode.add(vpa.bytes[0][len+preLen], recNextMergeOnFDM(
          oriNode, vpa.bytes, len + preLen + 1, ms, mt, height
      ));
    }
    return repNode;
  }

  /**
   * PRIMARY; enter with preLen=0
   * @param oriNode the logical node, containing the original key-node mapping
   * @param preLen entry for 0
   * @param height the logical distance from the whole root
   */
  public static INode recNextMergeOnHash(INode oriNode, byte[][] keys, int preLen,
                                         MergeStrategy ms, MapType mt, int height) {

    final int len = findLCPLength(keys, preLen);

    if (len == 0) {
      // simple only extract direct common prefix which is none here
      if (ms.equals(MergeStrategy.SIMPLE)) return oriNode;
      // where partial and all strategy diff from simple

      // todo remove debug
      // System.out.println("DIFF");
    }

    // find the key exactly IS the common prefix
    INode preNode = null;
    if (preLen + len != 0) {
      List<byte[]> a = Arrays.stream(keys).filter(e -> e.length == len + preLen)
          .collect(Collectors.toList());
      if (!a.isEmpty()) {
        preNode = oriNode.getChild(new String(a.get(0), StandardCharsets.UTF_8));
      }
    }

    // the node is now merging
    // todo how thread knows the key if there is a partial merge, for hash?
    //  the ans is, query twice,
    //  first for the remaining, then for first byte
    //  Anyway to improve?
    // the node must replace the original one
    final INode repNode = initNodeWithPartialKey(keys[0], preLen, len, mt, preNode, 0);

    if (ms.equals(MergeStrategy.SIMPLE)) {
      // all keys longer than prefix will be added AS IS
      List<byte[]> longerKeys = Arrays.stream(keys).filter(e -> e.length > len + preLen).collect(Collectors.toList());
      for (byte[] nk : longerKeys) {
        repNode.addChild(new String(Arrays.copyOfRange(nk, preLen + len, nk.length), StandardCharsets.ISO_8859_1),
            oriNode.getChild(new String(nk, StandardCharsets.UTF_8)));
      }
      occ.incrementAndGet();
      ttlLen.addAndGet(longerKeys.size() * len);
      return repNode;
    }

    // for partial or all, recursively group keys after prefix and merge again
    List<ValuedPrefixArray> groupedPrefix = groupPrefixes(keys, len + preLen, 1);
    boolean eva;
    for (ValuedPrefixArray vpa : groupedPrefix) {
      if (( (eva = Evaluator.evaluateMerge(vpa, preLen, height, keys.length, mt))
          || ms.equals(MergeStrategy.FULL)) && vpa.bytes.length > 1) {
        // when to execute: Full merge or evaluated worthy, and shared by more than ONE key

        occ.incrementAndGet();
        ttlLen.addAndGet(vpa.prd);
        final INode recNode = recNextMergeOnHash(oriNode, vpa.bytes, len + preLen + 1,
            ms, mt, height);
        // add the node generated in rec to the current node
        repNode.addChild(
            new String(
                // index by first byte after common current prefix (len+preLen)
                Arrays.copyOfRange(vpa.bytes[0], len + preLen, len + preLen + 1 /* previously: vpa.len, but would dup with the pk on descendant*/ ),
                StandardCharsets.ISO_8859_1),
            recNode
        );
      } else {
        // go-through to the no-branching child
        for (byte[] k : vpa.bytes) {
          repNode.addChild(
              new String(Arrays.copyOfRange(k, preLen + len, k.length), StandardCharsets.ISO_8859_1),
              oriNode.getChild(new String(k, StandardCharsets.UTF_8))
          );
        }
      }
    }
    return repNode;
  }

   // public static void testRecNextMerge(String[] args) {
  public static void main(String[] args) {
    HNode n1 = new HNode();
    byte[][] keys = new byte[][] {
        "aaabcg".getBytes(StandardCharsets.UTF_8),
        "aaabc".getBytes(StandardCharsets.UTF_8),
        "aaabcgxxab".getBytes(StandardCharsets.UTF_8),
        "aaabcgxxdb".getBytes(StandardCharsets.UTF_8),
        "edf".getBytes(StandardCharsets.UTF_8),
    };

    for (byte[] k : keys) {
      n1.setChild(new String(k, StandardCharsets.UTF_8), new LLeaf(k.length));
    }

    // INode res = recNextMergeOnHash(n1, keys, 0, MergeStrategy.PARTIAL, MapType.HASH, 1);
    INode res = recNextMergeOnFDM(n1, keys, 0, MergeStrategy.FULL, MapType.FDM, 1);
    System.out.println(GraphLayout.parseInstance(res).totalSize());
    reportMergeStatus();
    INode a = res.getChild("aaabcgxxab");
    System.out.println("HELLO");
  }

  public static void hashSimple(TSTree tree) {
    tree.traversePostOrderRec((par, key, cur, stk) -> {

      List<String> keys;
      if ((keys = cur.getKeys()) == null || keys.size() <= 1) return;

      byte[][] codedKeys = strings2ByteArrays(keys);
      int len = findLCPLength(codedKeys);
      if (len == 0) return;
      occ.incrementAndGet();
      ttlLen.addAndGet(len * (codedKeys.length-1));

      INode snode = new HNode();
      ((HNode)snode).pk = Arrays.copyOfRange(codedKeys[0],0, len);
      for (int i = 0; i < keys.size(); i++) {
        byte[] ck = codedKeys[i];
        // snode.addChild(k.substring(len), cur.getChild(k));
        ((HNode)snode).setChild(
          new String(Arrays.copyOfRange(ck, len, ck.length),
              StandardCharsets.ISO_8859_1) ,
            cur.getChild(keys.get(i))
        );
      }

      // inc.addAndGet((int) GraphLayout.parseInstance(snode).totalSize());
      // inc.getAndAdd(-1 * (int) GraphLayout.parseInstance(cur).totalSize());

      if (par != null) {
        par.replace(key, snode);
      }
    });
    reportMergeStatus();
  }

  public static final List<String> dupPaths = new ArrayList<>();
  public static void mergeWithFDM(TSTree tree) {
    tree.traversePostOrderRec((par, key, cur, stk) -> {
        List<String> keys = cur.getKeys();
        if (keys != null && keys.size() > 0) {
          // byte[][] codedKeys = strings2ByteArrays(keys);
          // int len = findLCPLength(codedKeys);
          // if (len == 0) return;

          VirtualFNode vfnode = new VirtualFNode();
          occ.incrementAndGet();

          for (int i = 0 ; i <keys.size(); i++) {
            vfnode.addChild(keys.get(i), cur.getChild(keys.get(i)));
          }

          if (par != null) {
            par.replace(key, vfnode);
          }
        }
    });

    reportMergeStatus();
  }

  public static void mergePrefixes(TSTree tree, MapType mt, MergeStrategy ms) {
    switch (mt) {
      case CDM:
        tree.traversePostOrderRec((par, key, cur, stk) -> {
          List<String> keyList = null;
          if (( keyList = cur.getKeys()) == null) {
            return;
          }
          byte[][] keyBytes = strings2ByteArrays(keyList);

          INode n2 = recNextMergeOnCDM(cur, keyBytes, 0, ms, mt, stk.size());

          if (n2 != cur) {
            if (par == null) {
              tree.root = n2;
            } else {
              par.replace(key, n2);
            }
          }
        });
        reportMergeStatus();
        return;
      case FDM:
        tree.traversePostOrderRec((par, key, cur, stk) -> {
          List<String> keyList = null;
          if (( keyList = cur.getKeys()) == null) {
            return;
          }
          byte[][] keyBytes = strings2ByteArrays(keyList);

          INode n2 = recNextMergeOnFDM(cur, keyBytes, 0, ms, mt, stk.size());

          if (n2 != cur) {
            if (par == null) {
              tree.root = n2;
            } else {
              par.replace(key, n2);
            }
          }
        });
        reportMergeStatus();
        return;
      case HASH:
        tree.traversePostOrderRec((par, key, cur, stk) -> {
          List<String> keyList = null;
          if (( keyList = cur.getKeys()) == null) {
            return;
          }
          byte[][] keyBytes = strings2ByteArrays(keyList);

          INode n2 = recNextMergeOnHash(cur, keyBytes, 0, ms, mt, stk.size());

          if (n2 != cur) {
            if (par == null) {
              tree.root = n2;
            } else {
              par.replace(key, n2);
            }
          }
        });
        reportMergeStatus();
    }
  }

  public static void main2(String[] args) {
    String a = "你好";
    byte[] aa = a.getBytes(StandardCharsets.UTF_8);
    byte[] bb = a.getBytes(StandardCharsets.ISO_8859_1);

    System.out.println("HHH");

    Map<String, INode> hash = new HashMap<>(1);
    hash.put("b", null);
    System.out.println(GraphLayout.parseInstance(hash).toPrintable());
    System.out.println(GraphLayout.parseInstance(hash).totalSize());
    System.out.println(ClassLayout.parseInstance(hash).toPrintable());
  }
}
