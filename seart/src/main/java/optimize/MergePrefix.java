package optimize;

import optimize.nodes.INode;
import optimize.nodes.cdm.CNodeHelper;
import optimize.nodes.fdm.VirtualFNode;
import optimize.nodes.hash.HNode;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import seart.exception.PrefixPropertyException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static optimize.Evaluator.MergeStrategy;
import static optimize.Evaluator.MapType;
import static optimize.nodes.cdm.CNodeHelper.evaluatePrefixes;
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

  public static INode initNodeWithPartialKey(byte[] key, int len, MapType mapType) {
    return null;
  }

  public static boolean evaluateMerge(ValuedPrefixArray vpa, int h) {
    return h > 0;
  }

  // PRIMARY
  public static INode recNextMerge(INode oriNode, byte[][] keys, int preLen,
                                   MergeStrategy ms, MapType mt, int height) {
    // fixme height is for Logical Tree which is pretty rough

    int len = findLCPLength(keys);

    // the node is now merging
    final INode megNode = initNodeWithPartialKey(keys[0], len, mt);
    // todo handle the prefixed key, it should not join later process

    List<ValuedPrefixArray> groupedPrefix = groupPrefixes(keys, preLen, 1);

    for (ValuedPrefixArray vpa : groupedPrefix) {
      if (evaluateMerge(vpa, height) && vpa.bytes.length > 1) {
        final INode recNode = recNextMerge(oriNode, vpa.bytes, preLen + vpa.len,
            ms, mt, height);
        megNode.addChild(
            new String(
                Arrays.copyOfRange(vpa.bytes[0], preLen, preLen + vpa.len),
                StandardCharsets.ISO_8859_1),
            recNode
        );
      } else {
        for (byte[] k : vpa.bytes) {
          megNode.addChild(
              new String(Arrays.copyOfRange(k, preLen, k.length), StandardCharsets.ISO_8859_1),
              oriNode.getChild(new String(k, StandardCharsets.UTF_8))
          );
        }
      }
    }

    return megNode;
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
  public static void FDMFull(TSTree tree) {
    tree.traversePreOrder((par, key, cur, stk) -> {
        List<String> keys = cur.getKeys();
        if (keys != null && keys.size() > 1) {
          // byte[][] codedKeys = strings2ByteArrays(keys);
          // int len = findLCPLength(codedKeys);
          // if (len == 0) return;

          VirtualFNode vfnode = new VirtualFNode();
          VirtualFNode virtualFNode = new VirtualFNode(); // to measure net space
          occ.incrementAndGet();

          for (int i = 0 ; i <keys.size(); i++) {
            try {
              vfnode.addChild(keys.get(i), cur.getChild(keys.get(i)));
            } catch (PrefixPropertyException e) {
              dupPaths.add(keys.get(i));
            }
          }


          for (int i = 0 ; i <keys.size(); i++) {
            try {
              virtualFNode.addChild(keys.get(i), null);
            } catch (PrefixPropertyException e) {
              dupPaths.add(keys.get(i));
            }
          }

          inc.addAndGet((int) GraphLayout.parseInstance(virtualFNode).totalSize());

          if (par != null) {
            par.replace(key, vfnode);
          }
        }
    });

    System.out.println("Total Dup Str len" + dupPaths.stream().mapToInt(String::length).sum());
    reportMergeStatus();
  }

  public static void mergePrefixes(TSTree tree, MapType mt, MergeStrategy ms) {
    switch (mt) {
      case CDM:
        switch (ms) {
          case FULL:
          case SIMPLE:
          case PARTIAL:
        }
      case FDM:
        switch (ms) {
          case FULL:
            FDMFull(tree);
            return;
          case SIMPLE:
          case PARTIAL:
        }
      case HASH:
        switch (ms) {
          case FULL:
          case SIMPLE:
            hashSimple(tree);
            return;
          case PARTIAL:
        }
    }
  }

  public static void main(String[] args) {
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
