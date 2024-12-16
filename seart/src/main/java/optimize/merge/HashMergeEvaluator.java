package optimize.merge;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import optimize.nodes.INode;
import optimize.nodes.cdm.CNodeHelper;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

/** For both TargetFunction and ability to index with related map type */
public class HashMergeEvaluator {
  // weight between space and time
  public static final float alpha = 1f;
  public static final float t0 = 13; // nano-sec per op
  public static final float HASH_MERGE_COST_FACTOR = 0.2f;

  public static final int REF_SIZE = 4; // unit: byte
  public static final float HASH_LOAD_FACTOR = 0.75f;

  public static final float HASH_MERGE_LOW_BOUND = 0.02f;

  /**
   * @param preLen start of the prefix within the keys
   * @param h height of the logical tree
   * @param ttlChd total child number of current node
   */
  public static boolean evaluateMerge(
      CNodeHelper.ValuedPrefixArray vpa, int preLen, int h, int ttlChd, MapType mapType) {
    if (vpa.bytes.length == 1) return false;
    boolean res = false;
    switch (mapType) {
      case HASH:
        {
          // get the
          // final List<String> keys = new ArrayList<>();
          // for (byte[] key : vpa.bytes) {
          //   keys.add(
          //       new String(
          //           Arrays.copyOfRange(key, preLen, vpa.len + preLen),
          //           StandardCharsets.ISO_8859_1));
          // }

          // legacy
          // int deltaSpace = calcMapMinSpace(keys) - vpa.prd + vpa.len;
          int deltaSpace = 68 - vpa.prd + vpa.len; // 68 for map header and new ptr

          // the 2nd term is definitely > 0, place h in denominator to indicate that
          //  the higher the merge is, the higher the time penalty
          //  i.e., if the merge happens at very deep level, it can rarely affect read perf.
          res = alpha * deltaSpace + (1 - alpha) * t0 * ttlChd / (vpa.bytes.length * h) < 0;
          break;
        }
    }

    return res;
  }

  private static Random dice = new Random();

  /**
   * @param keys keys AFTER prefix truncated.
   */
  public static boolean canFinalIndex(List<String> keys, MapType type) {
    switch (type) {
      case HASH:
        return true;
      case CDM:
        return CNodeHelper.parallelGetBranchingPositions(keys, 5).size() <= 4;
      case FDM:
        // first byte differs
        BitSet bitmap = new BitSet(256);
        for (String s : keys) {
          byte[] sb = s.getBytes(StandardCharsets.UTF_8);
          if (bitmap.get(sb[0])) {
            return false;
          }
          bitmap.set(sb[0]);
        }
        return true;
    }

    return false;
  }

  // keys should be prefix-truncated
  public static int estSpaceGain(List<String> keys, MapType type) {
    switch (type) {
      case HASH:
        {
          int cap = (int) (keys.size() / HASH_LOAD_FACTOR);
          int nodeSiz = cap * 2 * REF_SIZE;
          int pkSiz = keys.stream().mapToInt(String::length).sum();
          return nodeSiz + pkSiz;
        }
      case FDM:
        {
          // except the first byte, trailing bytes are moved to succeeding partial keys
          // while lengths should be accounted here
          int siz = keys.size();
          int sucNodPkLen = keys.stream().mapToInt(String::length).sum() - siz;
          // the FDM node size can be counted as follows
          int nodeSiz;
          if (siz <= 4) {
            nodeSiz = 4 + 4 * REF_SIZE;
          } else if (siz <= 16) {
            nodeSiz = 16 + 16 * REF_SIZE;
          } else if (siz <= 48) {
            nodeSiz = 256 + 48 * REF_SIZE;
          } else if (siz <= 256) {
            nodeSiz = 256 * REF_SIZE;
          } else {
            throw new UnsupportedOperationException();
          }

          // trailing 2 ref for key and ptr array
          return sucNodPkLen + nodeSiz + 2 * REF_SIZE;
        }
      case CDM:
        break;
    }

    return -1;
  }

  public static int calcSpace(byte[] b) {
    return 16 + ((int) Math.ceil(b.length * 1.0 / 8) * 8);
  }

  public static int calcSpace(String s) {
    return 24 + calcSpace(s.getBytes(StandardCharsets.UTF_8));
  }

  public static int calcMapMinSpace(Collection<String> keys) {
    // notice could be smaller than actual
    // Note(zx) INACCURATE especially when treeify starts (single bin with more than 7 items)
    int keySize = keys.stream().mapToInt(HashMergeEvaluator::calcSpace).sum();
    return (int)
        (HASH_MERGE_COST_FACTOR
            * (keySize + 48 /* Map itself */ + 16 /* header of the table */
            /** + keys.size() * (32 + 4) */
            )) /* Node and the slot */;
  }

  // --add-opens java.base/java.util=ALL-UNNAMED
  // public static void testSpace(String[] args) {
  public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
    Map<String, INode> min = new HashMap<>(8, 1.0f);
    min.put("a", null);
    System.out.println(ClassLayout.parseInstance(min).toPrintable());
    System.out.println(GraphLayout.parseInstance(min).totalSize());
    System.out.println(GraphLayout.parseInstance(min).toPrintable());

    // a HashMap has 48 fixed bytes wrapper (4 pads included)
    // the table (Node[]) is an array, so fixed with 16 bytes wrapper
    // above 64 bytes are essential
    System.out.println("HashMap.table Layout: ------------------");
    Field tableField = HashMap.class.getDeclaredField("table");
    tableField.setAccessible(true);
    Object table = tableField.get(min);
    System.out.println(GraphLayout.parseInstance(table).toPrintable());
    System.out.println(ClassLayout.parseInstance(table).toPrintable());

    // each slot in table costs 4 bytes (java.util.HashMap$Node HashMap$Node;.<elements>)
    // 32 bytes per node instance wrapper
    // pointers inside pointing to objects already has been counted (key String and val INode)
    System.out.println("HashMap.Node Layout: ---------------------");
    System.out.println(ClassLayout.parseInstance(table).toPrintable());
    // System.out.println(ClassLayout.parseInstance(Array.get(table, 0)).toPrintable());

    // and then String(bytes) and values

    String a = "xxxxxxxxxxxxxxxxxxxxa";
    byte[] b = "xxxxxxxxxxxa".getBytes(StandardCharsets.UTF_8);

    // array has 16 bytes (pads included) and bytes content (aligned to 8)
    // a String wrapping the byte array, occupies another 24 (fixed)
    System.out.println("String and byte array layout: -------------- ");
    System.out.println(ClassLayout.parseInstance(a).toPrintable());
    System.out.println(GraphLayout.parseInstance(a).totalSize());
    System.out.println(ClassLayout.parseInstance(b).toPrintable());
    System.out.println(GraphLayout.parseInstance(b).totalSize());

    // typically, lists are bigger than strings

    if (GraphLayout.parseInstance(b).totalSize() == calcSpace(b)) {
      System.out.println("byte array calculate right");
    }

    if (GraphLayout.parseInstance(a).totalSize() == calcSpace(a)) {
      System.out.println("string calculate right");
    }

    min.put("xxxxb", null);
    // min.put("xxxxbaaaaaaaaaaaa", null);
    // min.put("aatacca", null);
    // min.put("xxxxbaacaa", null);
    // min.put("xccaabbb", null);
    int diff = 0;
    if ((diff = (int) (GraphLayout.parseInstance(min).totalSize() - calcMapMinSpace(min.keySet())))
        > 0) {
      // Note(zx) error derives as the key set not created when measured by GraphLayout,
      //  and table.len could be inaccurate
      System.out.println("map calculate diff: " + diff);
      System.out.println(GraphLayout.parseInstance(min).toPrintable());
    }
  }
}
