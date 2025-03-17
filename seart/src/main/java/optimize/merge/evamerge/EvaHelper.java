package optimize.merge.evamerge;

import optimize.nodes.IMicroNode;
import optimize.nodes.fdm.IFNode;
import optimize.util.ByteArray;

import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;

import static optimize.merge.evamerge.EvaMergeConfig.IndexType.sorted;

public class EvaHelper {
  public static final int c0=1, c1=1, c2=2;
  private static final double log2 = Math.log(2);

  public static int xi(int v) {
    return (v + 7) & ~7;
  }

  // transform mini-tree height to hyper node width
  private static int width(int h) {
    if (h == 1 || h > 8) throw new RuntimeException();
    if (h <= 2) return 2;
    if (h <= 4) return 4;
    return 8;
  }

  // region Space Estimation

  @Deprecated
  public static int estSpaceNativeART(byte[] pk, int k) {
    return mpNativeART(pk, k) + mkNativeART(k);
  }

  private static int mpNativeART(byte[] pk, int k) {
    return (pk == null || pk.length == 0) ? 56 : 56 + 16 + xi(pk.length);
  }

  public static int estSpcNativeARTV2(IMicroNode node) {
    return estSpcNativeARTV2(node.getParKeyLen(), node.getChildren().size());
  }

  public static int estSpcNativeARTV2(int pkLen, int k) {
    int res = 0;
    // although the two if branches goes into same...
    if (k <= 48) {
      // header + pk:ref + brk:ref + chd:ref
      res += xi(12 + 4 + 4 + 4);
    } else {
      // header + pk:ref + brk:ref + chd:ref
      res += xi(12 + 4 + 4);
    }

    if (pkLen != 0) res += 16 + xi(pkLen); // pk header, len, elem

    if (k <= 4) {
      res += 16 + xi(4); // key arr
      res += 16 + xi(4*4); // chd arr
    } else if (k <= 16) {
      res += 16 + xi(16);
      res += 16 + xi(4*16);
    } else if (k <= 48) {
      res += 16 + 256;
      res += 16 + xi(48*4);
    } else if (k <= 1024) {
      res += 16 + 1024;  // no more key arr
    }

    return res;
  }

  @Deprecated
  private static int mkNativeART(int k) {
    if (k <= 4) {
      return 24;  // 16 + xi(4)=16+8
    } else if (k <= 16) {
      return 80;  // 64 + 16
    } else if (k <= 48) {
      return 448; // 256 + 192
    } else if (k <= 1024) {
      return 1024; // wrong! no ref, header nor len for key arr
    }
    throw new RuntimeException();
  }


  // k for number of children
  @Deprecated
  public static int estSpaceMultiBranchNode(byte[] pk, int k, Collection<byte[]> rmk, TreeSet<Integer> posSet) {
    int pmin = posSet.first(), pmax = posSet.last(), d = posSet.size();
    int wid = width(d);

    boolean consecutive = (pmax - pmin + 1 == d);
    int sum = 56 + xi(4+wid) + xi(wid*k) + xi(4*k);
    if (pk != null && pk.length != 0) {
      sum += 16 + xi(pk.length);
    }
    if (!consecutive) {
      if (rmk.isEmpty()) throw new RuntimeException();
      for (byte[] r : rmk) {
        if (r != null && r.length != 0) {
          sum += 16 + xi(r.length);
        }
      }
    }
    return sum;
  }

  /**
   * A wrapper to actual accurate estimation.
   * @param map refs to {@link MergingStatusV2#candidKeysMaps}.
   */
  public static int estSpaceMultiBranchNodeV2(
      byte[] pk, Map<ByteArray, Object[]> map,
      TreeSet<Integer> posSet, float lf) {

    // extract rmk array
    byte[][] rmkArr = new byte[map.size()][];
    int index = 0;
    for (Object[] value : map.values()) {
      rmkArr[index++] = (byte[]) value[1];
    }

    return accurateEstSpaceMultiBranchNode(
        pk,
        map.size(),
        rmkArr,
        posSet,
        lf
    );
  }

  public static int accurateEstSpaceMultiBranchNode(
      byte[] pk, int mapSize, byte[][] rmkArr,
      TreeSet<Integer> posSet, float lf) {
    // header + pk:ref + brk[]:ref + chd[]:ref + rmk[][]:ref + pos bytes
    int res = xi(12 + 4 + 4 + 4 + 4 + posSet.size());  // equals 24 + xi(4+pos.size)

    if (pk != null && pk.length > 0) {
      res += 16 + xi(pk.length);
    }

    // k: actual slots in each array
    int k = EvaMergeConfig.INDEX_TYPE == sorted ? mapSize : (int) (mapSize / lf);
    int pmin = posSet.first(), pmax = posSet.last(), d = posSet.size();
    int wid = width(d);
    res += 16 + xi(k*wid); // brk array
    res += 16 + xi(4 * k); // chd array

    boolean consecutive = (pmax - pmin + 1 == d);
    // rmk array object
    // todo RMK 是不是应该全部考虑空间？是不是某些孩子的 rmk 应该不应该记入？
    // 应该和 raw space 对齐：叶子的 pk 应该全部移出；
    if (!consecutive) {
      int rmkTtlLen = 0; // total size of all rmk object(byte[])
      for (byte[] rmk : rmkArr) {
        rmkTtlLen += (rmk == null || rmk.length == 0) ? 0 : 16 + xi(rmk.length);
      }
      if (rmkTtlLen != 0) {
        // need to build complete rmk array
        res += 16 + xi(4*k); // the array of refs, the body of byte[][]
        res += rmkTtlLen;  // the content of each byte[]
      }
    }

    return res;
  }

  // endregion

  // region Time Estimation

  public static double estTimeART(IFNode node) {
    int siz = node.getChildNum();
    if (EvaMergeConfig.ART_TYPE == EvaMergeConfig.ARTNodeType.naive) {
      if (siz <= 16) {
        return (int) (c0 * Math.log(siz * 1.0) / log2);
      } else if (siz <= 48) {
        return c1;
      } else if (siz <= 256) {
        return c2;
      }
    } else {
      if (siz <= 16) {
        return (int) (c0 * Math.log(siz * 1.0) / log2);
      } else if (siz <= 192) {
        return c1;
      } else if (siz <= 256) {
        return c2;
      }
    }
    throw new RuntimeException();
  }

  public static double estTimeNativeART(int k) {
    if (k <= 16) {
      return (int) (c0 * Math.log(k * 1.0) / log2);
    } else if (k <= 48) {
      return c1;
    } else if (k <= 256) {
      return c2;
    }

    throw new RuntimeException();
  }

  @Deprecated
  public static double estTimeMBNHash(int k) {
    return k * 1.0 /10;
  }

  public static double estTimeMBNSorted(int k) {
    return Math.log(k) / log2;
  }

  public static double linerProbeHashLatency(double lf, int n) {
    if (lf == 1.0d) {
      return n*1.0d/2;
    }

    double res = (1 + (1.0f / (1 - lf))) * 1.0f / 2;
    return Math.min(res, n*1.0d/2);
  }

  // endregion

  public static void main(String[] args) {
    System.out.println(xi(7));
    System.out.println(xi(11));
    System.out.println(xi(255));

    System.out.println(linerProbeHashLatency(0.75, 100));
  }
}
