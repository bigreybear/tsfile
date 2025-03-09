package optimize.merge.evamerge;

import optimize.nodes.IMicroNode;
import org.antlr.v4.runtime.atn.PredicateTransition;

import java.util.Collection;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class EvaHelper {
  public static final int c0=1, c1=1, c2=1;
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

  public static int estSpaceNativeART(byte[] pk, int k) {
    return mpNativeART(pk, k) + mkNativeART(k);
  }

  private static int mpNativeART(byte[] pk, int k) {
    return (pk == null || pk.length == 0) ? 56 : 56 + 16 + xi(pk.length);
  }

  private static int mkNativeART(int k) {
    if (k <= 4) {
      return 24;
    } else if (k <= 16) {
      return 80;
    } else if (k <= 48) {
      return 448;
    } else if (k <= 1024) {
      return 1024;
    }
    throw new RuntimeException();
  }


  // k for number of children
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

  // endregion

  // region Time Estimation

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

  public static double estTimeMBNHash(int k) {
    return k * 1.0 /10;
  }

  public static double estTimeMBNSorted(int k) {
    return Math.log(k) / log2;
  }

  // endregion

  public static void main(String[] args) {
    System.out.println(xi(7));
    System.out.println(xi(11));
    System.out.println(xi(255));
  }
}
