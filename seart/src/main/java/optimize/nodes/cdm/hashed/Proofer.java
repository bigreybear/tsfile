package optimize.nodes.cdm.hashed;

import java.util.Random;

public class Proofer {
  static final int N_KEYS = 200;
  static final int NUM_SETS = 1;
  static final long P = 4294967291L; // 适合 32 位 key 的大质数

  // 随机生成 n 个 32 位整型 key
  static long[] generateRandomKeys(Random rnd, int n) {
    long[] keys = new long[n];
    for (int i = 0; i < n; i++) {
      // 这里随意生成，也可改成读实际数据
      keys[i] = rnd.nextInt() & 0xFFFFFFFFL;
    }
    return keys;
  }

  // 检查对给定 keys 的哈希是否无碰撞
  static boolean isPerfect(long a, long b, long[] keys) {
    boolean[] used = new boolean[N_KEYS];
    for (long key : keys) {
      long h = Math.abs(((a * key + b) % P) % N_KEYS);
      if (used[(int)h]) {
        return false;
      }
      used[(int)h] = true;
    }
    return true;
  }

  public static void main(String[] args) {
    Random rnd = new Random(0);

    long start = System.currentTimeMillis();
    for (int s = 0; s < NUM_SETS; s++) {
      long[] keys = generateRandomKeys(rnd, N_KEYS);
      // 通过暴力搜索 (a, b)，直到找到完美哈希
      while (true) {
        long a = 1 + (Math.abs(rnd.nextLong()) % (P - 1));
        long b = Math.abs(rnd.nextLong()) % P;
        if (isPerfect(a, b, keys)) {
          // 找到 (a, b)，可以记录 / 存储起来
          break;
        } else {
          System.out.println(String.format("Not perfect: %d %d", a, b));
        }
      }
    }
    long end = System.currentTimeMillis();

    System.out.println("构造 " + NUM_SETS + " 个完美哈希的总耗时: "
        + (end - start) + " ms");
  }
}
