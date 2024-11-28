package optimize;

import optimize.nodes.cdm.CNodeHelper;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;

/**
 * For both TargetFunction and ability to index with related map type
 */
public class Evaluator {
  // weight between space and time
  public static final float alpha = 0.5f;

  public static final int REF_SIZE = 4; // unit: byte
  public static final float HASH_LOAD_FACTOR = 0.75f;

  public enum MergeStrategy {
    SIMPLE,
    PARTIAL,
    FULL;
  }

  public enum MapType {
    HASH,
    FDM, // first diff map
    CDM; // complete diff map
  }

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
      case HASH: {
        int cap = (int) (keys.size() / HASH_LOAD_FACTOR);
        int nodeSiz = cap * 2 * REF_SIZE;
        int pkSiz = keys.stream().mapToInt(String::length).sum();
        return nodeSiz + pkSiz;
      }
      case FDM: {
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


}

