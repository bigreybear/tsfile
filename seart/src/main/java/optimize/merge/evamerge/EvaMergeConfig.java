package optimize.merge.evamerge;

public class EvaMergeConfig {
  public static final ARTNodeType ART_TYPE = ARTNodeType.naive;
  public static final double LOAD_FACTOR = 0.75d;
  public static final int NODE_OBJ_ACC = 1;  // estimated cost of node object access penalty
  public static IndexType INDEX_TYPE = IndexType.hash;
  public static float ALPHA = 0f;  // weight of space

  public enum IndexType {
    hash,
    sorted;
  }

  public enum ARTNodeType {
    naive,
    compact;
  }
}
