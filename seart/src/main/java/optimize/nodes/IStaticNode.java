package optimize.nodes;

public interface IStaticNode extends INode{

  @Override
  default INode addChild(String name, INode child) {
    throw new UnsupportedOperationException();
  }

  static String truncatePrefix(String src, String prefix) {
    if (src == null || prefix == null) {
      throw new IllegalArgumentException("Source string and prefix must not be null");
    }

    if (!src.startsWith(prefix)) {
      throw new IllegalArgumentException("Source string does not start with the given prefix: " + prefix);
    }

    return src.substring(prefix.length());
  }
}
