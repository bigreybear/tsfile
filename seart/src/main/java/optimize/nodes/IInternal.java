package optimize.nodes;

public interface IInternal extends INode{
  @Override
  default long getValue() {
    throw new UnsupportedOperationException();
  }

  INode replace(String key, INode nNode);
}
