package mtree;

public interface IMNode {

  long getValue();

  IMNode getChild(String name);

  void addChild(String name, IMNode child);
}
