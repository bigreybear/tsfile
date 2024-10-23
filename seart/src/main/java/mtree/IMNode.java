package mtree;

import java.io.Serializable;

public interface IMNode extends Serializable {

  long getValue();

  IMNode getChild(String name);

  void addChild(String name, IMNode child);
}
