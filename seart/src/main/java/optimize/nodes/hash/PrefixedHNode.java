package optimize.nodes.hash;

import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;

@Deprecated
public class PrefixedHNode extends HNode implements IStaticNode, IInternal {
  // for prefix-covered child, as Logical Hash may include {"aa", "aab"}
  public INode prePtr = null;
}
