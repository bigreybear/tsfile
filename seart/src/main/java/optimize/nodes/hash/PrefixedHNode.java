package optimize.nodes.hash;

import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrefixedHNode extends HNode implements IStaticNode, IInternal  {
  // for prefix-covered child, as Logical Hash may include {"aa", "aab"}
  public INode prePtr = null;
}
