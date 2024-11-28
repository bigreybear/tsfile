package optimize.nodes.fdm.vfull;

import optimize.nodes.INode;

public interface SeriesIndexTree {
  void insert(String key, INode value);

  INode search(String sk);
}
