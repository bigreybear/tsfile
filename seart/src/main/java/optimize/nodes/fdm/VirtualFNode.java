package optimize.nodes.fdm;

import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.fdm.vfull.SEARTree;

import java.util.List;

// employ an ART to imitate FNode
public class VirtualFNode implements INode, IInternal {
  SEARTree tree = new SEARTree();

  @Override
  public INode getChild(String name) {
    return tree.search(name);
  }

  @Override
  public List<INode> getChildren() {
    return null;
  }

  @Override
  public List<String> getKeys() {
    return null;
  }

  @Override
  public byte[] getPartialKey() {
    return null;
  }

  @Override
  public INode addChild(String name, INode child) {
    tree.insert(name, child);
    // fixme should not return null
    return null;
  }

  @Override
  public INode replace(String key, INode nNode) {
    return null;
  }
}
