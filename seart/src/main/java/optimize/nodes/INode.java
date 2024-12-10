package optimize.nodes;

import optimize.util.ByteArray;

import java.rmi.StubNotFoundException;
import java.util.List;
import java.util.Map;

public interface INode {

  long getValue(); // only for leaves

  /**
   * @param name is a segment of the series identifier, may across multiple nodes
   * @return the result corresponds to the whole nodes
   */
  INode getChild(String name);

  List<INode> getChildren();

  List<String> getKeys();

  byte[] getPartialKey();

  INode addChild(String name, INode child);

  INode replace(String key, INode nNode);

  default INode replace(byte[] key, INode nNode) {throw new UnsupportedOperationException();}

  default List<byte[]> getKeyBytes() {return null;}

  default byte[] getKeysFromFDM() {return null;}

  default INode getChildByBytes(byte[] k) {throw new UnsupportedOperationException();}

  default byte[][] getKeysFromCDM() {throw new UnsupportedOperationException();}
}
