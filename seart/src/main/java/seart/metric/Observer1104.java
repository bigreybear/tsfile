package seart.metric;

import loader.PathTxtLoader;
import seart.ISEARTNode;
import seart.SEARTree;
import seart.traversal.DFSTraversal;
import seart.utils.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Observer1104 {
  public static void checkPrefix() throws IOException{
    List<String> path = PathTxtLoader.getAllLines("mtreedata/baowu.txt");
    SEARTree tree = new SEARTree();
    for (String p : path) {
      tree.insert(p, p.hashCode());
    }
    System.out.println("HERE");

    Set<Pair<ISEARTNode, String>> tar = new HashSet<>();
    DFSTraversal.consumeNodes(tree.root, (node, curPath) -> {
      if (!node.isLeaf() && node.getKeys().length > 3) {
        byte[] keys = node.getKeys();
        Set<String> pks = new HashSet<>();
        ISEARTNode child;
        for (byte k : keys) {
          child = node.getChildByKeyByte(k);
          if (child.getPartialKey() == null || child.getPartialKey().length < 4) continue;
          if (pks.contains(new String(child.getPartialKey(), StandardCharsets.UTF_8))) {
            tar.add(new Pair<>(node, curPath));
            break;
          } else {
            pks.add(new String(child.getPartialKey(), StandardCharsets.UTF_8));
          }
        }
      }
    });

    System.out.println("HERE");
  }

  public static void main(String[] args) throws IOException {
    checkPrefix();
  }
}
