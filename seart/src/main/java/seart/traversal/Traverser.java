package seart.traversal;

import seart.ISEARTNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

public class Traverser {

  public static void traverseDFS(ISEARTNode node, Deque<String> stack) {
    Deque<String> path = stack == null ? new ArrayDeque<>() : stack;

    if (node.isLeaf()) {
      System.out.println(
          String.join("", path) + new String(node.getPartialKey(), StandardCharsets.UTF_8)
              + "," + node.getValue()
      );
      return;
    }

    path.addLast(new String(node.getPartialKey(), StandardCharsets.UTF_8));
    for (byte b : node.getKeys()) {
      path.addLast(String.valueOf((char) b));
      traverseDFS(node.getChildByKeyByte(b), path);
      path.removeLast();
    }
    path.removeLast();
  }
}
