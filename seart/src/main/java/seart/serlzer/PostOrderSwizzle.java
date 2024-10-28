package seart.serlzer;

import static seart.utils.NumericalIOUtil.INT_LEN;
import static seart.utils.NumericalIOUtil.LONG_LEN;
import static seart.utils.NumericalIOUtil.readInt;
import static seart.utils.NumericalIOUtil.readLong;
import static seart.utils.NumericalIOUtil.readUnsignedVarInt;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import seart.ISEARTNode;
import seart.Leaf;
import seart.RefNode;
import seart.SEARTree;
import seart.SeriesIndexTree;
import seart.miner.MockSubtreeMiner;
import seart.traversal.DFSTraversal;
import seart.utils.NumericalIOUtil;
import seart.utils.PublicBAOS;

public class PostOrderSwizzle {

  static final byte REF_FLG = (byte) 0xff;
  static final byte LEF_FLG = (byte) 0xfe;

  private static void createAndWriteFile(String fileName, PublicBAOS content) {
    try (FileOutputStream fos = new FileOutputStream(fileName)) {
      content.writeTo(fos);
    } catch (IOException e) {
      System.out.println("Exception while writing file:" + fileName);
    }
  }

  public static void serializeSEART(SeriesIndexTree tree, String fileName) throws IOException {
    serializeSEART(((SEARTree) tree).root, fileName);
  }

  /*-
   * File organized: main tree, template trees, roots of tree (long), num of trees (int)
   * For each internal: [Partial Key bytes with Var Len] [num of child: Byte] [child byte and ofs/ptr]
   * For each leaf, [pk vl] [num Byte = 0] [0xfe] [value: long]
   * For each ref node, [pk vl] [num Byte = 0] [0xff] [tID: var uint] [value num var int] [values array: Long]
   * Both [0xff] and [0xfe] in leaf and ref node is identifying flags.
   * </br>
   * Deficiency: value of leaf must be positive.
   */
  public static void serializeSEART(ISEARTNode root, String fileName) throws IOException {
    PublicBAOS baos = new PublicBAOS();
    Map<ISEARTNode, Integer> tplMap = new HashMap<>();
    long mainTreeOffset = writeNode(root, baos, tplMap);

    long[] tTreeOffsets = new long[tplMap.size()];
    List<ISEARTNode> sortedTemplates =
        tplMap.entrySet().stream()
            .sorted(Comparator.comparingInt(Map.Entry::getValue))
            .map(e -> e.getKey())
            .collect(Collectors.toList());
    tplMap.clear();
    for (int i = 0; i < sortedTemplates.size(); i++) {
      tTreeOffsets[i] = writeNode(sortedTemplates.get(i), baos, tplMap);
      if (!tplMap.isEmpty())
        throw new RuntimeException("Template trees should not nest each other.");
    }

    NumericalIOUtil.write(mainTreeOffset, baos);
    for (int i = 0; i < tTreeOffsets.length; i++) {
      NumericalIOUtil.write(tTreeOffsets[i], baos);
    }

    NumericalIOUtil.write(tTreeOffsets.length + 1, baos);
    createAndWriteFile(fileName, baos);
  }

  // return the bytes offset of the passing in node
  private static long writeNode(ISEARTNode node, PublicBAOS baos, Map<ISEARTNode, Integer> tplMap)
      throws IOException {
    if (!node.isLeaf()) {
      byte[] keys = node.getKeys();
      long[] chdOfs = new long[keys.length];
      for (int i = 0; i < chdOfs.length; i++) {
        chdOfs[i] = writeNode(node.getChildByKeyByte(keys[i]), baos, tplMap);
      }

      // only now the offset is accurate since all children have been serialized
      long off = baos.size();
      writePartialKey(node.getPartialKey(), baos);
      baos.write((byte) (chdOfs.length & 0xff));
      for (int i = 0; i < chdOfs.length; i++) {
        baos.write(keys[i]);
        NumericalIOUtil.write(chdOfs[i], baos);
      }
      return off;
    }

    long off = baos.size();
    writePartialKey(node.getPartialKey(), baos);
    baos.write((byte) 0); // means a node with no children
    if (node instanceof RefNode) {
      baos.write(REF_FLG);
      ISEARTNode tn = ((RefNode) node).templateRoot;
      int tid = tplMap.computeIfAbsent(tn, k -> tplMap.size());
      NumericalIOUtil.writeUnsignedVarInt(tid, baos);
      NumericalIOUtil.writeUnsignedVarInt(((RefNode) node).values.length, baos);
      for (long val : ((RefNode) node).values) {
        NumericalIOUtil.write(val, baos);
      }
      return off;
    }

    baos.write(LEF_FLG);
    NumericalIOUtil.write(node.getValue(), baos);
    return off;
  }

  private static void writePartialKey(byte[] pkb, OutputStream os) throws IOException {
    if (pkb == null || pkb.length == 0) {
      os.write(NumericalIOUtil.getUnsignedVarInt(0));
      return;
    }
    os.write(NumericalIOUtil.getUnsignedVarInt(pkb.length));
    os.write(pkb);
  }

  private static byte[] readPartialKey(InputStream is) throws IOException {
    int size = readUnsignedVarInt(is);
    byte[] res = new byte[size];
    int len = is.read(res);
    if (len != size) {
      throw new RuntimeException("Incomplete Partial Key incurred.");
    }
    return res;
  }

  public static SEARTree[] loadSEART(String fileName) throws IOException {
    ISEARTNode[] trees;
    try (FileInputStream fis = new FileInputStream(fileName);
        FileChannel channel = fis.getChannel()) {
      long fileSize = channel.size();
      channel.position(fileSize - INT_LEN);
      int treNum = readInt(fis);
      long[] rootPos = new long[treNum];
      channel.position(fileSize - INT_LEN - (long) treNum * LONG_LEN);
      for (int i = 0; i < treNum; i++) {
        rootPos[i] = readLong(fis);
      }
      trees = new ISEARTNode[treNum];
      for (int i = treNum - 1; i >= 0; i--) {
        // deser trees backward
        trees[i] = readSEART(rootPos[i], fis, channel, trees);
      }
    }
    return Arrays.stream(trees).map(SEARTree::new).toArray(SEARTree[]::new);
  }

  private static ISEARTNode readSEART(
      long nodPos, FileInputStream fis, FileChannel channel, ISEARTNode[] templates)
      throws IOException {
    channel.position(nodPos);
    byte[] pk = readPartialKey(fis);
    int chdNum = fis.read();
    if (chdNum == 0) {
      byte flag = (byte) (fis.read() & 0xff);
      switch (flag) {
        case LEF_FLG:
          Leaf n0 = new Leaf();
          n0.reassignPartialKey(pk);
          n0.setValue(NumericalIOUtil.readLong(fis));
          return n0;
        case REF_FLG:
          // RefNode
          RefNode n = new RefNode();
          n.reassignPartialKey(pk);
          n.templateRoot = templates[NumericalIOUtil.readUnsignedVarInt(fis) + 1];
          int num = NumericalIOUtil.readUnsignedVarInt(fis);
          n.values = new long[num];
          for (int i = 0; i < num; i++) {
            n.values[i] = NumericalIOUtil.readLong(fis);
          }
          return n;
        default:
          throw new UnsupportedEncodingException("Flag broken." + flag);
      }
    }

    byte[] keys = new byte[chdNum];
    long[] offs = new long[chdNum];
    ISEARTNode[] des = new ISEARTNode[chdNum];
    for (int i = 0; i < chdNum; i++) {
      keys[i] = (byte) (fis.read() & 0xff);
      offs[i] = NumericalIOUtil.readLong(fis);
    }
    for (int i = 0; i < chdNum; i++) {
      des[i] = readSEART(offs[i], fis, channel, templates);
    }
    return ISEARTNode.constructNode(pk, keys, des);
  }

  public static void main(String[] args) throws IOException {
    String[] testPaths =
        new String[] {
          "root.ab.c.d", "root.adddd", "root.ggax", "root.bc", "root.dddd", "root.eeee"
        };

    String[] pattern = new String[] {"humidity", "current", "cin", "I.c", "xz", "gan"};

    System.out.println(Arrays.toString(testPaths));

    SEARTree tree = new SEARTree();
    SEARTree pat = new SEARTree();
    for (String s : testPaths) {
      tree.insert(s, s.hashCode());
    }
    for (String p : pattern) {
      pat.insert(p, p.hashCode());
    }

    MockSubtreeMiner.replaceV1(tree.root, pat.root, (a, b) -> true);
    serializeSEART(tree.root, "test1028.seart");

    SEARTree[] trees = loadSEART("test1028.seart");
    DFSTraversal.printAllPathsInStatic(trees[0]);
    for (String s : DFSTraversal.getAllPaths(trees[0].root)) {
      if (s.hashCode() != trees[0].search(s)) {
        System.out.println("WRONG");
      }
    }
  }
}
