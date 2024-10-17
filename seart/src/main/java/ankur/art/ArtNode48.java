/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package ankur.art;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

// NOTE keys set in corresponding slot, children set at first null slot
class ArtNode48 extends ArtNode {
  public static int count;
  // NOTE initial value of keys is zero, so handle it as an exception
  // Note(zx) [elem of keys] is [index of children]
  public byte[] keys = new byte[256];
  public Node[] children = new Node[48];

  // region Mod Methods


  @Override
  public int compactedSize() {
    // prefix + key_maps + pointers
    int totalSize = this.compactedPartialLen() + 256 + 48 * 8;
    Iterator<Node> nodeIterator = this.getChildren();
    for (Iterator<Node> it = nodeIterator; it.hasNext(); ) {
      Node n = it.next();
      totalSize += n.compactedSize();
    }
    return totalSize;
  }

  @Override
  public byte getType() {
    return 3;
  }

  @Override
  public byte getKeyAt(int i) {
    return (byte) i;
  }

  public static ArtNode48 padding(List<Byte> k, List<Node> c, String p) {
    ArtNode48 res = new ArtNode48();

    if (p != null) {
      res.partial_len = p.getBytes().length;
      System.arraycopy(p.getBytes(), 0, res.partial, 0, res.partial_len);
    }

    for (int i = 0; i < k.size(); i++) {
      res.add_child(null, k.get(i), c.get(i));
    }
    return res;
  }

  public static void main(String[] args) throws IOException {
    byte[] exa = new byte[10];
    exa[3] = 22;
    exa[5] = 66;

    ArtNode48 node = new ArtNode48();
    Leaf l = new Leaf(exa, (long) 123);

    node.add_child(null, (byte) 12, l);
    node.add_child(null, (byte) 23, l);
    node.add_child(null, (byte) 45, l);
    node.add_child(null, (byte) 67, l);
    node.add_child(null, (byte) 11, l);
    node.add_child(null, (byte) 14, l);
    node.add_child(null, (byte) 15, l);
    node.add_child(null, (byte) 16, l);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ArtTree.serialize(node, baos);
    ReadWriteIOUtils.write(node.offset, baos);
    exa = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(exa);
    buffer.position(buffer.capacity() - 8);
    buffer.position((int) ReadWriteIOUtils.readLong(buffer));
    Node node2 = ArtTree.deserialize(buffer);
    System.out.println(node2);

    Iterator<Node> ite = node.getChildren();
    while (ite.hasNext()) {
      System.out.println(ite.next());
    }
  }

  @Override
  public boolean valid(int i) {
    return keys[i] != 0;
  }

  @Override
  public Iterator<Node> getChildren() {
    return new Iterator<Node>() {
      int i = 0, lastIndex = 0;

      @Override
      public boolean hasNext() {
        return i < num_children;
      }

      @Override
      public Node next() {
        while (!valid(lastIndex)) {
          lastIndex++;
        }
        i++;
        return childAt(lastIndex++);
      }
    };
  }
  // endregion

  public ArtNode48() {
    super();
    count++;
  }

  public ArtNode48(final ArtNode48 other) {
    super(other);
    System.arraycopy(other.keys, 0, keys, 0, 256);
    // Copy the children. We have to look at all elements of `children`
    // rather than just the first num_children elements because `children`
    // may not be contiguous due to deletion
    for (int i = 0; i < 48; i++) {
      children[i] = other.children[i];
      if (children[i] != null) {
        children[i].refcount++;
      }
    }
    count++;
  }

  public ArtNode48(final ArtNode16 other) {
    this();
    // ArtNode
    this.num_children = other.num_children;
    this.partial_len = other.partial_len;
    System.arraycopy(other.partial, 0, this.partial, 0, Math.min(MAX_PREFIX_LEN, this.partial_len));

    // ArtNode48 from ArtNode16
    for (int i = 0; i < this.num_children; i++) {
      keys[to_uint(other.keys[i])] = (byte) (i + 1);
      children[i] = other.children[i];
      children[i].refcount++;
    }
  }

  public ArtNode48(final ArtNode256 other) {
    this();
    assert (other.num_children <= 48);
    // ArtNode
    this.num_children = other.num_children;
    this.partial_len = other.partial_len;
    System.arraycopy(other.partial, 0, this.partial, 0, Math.min(MAX_PREFIX_LEN, this.partial_len));

    // ArtNode48 from ArtNode256
    int pos = 0;
    for (int i = 0; i < 256; i++) {
      if (other.children[i] != null) {
        keys[i] = (byte) (pos + 1);
        children[pos] = other.children[i];
        children[pos].refcount++;
        pos++;
      }
    }
  }

  @Override
  public Node n_clone() {
    return new ArtNode48(this);
  }

  @Override
  public ChildPtr find_child(byte c) {
    int idx = to_uint(keys[to_uint(c)]);
    // Note(zx) this line indicates that 0 is illegal for indexing children
    if (idx != 0) return new ArrayChildPtr(children, idx - 1);
    return null;
  }

  @Override
  public Leaf minimum() {
    int idx = 0;
    while (keys[idx] == 0) idx++;
    Node child = children[to_uint(keys[idx]) - 1];
    return Node.minimum(child);
  }

  @Override
  public void add_child(ChildPtr ref, byte c, Node child) {
    assert (refcount <= 1);

    if (this.num_children < 48) {
      // Have to do a linear scan because deletion may create holes in
      // children array
      int pos = 0;
      while (children[pos] != null) pos++;

      this.children[pos] = child;
      child.refcount++;
      this.keys[to_uint(c)] = (byte) (pos + 1);
      this.num_children++;
    } else {
      // Copy the node48 into a new node256
      ArtNode256 result = new ArtNode256(this);
      // Update the parent pointer to the node256
      ref.change(result);
      // Insert the element into the node256 instead
      result.add_child(ref, c, child);
    }
  }

  @Override
  public void remove_child(ChildPtr ref, byte c) {
    assert (refcount <= 1);

    // Delete the child, leaving a hole in children. We can't shift children
    // because that would require decrementing many elements of keys
    int pos = to_uint(keys[to_uint(c)]);
    keys[to_uint(c)] = 0;
    children[pos - 1].decrement_refcount();
    children[pos - 1] = null;
    num_children--;

    if (num_children == 12) {
      ArtNode16 result = new ArtNode16(this);
      ref.change(result);
    }
  }

  @Override
  public boolean exhausted(int c) {
    // NOTE still non-zero key ahead, c as index
    for (int i = c; i < 256; i++) {
      if (keys[i] != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int nextChildAtOrAfter(int c) {
    int pos = c;
    for (; pos < 256; pos++) {
      if (keys[pos] != 0) {
        break;
      }
    }
    return pos;
  }

  @Override
  public Node childAt(int c) {
    return children[to_uint(keys[c]) - 1];
  }

  @Override
  public int decrement_refcount() {
    if (--this.refcount <= 0) {
      int freed = 0;
      for (int i = 0; i < this.num_children; i++) {
        if (children[i] != null) {
          freed += children[i].decrement_refcount();
        }
      }
      count--;
      // delete this;
      return freed + 728;
      // object size (8) + refcount (4) +
      // num_children int (4) + partial_len int (4) +
      // pointer to partial array (8) + partial array size (8+4+1*MAX_PREFIX_LEN)
      // pointer to key array (8) + key array size (8+4+1*256) +
      // pointer to children array (8) + children array size (8+4+8*48)
    }
    return 0;
  }
}
