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
package seart.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/** Utils to read/write stream. */
public class NumericalIOUtil {
  private NumericalIOUtil() {}

  public static final int BOOLEAN_LEN = 1;
  public static final int SHORT_LEN = 2;
  public static final int INT_LEN = 4;
  public static final int LONG_LEN = 8;
  public static final int DOUBLE_LEN = 8;
  public static final int FLOAT_LEN = 4;
  public static final float BIT_LEN = 0.125F;

  private static final int NO_BYTE_TO_READ = -1;

  private static final String RETURN_ERROR = "Intend to read %d bytes but %d are actually returned";

  /** transform an int var to byte[] format. */
  public static byte[] getUnsignedVarInt(int value) {
    int preValue = value;
    int length = 0;
    while ((value & 0xFFFFFF80) != 0L) {
      length++;
      value >>>= 7;
    }
    length++;

    byte[] res = new byte[length];
    value = preValue;
    int i = 0;
    while ((value & 0xFFFFFF80) != 0L) {
      res[i] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
      i++;
    }
    res[i] = (byte) (value & 0x7F);
    return res;
  }

  /**
   * read an unsigned var int in stream and transform it to int format.
   *
   * @param in stream to read an unsigned var int
   * @return integer value
   * @throws IOException exception in IO
   */
  public static int readUnsignedVarInt(InputStream in) throws IOException {
    int value = 0;
    int i = 0;
    int b = in.read();
    while (b != -1 && (b & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      b = in.read();
    }
    return value | (b << i);
  }

  public static int readVarInt(InputStream in) throws IOException {
    int value = readUnsignedVarInt(in);
    int x = value >>> 1;
    if ((value & 1) != 0) {
      x = ~x;
    }
    return x;
  }

  /**
   * read an unsigned var int in stream and transform it to int format.
   *
   * @param buffer stream to read an unsigned var int
   * @return integer value
   */
  public static int readUnsignedVarInt(ByteBuffer buffer) {
    int value = 0;
    int i = 0;
    int b = 0;
    while (buffer.hasRemaining() && ((b = buffer.get()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
    }
    return value | (b << i);
  }

  public static int readVarInt(ByteBuffer buffer) {
    int value = readUnsignedVarInt(buffer);
    int x = value >>> 1;
    if ((value & 1) != 0) {
      x = ~x;
    }
    return x;
  }

  /**
   * write a value to stream using unsigned var int format. for example, int 123456789 has its
   * binary format 00000111-01011011-11001101-00010101 (if we omit the first 5 0, then it is
   * 111010-1101111-0011010-0010101), function writeUnsignedVarInt will split every seven bits and
   * write them to stream from low bit to high bit like: 1-0010101 1-0011010 1-1101111 0-0111010 1
   * represents has next byte to write, 0 represents number end.
   *
   * @param value value to write into stream
   * @param out output stream
   * @return the number of bytes that the value consume.
   */
  public static int writeUnsignedVarInt(int value, ByteArrayOutputStream out) {
    int position = 1;
    while ((value & 0xFFFFFF80) != 0L) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
      position++;
    }
    out.write(value & 0x7F);
    return position;
  }

  public static int writeVarInt(int value, ByteArrayOutputStream out) {
    int uValue = value << 1;
    if (value < 0) {
      uValue = ~uValue;
    }
    return writeUnsignedVarInt(uValue, out);
  }

  public static int writeUnsignedVarInt(int value, OutputStream out) throws IOException {
    int position = 1;
    while ((value & 0xFFFFFF80) != 0L) {
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
      position++;
    }
    out.write(value & 0x7F);
    return position;
  }

  public static int writeVarInt(int value, OutputStream out) throws IOException {
    int uValue = value << 1;
    if (value < 0) {
      uValue = ~uValue;
    }
    return writeUnsignedVarInt(uValue, out);
  }

  /**
   * write a value to stream using unsigned var int format. for example, int 123456789 has its
   * binary format 111010-1101111-0011010-0010101, function writeUnsignedVarInt will split every
   * seven bits and write them to stream from low bit to high bit like: 1-0010101 1-0011010
   * 1-1101111 0-0111010 1 represents has next byte to write, 0 represents number end.
   *
   * @param value value to write into stream
   * @param buffer where to store the result. buffer.remaining() needs to >= 32. Notice: (1) this
   *     function does not check buffer's remaining(). (2) the position will be updated.
   * @return the number of bytes that the value consume.
   * @throws IOException exception in IO
   */
  public static int writeUnsignedVarInt(int value, ByteBuffer buffer) {
    int position = 1;
    while ((value & 0xFFFFFF80) != 0L) {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
      position++;
    }
    buffer.put((byte) (value & 0x7F));
    return position;
  }

  public static int writeVarInt(int value, ByteBuffer buffer) {
    int uValue = value << 1;
    if (value < 0) {
      uValue = ~uValue;
    }
    return writeUnsignedVarInt(uValue, buffer);
  }

  /**
   * Returns the encoding size in bytes of its input value.
   *
   * @param value the integer to be measured
   * @return the encoding size in bytes of its input value
   */
  public static int varIntSize(int value) {
    int uValue = value << 1;
    if (value < 0) {
      uValue = ~uValue;
    }
    int position = 1;
    while ((uValue & 0xFFFFFF80) != 0L) {
      uValue >>>= 7;
      position++;
    }
    return position;
  }

  /**
   * Returns the encoding size in bytes of its input value.
   *
   * @param value the unsigned integer to be measured
   * @return the encoding size in bytes of its input value
   */
  public static int uVarIntSize(int value) {
    int position = 1;
    while ((value & 0xFFFFFF80) != 0L) {
      value >>>= 7;
      position++;
    }
    return position;
  }

  public static long readLong(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[8];
    int readLen = inputStream.read(bytes);
    if (readLen != 8) {
      throw new IOException(String.format(RETURN_ERROR, LONG_LEN, readLen));
    }
    return bytesToLong(bytes);
  }

  private static long bytesToLong(byte[] byteNum) {
    long num = 0;
    for (int ix = 0; ix < 8; ix++) {
      num <<= 8;
      num |= (byteNum[ix] & 0xff);
    }
    return num;
  }

  public static int write(long n, OutputStream outputStream) throws IOException {
    outputStream.write(longToBytes(n));
    return LONG_LEN;
  }

  private static byte[] longToBytes(long num) {
    byte[] byteNum = new byte[8];
    for (int ix = 0; ix < 8; ix++) {
      byteNum[8 - ix - 1] = (byte) ((num >> ix * 8) & 0xFF);
    }
    return byteNum;
  }

  public static int write(int n, OutputStream outputStream) throws IOException {
    outputStream.write(intToBytes(n));
    return INT_LEN;
  }

  private static byte[] intToBytes(int i) {
    return new byte[] {
      (byte) ((i >> 24) & 0xFF),
      (byte) ((i >> 16) & 0xFF),
      (byte) ((i >> 8) & 0xFF),
      (byte) (i & 0xFF)
    };
  }

  public static int readInt(InputStream inputStream) throws IOException {
    byte[] bytes = new byte[INT_LEN];
    int readLen = inputStream.read(bytes);
    if (readLen != INT_LEN) {
      throw new IOException(String.format(RETURN_ERROR, INT_LEN, readLen));
    }
    return bytesToInt(bytes);
  }

  private static int bytesToInt(byte[] bytes) {
    int length = bytes.length;
    long r = 0;
    for (int i = 0; i < length; i++) {
      r += ((bytes[length - 1 - i] & 0xFF) << (i * 8));
    }

    if (r > Integer.MAX_VALUE) {
      throw new RuntimeException("Row count is larger than Integer.MAX_VALUE");
    }

    return (int) r;
  }
}
