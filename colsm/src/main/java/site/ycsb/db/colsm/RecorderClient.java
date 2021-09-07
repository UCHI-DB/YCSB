package site.ycsb.db.colsm;

import site.ycsb.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class RecorderClient extends DB {

  FileOutputStream output;

  CoLSMClient inner;

  protected byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(ISO_8859_1);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  void writeInt(OutputStream o, int value) throws IOException {
    o.write(value);
    o.write(value >> 8);
    o.write(value >> 16);
    o.write(value >> 24);
  }

  public RecorderClient() {
    inner = new CoLSMClient();
  }

  @Override
  public void init() throws DBException {
    inner.init();
    try {
      output = new FileOutputStream("workload_log");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    inner.cleanup();
    try {
      output.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Status s = inner.read(table, key, fields, result);
    if (s == Status.NOT_FOUND) {
      try {
        byte[] notfound = key.getBytes(ISO_8859_1);
        writeInt(output, notfound.length);
        output.write(notfound);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return s;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return inner.scan(table, startkey, recordcount, fields, result);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return inner.update(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      byte[] bytekey = key.getBytes(StandardCharsets.ISO_8859_1);
      byte[] bytevalue = serializeValues(values);
      writeInt(output, bytekey.length);
      output.write(bytekey);
      writeInt(output, bytevalue.length);
      output.write(bytevalue);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return inner.insert(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
