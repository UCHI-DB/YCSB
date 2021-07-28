package site.ycsb.db.colsm;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class RecorderClient extends CoLSMClient {

  FileOutputStream output;

  void writeInt(OutputStream o, int value) throws IOException {
    o.write(value);
    o.write(value >> 8);
    o.write(value >> 16);
    o.write(value >> 24);
  }

  @Override
  public void init() throws DBException {
    super.init();
    try {
      output = new FileOutputStream("workload_log");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    try {
      output.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      byte[] bytekey = key.getBytes(StandardCharsets.ISO_8859_1);
      byte[] bytevalue = serializeValues(values);
      writeInt(output,bytekey.length);
      output.write(bytekey);
      writeInt(output,bytevalue.length);
      output.write(bytevalue);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
