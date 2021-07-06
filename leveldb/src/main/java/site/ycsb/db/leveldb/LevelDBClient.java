package site.ycsb.db.leveldb;

import org.apache.commons.lang3.StringUtils;
import site.ycsb.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import static java.nio.charset.StandardCharsets.UTF_8;
import static site.ycsb.db.leveldb.LevelDBStatus.translate;

public class LevelDBClient extends DB {

  static final String PROPERTY_LEVELDB_DIR = "leveldb.dir";

  LevelDB db;

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
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

  protected byte[] convert(String input) {
    return input.getBytes(UTF_8);
  }

  @Override
  public void init() throws DBException {
    super.init();
    String dbDir = getProperties().getProperty(PROPERTY_LEVELDB_DIR);
    if (StringUtils.isEmpty(dbDir)) {
      dbDir = "/tmp/leveldbtestdb";
    }
    db = new LevelDB(dbDir);
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    db.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    byte[][] ref = new byte[1][];
    Status status = translate(db.get(convert(key), ref));
    if (status.isOk()) {
      deserializeValues(ref[0], fields, result);
    }
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    byte[][] values = new byte[recordcount][];
    Status status = translate(db.scan(convert(startkey), recordcount, values));
    if (status.isOk()) {
      for (int i = 0; i < recordcount; ++i) {
        if (values[i] == null) {
          break;
        }
        HashMap<String, ByteIterator> entry = new HashMap<>();
        deserializeValues(values[i], fields, entry);
        result.add(entry);
      }
    }
    return status;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      Status status = translate(db.put(convert(key), serializeValues(values)));
      return status;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      return translate(db.put(convert(key), serializeValues(values)));
    } catch (IOException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return translate(db.delete(convert(key)));
  }
}
