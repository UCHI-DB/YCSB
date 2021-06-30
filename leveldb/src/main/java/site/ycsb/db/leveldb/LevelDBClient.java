package site.ycsb.db.leveldb;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import site.ycsb.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LevelDBClient extends DB {

  static final String PROPERTY_LEVELDB_DIR = "leveldb.dir";

  LevelDB db;

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
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

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
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

  @Override
  public void init() throws DBException {
    super.init();
    String dbDir = getProperties().getProperty(PROPERTY_LEVELDB_DIR);
    try {
      db = new LevelDB(dbDir);
    } catch (LevelDBException e) {
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    try {
      db.close();
    } catch (LevelDBException e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      String jsonContent = db.get(key);
      Map<String, String> content = deserialize(jsonContent);
      for (String field : fields) {
        // TODO What if the key is not found
        result.put(field, new StringByteIterator(content.get(field)));
      }
      return Status.OK;
    } catch (LevelDBException e) {
      return Status.NOT_FOUND;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      db.scanStart(startkey);
      for (int i = 0; i < recordcount; ++i) {
        Entry entry = db.scanNext();
        Map<String, String> composed = deserialize(entry.value);
        HashMap<String, ByteIterator> oneresult = new HashMap<>();
        for (String field : fields) {
          oneresult.put(field, new StringByteIterator(composed.get(field)));
        }
        result.add(oneresult);
      }
      db.scanStop();
      return Status.OK;
    } catch (LevelDBException e) {
      return Status.NOT_FOUND;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      db.put(key, serialize(values));
      return Status.OK;
    } catch (LevelDBException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      db.put(key, serialize(values));
      return Status.OK;
    } catch (LevelDBException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      db.delete(key);
      return Status.OK;
    } catch (LevelDBException e) {
      return Status.ERROR;
    }
  }
}
