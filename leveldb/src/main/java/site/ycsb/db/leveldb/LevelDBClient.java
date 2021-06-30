package site.ycsb.db.leveldb;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import site.ycsb.*;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class LevelDBClient extends DB {

  static final String PROPERTY_LEVELDB_DIR = "leveldb.dir";

  LevelDB db;

  private Map<String, String> deserialize(String content) {
    JsonObject jobj = new Gson().fromJson(content, JsonObject.class);
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : jobj.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getAsString());
    }
    return result;
  }

  private String serialize(Map<String, ByteIterator> values) {
    StringBuffer result = new StringBuffer();
    result.append('{');
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      result.append(MessageFormat.format("\"{}\":\"{}\",", entry.getKey(), entry.getValue().toString()));
    }
    result.deleteCharAt(result.length() - 1);
    result.append('}');
    return result.toString();
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
