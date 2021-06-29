package site.ycsb.db.leveldb;

import java.util.logging.Level;

public class LevelDB {

  public LevelDB(String dir) throws LevelDBException {
    init(dir);
  }

  public native void init(String dir) throws LevelDBException;

  public native void close() throws LevelDBException;

  public native void put(String key, String value) throws LevelDBException;

  public native void delete(String key) throws LevelDBException;

  public native String get(String key) throws LevelDBException;

  public native void scanStart(String key) throws LevelDBException;

  public native Entry scanNext() throws LevelDBException;

  public native void scanStop() throws LevelDBException;
}
