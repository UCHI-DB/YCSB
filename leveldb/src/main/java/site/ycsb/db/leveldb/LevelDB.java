package site.ycsb.db.leveldb;

import java.nio.charset.StandardCharsets;

public class LevelDB {

  private long db_instance;
  private long db_comparator;

  public LevelDB(String dir) throws LevelDBException {
    init(dir.getBytes(StandardCharsets.UTF_8));
  }

  public native void init(byte[] dir) throws LevelDBException;

  public native void close() throws LevelDBException;

  public native void put(byte[] key, byte[] value) throws LevelDBException;

  public native void delete(byte[] key) throws LevelDBException;

  public native byte[] get(byte[] key) throws LevelDBException;

  public native void scanStart(byte[] key) throws LevelDBException;

  public native byte[] scanNext() throws LevelDBException;

  public native void scanStop() throws LevelDBException;
}
