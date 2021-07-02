package site.ycsb.db.leveldb;

public class Entry {
  public byte[] key;
  public byte[] value;

  public Entry(byte[] k, byte[] v) {
    key = k;
    value = v;
  }
}
