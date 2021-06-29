package site.ycsb.db.leveldb;

public class Entry {
  public String key;
  public String value;

  public Entry(String k, String v) {
    key = k;
    value = v;
  }
}
