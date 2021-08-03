package site.ycsb.db.colsm;

import java.nio.charset.StandardCharsets;

public class CoLSM {

  private long db;
  private long options;

  static {
    // Load Native Library (C++); calls JNI_OnLoad()
    System.loadLibrary("colsmjni");
  }

  public CoLSM(String dir) {
    init(dir.getBytes(StandardCharsets.UTF_8));
  }

  public native void init(byte[] dir);

  public native void close();

  public native int put(byte[] key, byte[] value);

  public native int delete(byte[] key);

  public native int get(byte[] key, byte[][] value);

  public native int scan(byte[] key, int limit, byte[][] values);

}
