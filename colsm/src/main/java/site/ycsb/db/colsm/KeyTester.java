package site.ycsb.db.colsm;

import site.ycsb.workloads.CoreWorkload;

import java.nio.charset.StandardCharsets;

public class KeyTester {

  public static void main(String[] args) {
    byte[] b = new byte[256];

    for (int i = 0; i < 256; i ++) {
      b[i] = (byte) (i - 128);
    }
    byte[] transformed = new String(b, StandardCharsets.ISO_8859_1).getBytes(StandardCharsets.ISO_8859_1);

    for (int i = 0; i < b.length; i ++) {
      if (b[i] != transformed[i]) {
        System.out.println("Wrong : " + i);
      }
    }

    CoLSM colsm = new CoLSM("");

    for(int i = Integer.MIN_VALUE ; i < Integer.MAX_VALUE;++i) {
      String key = CoreWorkload.buildKeyName(i, 0, false);
      byte[] keybytes = key.getBytes(StandardCharsets.ISO_8859_1);
      int result = colsm.put(keybytes,keybytes);
      if(i!=result) {
        System.out.println(i);
      }
    }
  }
}
