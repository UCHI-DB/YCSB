package site.ycsb.db.colsm;

public class LevelDBClient2 extends LevelDBClient {

  @Override
  protected byte[] convert(String input) {
    int value = Integer.valueOf(input);
    byte[] result = new byte[4];
    result[0] = (byte) (value & 0xFF);
    result[1] = (byte) ((value >> 8) & 0xFF);
    result[2] = (byte) ((value >> 16) & 0xFF);
    result[3] = (byte) ((value >> 24) & 0xFF);
    return result;
  }
}
