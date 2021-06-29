package site.ycsb.db.leveldb;

public class LevelDBException extends Exception {

  int error;

  public LevelDBException(int error) {
    this.error = error;
  }
}
