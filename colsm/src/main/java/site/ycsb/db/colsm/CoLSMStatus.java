package site.ycsb.db.colsm;

import site.ycsb.Status;

public enum CoLSMStatus {
  kOk,
  kNotFound,
  kCorruption,
  kNotSupported,
  kInvalidArgument,
  kIOError;

  static Status translate(int lstatus) {
    switch (lstatus) {
      case 0:
        return Status.OK;
      case 1:
        return Status.NOT_FOUND;
      case 2:
        return Status.ERROR;
      case 3:
        return Status.NOT_IMPLEMENTED;
      case 4:
        return Status.BAD_REQUEST;
      case 5:
        return Status.ERROR;
      default:
        return Status.ERROR;
    }
  }
}
