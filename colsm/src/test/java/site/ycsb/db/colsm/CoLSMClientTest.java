package site.ycsb.db.colsm;

import org.junit.jupiter.api.Test;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CoLSMClientTest {

  @Test
  public void testSimpleReadWrite() {
    CoLSMClient client = new CoLSMClient();
    client.getProperties().put(CoLSMClient.PROPERTY_COLSM_DIR, "/tmp/colsmtest" + UUID.randomUUID().toString());

    try {
      client.init();

      Map<String, ByteIterator> values = new HashMap<>();
      values.put("field1", new StringByteIterator("value1"));
      values.put("field2", new StringByteIterator("value2"));

      Status status = client.insert("", "aaa", values);
      assertTrue(status.isOk());
      Map<String, ByteIterator> result = new HashMap<>();
      Set<String> fields = new HashSet<>();
      fields.add("field1");
      status = client.read("", "aaa", fields, result);
      assertTrue(status.isOk());

      ByteIterator ite = result.get("field1");
      assertEquals("value1", ite.toString());

      client.cleanup();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
