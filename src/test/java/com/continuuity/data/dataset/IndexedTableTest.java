package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.IndexedTable;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Write;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class IndexedTableTest extends DataSetTestBase {

  static IndexedTable table;

  static byte[] idxCol = { 'i', 'd', 'x' };
  static byte[] valCol = { 'v', 'a', 'l' };
  static byte[] keyA = { 'a' };
  static byte[] keyAA = { 'a', 'a' };
  static byte[] keyB = { 'b' };
  static byte[] keyC = { 'c' };
  static byte[] keyD = { 'd' };
  static byte[] valA = { 'a' };
  static byte[] valAA = { 'a', 'a' };
  static byte[] valAB = { 'a', 'b' };
  static byte[] valB = { 'b' };
  static byte[] valBB = { 'b', 'b' };
  static byte[] valC = { 'c' };
  static byte[] valD = { 'd' };
  static byte[] idx1 = { '1' };
  static byte[] idx2 = { '2' };
  static byte[] idx3 = { '3' };

  static byte[][] colIdxVal = { idxCol, valCol };

  @BeforeClass
  public static void configure() throws Exception {
    setupInstantiator(new IndexedTable("tab", idxCol));
    table = instantiator.getDataSet("tab");
  }

  @Test
  public void testIndexedOperations() throws OperationException {

    OperationResult<Map<byte[], byte[]>> result;

    // start a new transaction
    newCollector();
    // add a value c with idx = 1, and b with idx = 2
    table.write(new Write(keyC, colIdxVal, new byte[][] { idx1, valC }));
    table.write(new Write(keyB, colIdxVal, new byte[][] { idx2, valB }));
    // commit the transaction
    executeCollector();
    // read by key c
    result = table.read(new Read(keyC, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][] { idx1, valC });
    // read by key b
    result = table.read(new Read(keyB, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][] { idx2, valB });
    // read by idx 1 -> c
    result = table.readBy(new Read(idx1, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][] { idx1, valC });
    // read by idx 2 -> b
    result = table.readBy(new Read(idx2, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][] { idx2, valB });

    // start a new transaction
    newCollector();
    // add a value a with idx = 1
    table.write(new Write(keyA, colIdxVal, new byte[][] { idx1, valA }));
    // commit the transaction
    executeCollector();
    // read by idx 1 -> a
    result = table.readBy(new Read(idx1, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][]{idx1, valA});

    // start a new transaction
    newCollector();
    // delete value a
    table.delete(new Delete(keyA, colIdxVal));
    // commit the transaction
    executeCollector();
    // read by idx 1 -> c
    result = table.readBy(new Read(idx1, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][]{idx1, valC});

    // start a new transaction
    newCollector();
    // add a value aa with idx 2
    table.write(new Write(keyAA, colIdxVal, new byte[][] { idx2, valAA }));
    // commit the transaction
    executeCollector();
    // read by idx 2 -> aa
    result = table.readBy(new Read(idx2, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][] { idx2, valAA });

    // start a new transaction
    newCollector();
    // swap value for aa to ab
    table.swap(new Swap(keyAA, valCol, valAA, valAB));
    // commit the transaction
    executeCollector();
    // read by idx 2 -> ab
    result = table.readBy(new Read(idx2, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][] { idx2, valAB });

    // start a new transaction
    newCollector();
    // swap value for aa to bb
    table.swap(new Swap(keyAA, valCol, valAB, valBB));
    // commit the transaction
    executeCollector();
    // read by idx 2 -> bb (value of key aa)
    result = table.readBy(new Read(idx2, colIdxVal));
    TableTest.verifyColumns(result, colIdxVal, new byte[][]{idx2, valBB});

    // start a new transaction
    newCollector();
    // swap value for aa to null
    table.swap(new Swap(keyAA, valCol, valBB, null));
    // commit the transaction
    executeCollector();
    // read by idx 2 -> null (value of b)
    result = table.readBy(new Read(idx2, colIdxVal));
    TableTest.verifyColumn(result, idxCol, idx2);

    // start a new transaction
    newCollector();
    // swap idx for c to 3
    table.swap(new Swap(keyC, idxCol, idx1, idx3));
    // commit the transaction
    executeCollector();
    // read by idx 1 -> null (no row has that any more)
    result = table.readBy(new Read(idx1, colIdxVal));
    TableTest.verifyNull(result, idx2);
    // read by idx 3 > c
    result = table.readBy(new Read(idx3, valCol));
    TableTest.verifyColumn(result, valCol, valC);
  }

}
