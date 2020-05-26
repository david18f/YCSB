package site.ycsb.db.cryptohbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.safeclient.secureTable.CryptoTable;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;
import site.ycsb.*;
import site.ycsb.measurements.Measurements;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static site.ycsb.Utils.splitField;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;
import static site.ycsb.workloads.CryptoWorkload.removeTableProperty;
import static site.ycsb.workloads.CryptoWorkload.schemafileproperty;

/*TODO safe: update comments*/
/**
 * HBase 2 client for YCSB framework.
 *
 * Intended for use with HBase's shaded client.
 */
public class HBaseClient2 extends site.ycsb.DB {
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  private Configuration config = HBaseConfiguration.create();

  private boolean debug = false;

  private String tableName = "";

  /**
   * A Cluster Connection instance that is shared by all running ycsb threads.
   * Needs to be initialized late so we pick up command-line configs if any.
   * To ensure one instance only in a multi-threaded context, guard access
   * with a 'lock' object.
   * @See #CONNECTION_LOCK.
   */
  private static Connection connection = null;

  // Depending on the value of clientSideBuffering, either bufferedMutator
  // (clientSideBuffering) or currentTable (!clientSideBuffering) will be used.
  private CryptoTable currentTable = null;
  private BufferedMutator bufferedMutator = null;

  /**
   * Durability to use for puts and deletes.
   */
  private Durability durability = Durability.USE_DEFAULT;

  /** Whether or not a page filter should be used to limit scan length. */
  private boolean usePageFilter = true;

  /**
   * If true, buffer mutations on the client. This is the default behavior for
   * HBaseClient. For measuring insert/update/delete latencies, client side
   * buffering should be disabled.
   */
  private boolean clientSideBuffering = false;
  private long writeBufferSize = 1024 * 1024 * 12;

  private String schemaFile;
  private TableSchema tableSchema;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // TODO safe: add conf.xml and key.txt?
    //    Inject the schema file path to the Configuration object
    config.addResource("conf.xml");
    config.set("schema", schemafileproperty);

    if ("true"
        .equals(getProperties().getProperty("clientbuffering", "false"))) {
      this.clientSideBuffering = true;
    }
    if (getProperties().containsKey("writebuffersize")) {
      writeBufferSize =
          Long.parseLong(getProperties().getProperty("writebuffersize"));
    }

    if (getProperties().getProperty("durability") != null) {
      this.durability =
          Durability.valueOf(getProperties().getProperty("durability"));
    }

    if ("kerberos".equalsIgnoreCase(config.get("hbase.security.authentication"))) {
      config.set("hadoop.security.authentication", "Kerberos");
      UserGroupInformation.setConfiguration(config);
    }

    if ((getProperties().getProperty("principal")!=null)
        && (getProperties().getProperty("keytab")!=null)) {
      try {
        UserGroupInformation.loginUserFromKeytab(getProperties().getProperty("principal"),
            getProperties().getProperty("keytab"));
      } catch (IOException e) {
        System.err.println("Keytab file is not readable or not found");
        throw new DBException(e);
      }
    }

    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }

    if ("false"
        .equals(getProperties().getProperty("hbase.usepagefilter", "true"))) {
      usePageFilter = false;
    }

    String table = "usertable";

    this.schemaFile = schemafileproperty;
    this.tableSchema = new DatabaseSchema(this.schemaFile).getTableSchema(table);

    try {
      THREAD_COUNT.getAndIncrement();
      synchronized (THREAD_COUNT) {
        if (connection == null) {
          // Initialize if not set up already.
          connection = ConnectionFactory.createConnection(config);

          if (!verifyTable(table).isOk())
            throw new DBException();
        }
      }
    } catch (java.io.IOException e) {
      throw new DBException(e);
    }

  }

  public boolean checkIfTableExists(String tablename) {
    try {
      return connection.getAdmin().tableExists(TableName.valueOf(tablename));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  public void createTable(TableSchema schema) {
    try {
      TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(schema.getTablename()));
      List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
      for (Family fam : schema.getColumnFamilies()) {
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(fam.getFamilyName()));
        columnFamilyDescriptors.add(columnFamilyDescriptorBuilder.build());
      }
      tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptors);
      connection.getAdmin().createTable(tableDescriptorBuilder.build());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void removeTable(String tablename) {
    try {
      Admin admin = connection.getAdmin();
      admin.disableTable(TableName.valueOf(tablename));
      admin.deleteTable(TableName.valueOf(tablename));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    // Get the measurements instance as this is the only client that should
    // count clean up time like an update if client-side buffering is
    // enabled.
    Measurements measurements = Measurements.getMeasurements();
    try {
      long st = System.nanoTime();
      if (bufferedMutator != null) {
        bufferedMutator.close();
      }
      if (currentTable != null) {
        currentTable.close();
      }
      long en = System.nanoTime();
      final String type = clientSideBuffering ? "UPDATE" : "CLEANUP";
      measurements.measure(type, (int) ((en - st) / 1000));
      int threadCount = THREAD_COUNT.decrementAndGet();
      if (threadCount <= 0) {
        // Means we are done so ok to shut down the Connection.
        synchronized (THREAD_COUNT) {
          if (connection != null) {
            connection.close();
            connection = null;
          }
        }
      }
      if(removeTableProperty.equals("true")) {
        removeTable(tableName);
      }
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public void getHTable(String table) throws IOException {
    final TableName tName = TableName.valueOf(table);
    if(!checkIfTableExists(table)) {
      System.out.println("Table does not exists. Creating table ...");
      createTable(this.tableSchema);
    }
    this.currentTable = new CryptoTable(config, table, this.tableSchema);
    if (clientSideBuffering) {
      final BufferedMutatorParams p = new BufferedMutatorParams(tName);
      p.writeBufferSize(writeBufferSize);
      this.bufferedMutator = connection.getBufferedMutator(p);
    }
  }

  public Status verifyTable(String table) {
    Status verifyStatus = Status.OK;
    //if this is a "new" tableName, init HTable object.  Else, use existing one
    if (!this.tableName.equals(table)) {
      currentTable = null;
      try {
        getHTable(table);
        this.tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase tableName: " + e);
        verifyStatus = Status.ERROR;
      }
    }

    return verifyStatus;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Status statusResult = verifyTable(table);

    if (statusResult == Status.OK) {
      Result r;
      try {
        if (debug) {
          System.out.println("Trying to read: " + key);
        }
        Get g = new Get(Bytes.toBytes(key));
        if (fields == null) {
          for (Family f : this.tableSchema.getColumnFamilies()) {
            g.addFamily(f.getFamilyName().getBytes());
          }
        } else {
          for (String field : fields) {
            String[] temp_fields = splitField(field);
            g.addColumn(Bytes.toBytes(temp_fields[0]), Bytes.toBytes(temp_fields[1]));
          }
        }
        // Perform the get operation in the Table interface
        r = currentTable.get(g);
//      } catch (IOException e) {
//        if (debug) {
//          System.err.println("Error doing get: " + e);
//        }
//        return Status.ERROR;
      } catch (ConcurrentModificationException e) {
        // do nothing for now...need to understand HBase concurrency model better
        return Status.ERROR;
      }

      if (r.isEmpty()) {
        return Status.NOT_FOUND;
      }

//      String stringResult = buildStringResult(r, "Get");
//          System.out.println(stringResult);

      while (r.advance()) {
        final Cell c = r.current();
        result.put(Bytes.toString(CellUtil.cloneQualifier(c)),
            new ByteArrayByteIterator(CellUtil.cloneValue(c)));
        if (debug) {
          System.out.println(
              "Result for field: " + Bytes.toString(CellUtil.cloneQualifier(c))
                  + " is: " + Bytes.toString(CellUtil.cloneValue(c)));
        }
      }
    }

    return statusResult;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Status statusResult = verifyTable(table);

    if(statusResult == Status.OK) {
//      Scan s = new Scan().withStartRow(Bytes.toBytes(startkey));
      Scan s = new Scan();
      // HBase has no record limit. Here, assume recordcount is small enough to
      // bring back in one call.
//       We get back recordcount records
//      s.setCaching(recordcount);

      // add specified fields or else all fields
//      if (fields == null) {
//        for (Family f : this.tableSchema.getColumnFamilies()) {
//          s.addFamily(f.getFamilyName().getBytes());
//        }
//      } else {
//        for (String field : fields) {
//          String[] temp_fields = splitField(field);
//          s.addColumn(Bytes.toBytes(temp_fields[0]), Bytes.toBytes(temp_fields[1]));
//        }
//      }
//      s.addColumn(Bytes.toBytes("Patient"), Bytes.toBytes("Patient ID"));

      // get results
      ResultScanner scanner = null;
      try {
        scanner = currentTable.getScanner(s);
        int numResults = 0;
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
          // get row key
          String key = Bytes.toString(rr.getRow());

          if (debug) {
            System.out.println("Got scan result for key: " + key);
          }

//          String stringResult = buildStringResult(rr, "Scan");
//            System.out.println(stringResult);

          HashMap<String, ByteIterator> rowResult =
              new HashMap<String, ByteIterator>();

          while (rr.advance()) {
            final Cell cell = rr.current();
            rowResult.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
                new ByteArrayByteIterator(CellUtil.cloneValue(cell)));
          }

          // add rowResult to result vector
          result.add(rowResult);
          numResults++;

          // PageFilter does not guarantee that the number of results is <=
          // pageSize, so this
          // break is required.
          if (numResults >= recordcount) {// if hit recordcount, bail out
            break;
          }
        } // done with row
      } catch (IOException e) {
        if (debug) {
          System.out.println("Error in getting/parsing scan result: " + e);
        }
        return Status.ERROR;
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    }

    return statusResult;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    Status statusResult = verifyTable(table);

    if(statusResult == Status.OK) {
      if (debug) {
        System.out.println("Setting up put for key: " + key);
      }
      Put p = new Put(Bytes.toBytes(key));
      p.setDurability(durability);
      for (Family f : this.tableSchema.getColumnFamilies()) {
        for (Qualifier q : f.getQualifiers()) {
          if(values.containsKey(new String(f.getFamilyName().getBytes()) +":"+ new String(q.getName().getBytes()))) {
            p.addColumn(f.getFamilyName().getBytes(), q.getName().getBytes(), values.get(new String(f.getFamilyName().getBytes()) +":"+ new String(q.getName().getBytes())).toArray());
          }
        }
      }

      try {
        if (clientSideBuffering) {
          // removed Preconditions.checkNotNull, which throws NPE, in favor of NPE on next line
          bufferedMutator.mutate(p);
        } else {
          currentTable.put(p);
        }
      } catch (IOException e) {
        if (debug) {
          System.err.println("Error doing put: " + e);
        }
        return Status.ERROR;
      } catch (ConcurrentModificationException e) {
        // do nothing for now...hope this is rare
        return Status.ERROR;
      }
    }

    return statusResult;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    Status statusResult = verifyTable(table);

    if(statusResult == Status.OK) {
      if (debug) {
        System.out.println("Doing delete for key: " + key);
      }

      final Delete d = new Delete(Bytes.toBytes(key));
      d.setDurability(durability);
      try {
        if (clientSideBuffering) {
          // removed Preconditions.checkNotNull, which throws NPE, in favor of NPE on next line
          bufferedMutator.mutate(d);
        } else {
          currentTable.delete(d);
        }
      } catch (IOException e) {
        if (debug) {
          System.err.println("Error doing delete: " + e);
        }
        return Status.ERROR;
      }
    }

    return statusResult;
  }

  @Override
  public Status filter(String table, String startkey, int recordcount, String filtertype, Object filterproperties, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Status statusResult = verifyTable(table);

    if(statusResult == Status.OK) {

      Scan s = new Scan().withStartRow(Bytes.toBytes(startkey));
      s.setCaching(recordcount);
      Filter filter = whichFilter(filtertype, (String[]) filterproperties);
//      Filter filter = new RowFilter(CompareOperator.GREATER,
//          new BinaryComparator(Bytes.toBytes("coisa")));
      s.setFilter(filter);

      //add specified fields or else all fields
      if (fields == null) {
        for (Family f : this.tableSchema.getColumnFamilies()) {
          s.addFamily(f.getFamilyName().getBytes());
        }
      } else {
        for (String field : fields) {
          String[] temp_fields = splitField(field);
          s.addColumn(Bytes.toBytes(temp_fields[0]), Bytes.toBytes(temp_fields[1]));
        }
      }

      //get results
      try (ResultScanner scanner = currentTable.getScanner(s)) {
        int numResults = 0;
        int total = 0;
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
          //get row key
          String key = Bytes.toString(rr.getRow());

          if (key != null) {

            if (debug) {
              System.out.println("Got filter scan result for key: " + key);
            }

//            String stringResult = buildStringResult(rr, "Filter");
//            System.out.println(stringResult);

            HashMap<String, ByteIterator> rowResult = new HashMap<>();

            while (rr.advance()) {
              final Cell cell = rr.current();
              rowResult.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
                  new ByteArrayByteIterator(CellUtil.cloneValue(cell)));
            }

            result.add(rowResult);
            numResults++;

            if (numResults >= recordcount) {
              break;
            }

          } //done with row
          total++;
        }
//      String[] fp1 = (String[]) filterproperties;
//      System.out.println("Filter Scan[" + startkey + ", " + fp1[0] + ", " + fp1[1] + ", " + numResults + ", " + total + "]");
//      System.out.println("Filter Scan NumResults: " + numResults + " - RecordCount: " + recordcount);

      } catch (IOException e) {
        if (debug) {
          System.out.println("Error in getting/parsing scan result: " + e);
        }
        return Status.ERROR;
      }
    }
    return statusResult;
  }

  public Filter whichFilter(String filtertype, String[] properties) {
    if (filtertype.equals("singlecolumnvaluefilter")) {
      return new SingleColumnValueFilter(
          Bytes.toBytes("Family"),
          Bytes.toBytes("Qualifier"),
          CompareOperator.GREATER,
          new BinaryComparator(Bytes.toBytes("coisa"))
      );
    } else {
      return new RowFilter(CompareOperator.GREATER,
          new BinaryComparator(Bytes.toBytes("coisa")));
    }

//    Filter filter;
//    switch (filtertype) {
//      case "singlecolumnvaluefilter":
//        filter = new SingleColumnValueFilter(
//            Bytes.toBytes(properties[2]),
//            Bytes.toBytes(properties[3]),
//            whichOperator(properties[0]),
//            new BinaryComparator(Bytes.toBytes(properties[1]))
//        );
//        break;
//      case "rowfilter":
//      default:
//        byte[] longKey = convertStringToLong(properties[1]);
//        filter = new RowFilter(
//            whichOperator(properties[0]),
//            new BinaryComparator(longKey)
//        );
////        filter = new RowFilter(
////          whichOperator(properties[0]),
////          new BinaryComparator(Bytes.toBytes(properties[1]))
////        );
//        break;
//    }
//    return filter;
  }

  public CompareOperator whichOperator(String operator) {
    CompareOperator op;
    switch (operator) {
      case "GREATER":
        op = CompareOperator.GREATER;
        break;
      case "GREATER_OR_EQUAL":
        op = CompareOperator.GREATER_OR_EQUAL;
        break;
      case "EQUAL":
        op = CompareOperator.EQUAL;
        break;
      case "LESS":
        op = CompareOperator.LESS;
        break;
      case "LESS_OR_EQUAL":
        op = CompareOperator.LESS_OR_EQUAL;
        break;
      default:
        op = CompareOperator.NO_OP;
        break;
    }
    return op;
  }

  public String buildStringResult(Result rr, String operation) {
//    StringBuilder sb = new StringBuilder();
//    sb.append("["+operation+" : ").append(Utils.removePadding(new String(rr.getRow()))).append("] - ");
//    for (Family f : ct.cryptoProperties.tableSchema.getColumnFamilies()) {
//      for (Qualifier q : f.getQualifiers()) {
//        byte[] value = rr.getValue(Bytes.toBytes(f.getFamilyName()), Bytes.toBytes(q.getName()));
//        if (value != null) {
//          sb.append("[(" + f.getFamilyName() + "," + q.getName() + ") - " + Utils.removePadding(new String(value)) + "] - ");
//        }
//      }
//    }
    StringBuilder sb = new StringBuilder();
//    sb.append("["+operation+" : ").append(convertByteArrayToLong(rr.getRow())).append("] - ");
    sb.append("["+operation+" : ").append(convertByteArrayToInt(rr.getRow())).append("] - ");
    for (Family f : this.tableSchema.getColumnFamilies()) {
      for (Qualifier q : f.getQualifiers()) {
//        byte[] value = rr.getValue(f.getFamilyName().array(), q.getName().array());
        byte[] value = rr.getValue(f.getFamilyName().getBytes(), q.getName().getBytes());

        if (value != null) {
          sb.append("[(" + new String(f.getFamilyName().getBytes()) + "," + new String(q.getName().getBytes()) + ") - " + Utils.removePadding(new String(value)) + "] - ");
        }
      }
    }
    return sb.toString();
  }

  public byte[] convertStringToLong(String value) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    long x = Long.parseLong(value);
    buffer.putLong(x);
    return buffer.array();
//    byte[] longArray = ByteBuffer.allocate(8).putLong(Long.parseLong(value)).array();
//    return longArray;
  }

  public long convertByteArrayToLong(byte[] value) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.put(value);
    buffer.flip();//need flip
    return buffer.getLong();
//    long val = ByteBuffer.wrap(value).getLong();
//    return val;
  }

  public byte[] convertIntToByteArray(String value) {
    return pt.uminho.haslab.cryptoenv.Utils.intArrayToByteArray(pt.uminho.haslab.cryptoenv.Utils.integerToIntArray(Integer.parseInt(value), 10));
  }

  public int convertByteArrayToInt(byte[] value) {
    return pt.uminho.haslab.cryptoenv.Utils.intArrayToInteger(pt.uminho.haslab.cryptoenv.Utils.byteArrayToIntArray(value),10);
  }

  // Only non-private for testing.
  void setConfiguration(final Configuration newConfig) {
    this.config = newConfig;
  }
}
