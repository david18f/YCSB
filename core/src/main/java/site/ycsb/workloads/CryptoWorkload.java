package site.ycsb.workloads;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.testingutils.ValuesGenerator;
import site.ycsb.*;
import site.ycsb.generator.*;
import site.ycsb.measurements.Measurements;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static site.ycsb.Utils.splitField;

/**
 * Created by rgmacedo on 3/7/17.
 * Updated by david18f on 23/03/2020.
 */
public class CryptoWorkload extends CoreWorkload {

  /**
   * Start Row Properties for Scan and filter operations
   */
  public static final String START_ROW_PROPERTY_DEFAULT = null;
  public static final String START_ROW_PROPERTY = "startrow";
  public static String startrow;

  /**
   * Scan Length Properties for Scan and filter operations
   */
  public static final String SCAN_LENGTH_PROPERTY_DEFAULT = String.valueOf(Integer.MAX_VALUE);
  public static final String SCAN_LENGTH_PROPERTY = "scanlengthproperty";
  public static String scanlengthproperty;

  /**
   * Seed Properties to build and generate pseudo-random values
   */
  public static final String SEED_PROPERTY_DEFAULT = null;
  public static final String SEED_PROPERTY = "seed";
  public static String seed;

  /**
   * The name of the property for the the distribution of requests across the keyspace. Options are
   * "uniform", "zipfian" and "latest"
   */
  public final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
  public final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "sequential";

  /**
   * DateGenerator Properties
   */
  public static final String INITIAL_YEAR_PROPERTY_DEFAULT = "1900";
  public static final String INITIAL_YEAR_PROPERTY = "initialyear";
  public static int initialyear;

  public static final String FINAL_YEAR_PROPERTY_DEFAULT = "2016";
  public static final String FINAL_YEAR_PROPERTY = "finalyear";
  public static int finalyear;

  /**
   * Full table scan/filter property
   */
  public static final String FULL_TABLE_SEARCH_PROPERTY_DEFAULT = "false";
  public static final String FULL_TABLE_SEARCH_PROPERTY = "fulltablesearch";
  public static boolean fulltablesearch;

  /**
   * Filter Properties: Filter Type
   */
  public static final String FILTER_TYPE_PROPERTY_DEFAULT = "rowfilter";
  public static final String FILTER_TYPE_PROPERTY = "filtertype";
  public static String filtertype;

  public static final String FAMILY_FILTER_PROPERTY_DEFAULT = null;
  public static final String FAMILY_FILTER_PROPERTY = "familyfilterproperty";
  public static String familyfilter;

  public static final String QUALIFIER_FILTER_PROPERTY_DEFAULT = null;
  public static final String QUALIFIER_FILTER_PROPERTY = "qualifierfilterproperty";
  public static String qualifierfilter;

  /**
   * Filter Properties: Filter Proportion
   */
  public static final String FILTER_PROPORTION_PROPERTY_DEFAULT = "0.0";
  public static final String FILTER_PROPORTION_PROPERTY = "filterproportion";
  public static String filterproportion;

  /**
   * Filter Properties: Compare Value
   */
  public static final String COMPARE_VALUE_PROPERTY_DEFAULT = null;
  public static final String COMPARE_VALUE_PROPERTY = "comparevalue";
  public static String comparevalue;

  /**
   * Filter Properties: Filter Operator Proportion
   */
  public static final String GREAT_PROPORTION_PROPERTY = "greatproportion";
  public static final String GREAT_PROPORTION_PROPERTY_DEFAULT = "0.5";

  public static final String GREAT_OR_EQUAL_PROPORTION_PROPERTY = "greatorequalproportion";
  public static final String GREAT_OR_EQUAL_PROPORTION_PROPERTY_DEFAULT = "0";

  public static final String EQUAL_PROPORTION_PROPERTY = "equalproportion";
  public static final String EQUAL_PROPORTION_PROPERTY_DEFAULT = "0.25";

  public static final String LESS_PROPORTION_PROPERTY = "lessproportion";
  public static final String LESS_PROPORTION_PROPERTY_DEFAULT = "0.25";

  public static final String LESS_OR_EQUAL_PROPORTION_PROPERTY = "lessorequalproportion";
  public static final String LESS_OR_EQUAL_PROPORTION_PROPERTY_DEFAULT = "0";

  public static final String SCHEMA_FILE_PROPERTY_DEFAULT = null;
  public static final String SCHEMA_FILE_PROPERTY = "schemafileproperty";
  public static String schemafileproperty;

  public static final String REMOVE_TABLE_PROPERTY_DEFAULT = "false";
  public static final String REMOVE_TABLE_PROPERTY = "removetableproperty";
  public static String removeTableProperty;

  // TODO safe: check this path
  public static final String CONFIGURATION_PATH_PROPERTY_DEFAULT = "/home/gsd/YCSB/schemas/macrotests/appointments/PLT-schema.xml";
  public static final String CONFIGURATION_PATH_PROPERTY = "configurationpathproperty";
  public static String configurationPathProperty;

  static final Log LOG = LogFactory.getLog(CryptoWorkload.class.getName());

  //  qualifiers of database
  private List<String> fieldnames;
  //  generators of each fieldname
  private List<String> generators;
  //  format size of each fieldname
  private List<Integer> formatsizes;
  private Integer keyFormatSize;
  private boolean dataintegrity;
  private Measurements measurements = Measurements.getMeasurements();

  NumberGenerator fieldgenerator;
  DiscreteGenerator compareOperationChooser;
  DateGenerator dateGenerator;
  String requestdistrib;
  TableSchema tableSchema;

  ConcurrentHashMap<String, String> patientsMainIdentification;
  Object[] patientsArray;
  Random patientsRandom;
  boolean firstTimeSingleColumnValue;

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
//    Read encrypted schema file path
    schemafileproperty = p.getProperty(SCHEMA_FILE_PROPERTY, SCHEMA_FILE_PROPERTY_DEFAULT);
    System.out.println("Schema File Property: "+schemafileproperty);

//    read the tablename to perform the operations
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

//    read schema to set up the qualifiers
    readParsedSchema(schemafileproperty);

//    total of qualifiers to insert in each row
    fieldcount = fieldnames.size();
//    System.out.println("FielCount: "+fieldcount);

    fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

    recordcount =
        Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }

//    what distribution should be used to select the records to operate on – uniform, zipfian or latest
    requestdistrib =
        p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    int minscanlength =
        Integer.parseInt(p.getProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_PROPERTY_DEFAULT));
    int maxscanlength =
        Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    String scanlengthdistrib =
        p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    long insertstart =
        Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount=
        Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    // Confirm valid values for insertstart and insertcount in relation to recordcount
    if (recordcount < (insertstart + insertcount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }

//    seed to generate pseudo-random values
    seed = p.getProperty(SEED_PROPERTY, SEED_PROPERTY_DEFAULT);
//    set seed property in Utils class
    if(seed != null) {
      Utils.setSeed(seed);
    }

    zeropadding =
        Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));
//    boolean flag to read all fields
    readallfields = Boolean.parseBoolean(
        p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
//    boolean flag to write in all fields
    writeallfields = Boolean.parseBoolean(
        p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

    dataintegrity = Boolean.parseBoolean(
        p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
    // Confirm that fieldlengthgenerator returns a constant if data
    // integrity check requested.
    if (dataintegrity && !(p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY,
        FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
      System.err.println("Must have constant field size to check data integrity.");
      System.exit(-1);
    }

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      orderedinserts = false;
    } else {
      orderedinserts = true;
    }

    keysequence = new CounterGenerator(insertstart);

    //    verify if will be performed a full table scan/filter operation
    fulltablesearch = p.getProperty(FULL_TABLE_SEARCH_PROPERTY, FULL_TABLE_SEARCH_PROPERTY_DEFAULT).equals("true");

//    start row for scan or filter operations
    if(p.getProperty(START_ROW_PROPERTY, START_ROW_PROPERTY_DEFAULT) != null) {
      startrow = p.getProperty(START_ROW_PROPERTY, START_ROW_PROPERTY_DEFAULT);
//      System.out.println("Startrow: " + startrow);
    }

//    read and choose the total amount of operations to perform in the runtime
    operationchooser = createOperationGenerator(p);

//    filter type to perform
    filtertype = p.getProperty(FILTER_TYPE_PROPERTY, FILTER_TYPE_PROPERTY_DEFAULT);

//    family for singlecolumnvaluefilter operation
    familyfilter = p.getProperty(FAMILY_FILTER_PROPERTY, FAMILY_FILTER_PROPERTY_DEFAULT);

//    qualifier for singlecolumnvaluefilter operation
    qualifierfilter = p.getProperty(QUALIFIER_FILTER_PROPERTY, QUALIFIER_FILTER_PROPERTY_DEFAULT);

//    compare operation chooser for filter operations
    compareOperationChooser = createCompareOperationGenerator(p);

//    compare value to perform the filter operation
//    if(properties.getProperty(COMPARE_VALUE_PROPERTY, COMPARE_VALUE_PROPERTY_DEFAULT) != null) {
//      comparevalue = new String(
//        Utils.addPadding(
//          properties.getProperty(
//            COMPARE_VALUE_PROPERTY,
//            COMPARE_VALUE_PROPERTY_DEFAULT),
//          keyFormatSize).getBytes());
//    }

    comparevalue = p.getProperty(COMPARE_VALUE_PROPERTY, COMPARE_VALUE_PROPERTY_DEFAULT);

    scanlengthproperty = p.getProperty(SCAN_LENGTH_PROPERTY, SCAN_LENGTH_PROPERTY_DEFAULT);

//    Transaction sequence
    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformLongGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the modulus over the
      // number of keys.
      // If the number of keys changes, this would shift the modulus, and we don't want that to
      // change which keys are popular so we'll actually construct the scrambled zipfian generator
      // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
      // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
      // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
      // the keyspace doesn't change from the perspective of the scrambled zipfian generator
      final double insertproportion = Double.parseDouble(
          p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
      int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor

      keychooser = new ScrambledZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys);
    } else if (requestdistrib.compareTo("latest") == 0) {
      keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(insertstart, insertstart + insertcount - 1,
          hotsetfraction, hotopnfraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }

    fieldchooser = new UniformLongGenerator(0, fieldcount - 1);

    if (scanlengthdistrib.compareTo("uniform") == 0) {
      scanlength = new UniformLongGenerator(minscanlength, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      scanlength = new ZipfianGenerator(minscanlength, maxscanlength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
    }

    insertionRetryLimit = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));

    initialyear = Integer.parseInt(p.getProperty(INITIAL_YEAR_PROPERTY, INITIAL_YEAR_PROPERTY_DEFAULT));
    finalyear = Integer.parseInt(p.getProperty(FINAL_YEAR_PROPERTY, FINAL_YEAR_PROPERTY_DEFAULT));

    dateGenerator = new DateGenerator(initialyear, finalyear);

    removeTableProperty = p.getProperty(REMOVE_TABLE_PROPERTY, REMOVE_TABLE_PROPERTY_DEFAULT);

    firstTimeSingleColumnValue = true;
    patientsMainIdentification = new ConcurrentHashMap<>();
    patientsRandom = new Random(Integer.parseInt(seed));

    configurationPathProperty = p.getProperty(CONFIGURATION_PATH_PROPERTY, CONFIGURATION_PATH_PROPERTY_DEFAULT);
    System.out.println("Configuration Path Property: "+configurationPathProperty);
  }

  public void readParsedSchema(String schemafilename) {
    DatabaseSchema schemaParser = new DatabaseSchema(schemafilename);
//    schemaParser.parseDatabaseTables(schemafilename);
    tableSchema = schemaParser.getTableSchema("usertable");
    System.out.println("-> "+tableSchema.getTablename());
    keyFormatSize = tableSchema.getKey().getFormatSize();

    fieldnames = new ArrayList<>();
    generators = new ArrayList<>();
    formatsizes = new ArrayList<>();
    for(Family f : tableSchema.getColumnFamilies()) {
      for(Qualifier q : f.getQualifiers()) {
        fieldnames.add(f.getFamilyName() +":"+ q.getName());
        generators.add(q.getProperties().get("GENERATOR"));
        formatsizes.add(q.getFormatSize());

        fieldcount++;
//        System.out.println("Reading Schema: "+q.getName());
      }
    }
    System.out.println("schema parser - "+schemaParser.printDatabaseSchemas());
  }

  // TODO safe: check if we have to change this
  @Override
  protected String buildKeyName(long keynum) {
//    if (!orderedinserts) {
//      keynum = Utils.hash(keynum);
//    }
//    String value = Long.toString(keynum);
//    int fill = zeropadding - value.length();
//    String prekey = "user";
//    for (int i = 0; i < fill; i++) {
//      prekey += '0';
//    }
//    return prekey + value;

    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }
    return String.valueOf((int)keynum);
  }

  private ByteIterator generateQualifierValue(int index) {
    String generator = this.generators.get(index);
    Integer formatSize = this.formatsizes.get(index);

    String field;

    switch (generator) {
      case "String":
        field = ValuesGenerator.randomString(formatSize);
        break;
      case "Date":
        field = buildDate();
        break;
      case "Integer":
        field = ValuesGenerator.randomNumber(formatSize);
        break;
      default:
        return null;
    }
    return new StringByteIterator(new String(Utils.addPadding(field, formatSize).getBytes()));
  }

  private String buildDate() {
    String date = this.dateGenerator.nextValue();

    return String.valueOf(dateGenerator.dateToTimestamp(date));
  }

  /**
   * Builds a value for a randomly chosen field.
   */
  private HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<String, ByteIterator>();
    String fieldkey = null;
    String fields = null;
    boolean hasField = false;
    while(!hasField) {
      fields = fieldnames.get(fieldchooser.nextValue().intValue());
      fieldkey = splitField(fields)[1];

      if(fieldkey.length() > "_STD".length()) {
        if (!fieldkey.substring(fieldkey.length() - "_STD".length()).equals("_STD")) {
          hasField = true;
        }
      }
      else {
        hasField = true;
      }
    }
    ByteIterator data = generateQualifierValue(fieldnames.indexOf(fields));
    value.put(fields, data);
    return value;
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
    for (String fieldkey : fieldnames) {
      ByteIterator data;
      data = generateQualifierValue(fieldnames.indexOf(fieldkey));
      values.put(fieldkey, data);
    }
    return values;
  }

  /**
   * Build a deterministic value given the key information.
   */
  private String buildDeterministicValue(String key, String fieldkey) {
    int size = fieldlengthgenerator.nextValue().intValue();
    StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  /**
   * Results are reported in the first three buckets of the histogram under
   * the label "VERIFY".
   * Bucket 0 means the expected data was returned.
   * Bucket 1 means incorrect data was returned.
   * Bucket 2 means null data was returned when some data was expected.
   */
  @Override
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();
    if (!cells.isEmpty()) {
      for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
        if (!entry.getValue().toString().equals(buildDeterministicValue(key, entry.getKey()))) {
          verifyStatus = Status.UNEXPECTED_STATE;
          break;
        }
      }
    } else {
      // This assumes that null data is never valid
      verifyStatus = Status.ERROR;
    }
    long endTime = System.nanoTime();
    measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    measurements.reportStatus("VERIFY", verifyStatus);
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    switch (operation) {
      case "READ":
        doTransactionRead(db);
        break;
      case "UPDATE":
        doTransactionUpdate(db);
        break;
      case "INSERT":
        doTransactionInsert(db);
        break;
      case "SCAN":
        doTransactionScan(db);
        break;
      case "FILTER":
        doTransactionFilter(db);
        break;
      case "READMODIFYWRITE":
        doTransactionReadModifyWrite(db);
        break;
      default:
        break;
    }

    return true;
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads,
   * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  public void doTransactionInsert(DB db) {
    // choose the next key
    long keynum = transactioninsertkeysequence.nextValue();

    try {
      String dbkey = buildKeyName(keynum);

      HashMap<String, ByteIterator> values = buildValues(dbkey);
      db.insert(table, dbkey, values);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }

  public void doTransactionRead(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<>(fieldnames);
    }

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
  }

  public void doTransactionUpdate(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    db.update(table, keyname, values);
  }

  public void doTransactionReadModifyWrite(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else {
      fields = new HashSet<>(fieldnames);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    // do the transaction

    HashMap<String, ByteIterator> cells = new HashMap<>();

    long ist = measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    db.read(table, keyname, fields, cells);

    db.update(table, keyname, values);

    long en = System.nanoTime();

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }

    measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
    measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
  }

  public void doTransactionScan(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String startkeyname = paddingToStartKeyName(keynum);

    int len = Integer.parseInt(scanlengthproperty);

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<>();
      fields.add(fieldname);
    } else {
      fields = new HashSet<>(fieldnames);
    }

    LOG.debug("Scan performed.");

    db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
  }

  /**
   * Do one compare operation.
   * This function will call a DiscreteGenerator, which will return a comparator
   * based on the Properties file.
   */
  public String doCompareOperation() {
    return compareOperationChooser.nextString();
  }

  public void doTransactionFilter(DB db) {
    long keynum = nextKeynum();
    String startkeyname = paddingToStartKeyName(keynum);

    int len = Integer.parseInt(scanlengthproperty);
    String compareValue;

//    String compareValue;
//    if(comparevalue != null) {
//      compareValue = comparevalue;
//    }
//    else {
//      compareValue = buildKeyName(nextKeynum());
//    }

    String operator = doCompareOperation();

    HashSet<String> fields;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else {
      fields = new HashSet<>();
      for(String f : fieldnames) {
        fields.add(f);
      }
    }

    String[] filterProperties;
    if(filtertype.equals("rowfilter")) {
      if(comparevalue != null) {
//        compareValue = Utils.addPadding(comparevalue, keyFormatSize);
        compareValue = comparevalue;
      }
      else {
        compareValue = buildKeyName(nextKeynum());
      }
      filterProperties = new String[2];
      filterProperties[0] = operator;
      filterProperties[1] = compareValue;
    }
    else {
//      String generator = this.tableSchema.getGeneratorTypeFromQualifier(ByteBuffer.wrap(familyfilter.getBytes()), ByteBuffer.wrap(qualifierfilter.getBytes()));
      String generator = this.tableSchema.getGeneratorTypeFromQualifier(familyfilter, qualifierfilter);

      if(generator.equals("Date")) {
        String timestamp = String.valueOf(dateGenerator.dateToTimestamp(comparevalue));
//        compareValue = Utils.addPadding(timestamp, this.tableSchema.getFormatSizeFromQualifier(ByteBuffer.wrap(familyfilter.getBytes()), ByteBuffer.wrap(qualifierfilter.getBytes())));
        compareValue = Utils.addPadding(timestamp, this.tableSchema.getFormatSizeFromQualifier(familyfilter, qualifierfilter));
      }
      else {
//        compareValue = Utils.addPadding(comparevalue, this.tableSchema.getFormatSizeFromQualifier(ByteBuffer.wrap(familyfilter.getBytes()), ByteBuffer.wrap(qualifierfilter.getBytes())));
        compareValue = Utils.addPadding(comparevalue, this.tableSchema.getFormatSizeFromQualifier(familyfilter, qualifierfilter));
      }

//      NOTE: this is temporary (to hardcoded to go to production)
      if(qualifierfilter.equals("Main Identification")) {
        if (firstTimeSingleColumnValue) {
//          System.out.println("First Time SIngle Column Value" + " - " + recordcount + " - " + (recordcount * 0.01));
          patientsMainIdentification = singleColumnPseudoFilter(db, qualifierfilter, recordcount, (int) (recordcount * 0.01));
          patientsArray = mapToEntries(patientsMainIdentification);

          String randomValue = (String) patientsArray[patientsRandom.nextInt(patientsArray.length)];
//          compareValue = Utils.addPadding(randomValue, this.tableSchema.getFormatSizeFromQualifier(ByteBuffer.wrap(familyfilter.getBytes()), ByteBuffer.wrap(qualifierfilter.getBytes())));
          compareValue = Utils.addPadding(randomValue, this.tableSchema.getFormatSizeFromQualifier(familyfilter, qualifierfilter));
//          System.out.println("Random Value - " + randomValue);
          firstTimeSingleColumnValue = false;

          patientsMainIdentification.clear();
        } else {
          String randomValue = (String) patientsArray[patientsRandom.nextInt(patientsArray.length)];
          compareValue = Utils.addPadding(randomValue, this.tableSchema.getFormatSizeFromQualifier(familyfilter, qualifierfilter));
//          compareValue = Utils.addPadding(randomValue, this.tableSchema.getFormatSizeFromQualifier(ByteBuffer.wrap(familyfilter.getBytes()), ByteBuffer.wrap(qualifierfilter.getBytes())));
//          System.out.println("Random Value - " + randomValue);
        }
      }


      filterProperties = new String[4];
      filterProperties[0] = operator;
      filterProperties[1] = compareValue;
      filterProperties[2] = familyfilter;
      filterProperties[3] = qualifierfilter;
    }

    db.filter(table, startkeyname, len, filtertype, filterProperties, fields, new Vector<HashMap<String, ByteIterator>>());

  }

  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
    final double filterproportion = Double.parseDouble(p.getProperty(
        FILTER_PROPORTION_PROPERTY, FILTER_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (scanproportion > 0) {
      operationchooser.addValue(scanproportion, "SCAN");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }

    if (filterproportion > 0 ) {
      operationchooser.addValue(filterproportion, "FILTER");
    }

    return operationchooser;
  }

  /**
   * Creates a weighted discrete values with compare operations for a Filter operation.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "GREATER", "EQUAL", "LESS", "GREATER_OR_EQUAL" and "LESS_OR_EQUAL".
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next comparison to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  public DiscreteGenerator createCompareOperationGenerator(Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double greatProportion = Double.parseDouble(
        p.getProperty(GREAT_PROPORTION_PROPERTY, GREAT_PROPORTION_PROPERTY_DEFAULT));
    final double equalProportion = Double.parseDouble(
        p.getProperty(EQUAL_PROPORTION_PROPERTY, EQUAL_PROPORTION_PROPERTY_DEFAULT));
    final double lessProportion = Double.parseDouble(
        p.getProperty(LESS_PROPORTION_PROPERTY, LESS_PROPORTION_PROPERTY_DEFAULT));
    final double greatOrEqualProportion = Double.parseDouble(
        p.getProperty(GREAT_OR_EQUAL_PROPORTION_PROPERTY, GREAT_OR_EQUAL_PROPORTION_PROPERTY_DEFAULT));
    final double lessOrEqualProportion = Double.parseDouble(
        p.getProperty(LESS_OR_EQUAL_PROPORTION_PROPERTY, LESS_OR_EQUAL_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator compareOperationChooser = new DiscreteGenerator();

    if(greatProportion > 0) {
      compareOperationChooser.addValue(greatProportion, "GREATER");
    }

    if(equalProportion > 0) {
      compareOperationChooser.addValue(equalProportion, "EQUAL");
    }

    if(lessProportion > 0) {
      compareOperationChooser.addValue(lessProportion, "LESS");
    }

    if(greatOrEqualProportion > 0) {
      compareOperationChooser.addValue(greatOrEqualProportion, "GREATER_OR_EQUAL");
    }

    if(lessOrEqualProportion > 0) {
      compareOperationChooser.addValue(lessOrEqualProportion, "LESS_OR_EQUAL");
    }

    return compareOperationChooser;
  }

  public String paddingToStartKeyName(long keynum) {
    String startkeyname;
    if(fulltablesearch) {
//      startkeyname = new String(Utils.addPadding("0", keyFormatSize).getBytes());
      startkeyname = "0";
    }
    else {
      if (startrow != null) {
//        startkeyname = new String(Utils.addPadding(startrow, keyFormatSize).getBytes());
//        startkeyname = Utils.addPadding(startrow, keyFormatSize, '0');
        startkeyname = startrow;
      } else {
//        startkeyname = new String(Utils.addPadding(buildKeyName(keynum), keyFormatSize).getBytes());
        startkeyname = buildKeyName(keynum);
      }
    }
    return startkeyname;
  }

  public HashMap<String, ByteIterator> temporaryRead(DB db, String keyname) {
    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else {
      fields = new HashSet<>();
      for(String f : fieldnames) {
        fields.add(f);
      }
    }

    HashMap<String, ByteIterator> cells = new HashMap<>();
    db.read(table, keyname, fields, cells);

    return cells;
  }

  public ConcurrentHashMap<String, String> singleColumnPseudoFilter(DB db, String qualifier, long totalValuesLoaded, int increment) {
//    Map<String, String> tempValues = new HashMap<>();
    ConcurrentHashMap<String, String> tempValues = new ConcurrentHashMap<>();
    if(qualifier.equals("Main Identification")) {
      long counter = 0;
      for(long i = 0; counter < totalValuesLoaded; i++) {
        counter = i*increment;
        String key = buildKeyName(counter);
        Map<String, ByteIterator> resultCells = temporaryRead(db, key);
        if(resultCells.containsKey("Main Identification")) {
          String tempID = new String(resultCells.get("Main Identification").toArray());
          tempValues.put(key, tempID);
        }
      }
    }

    return tempValues;
  }

  public Object[] mapToEntries(ConcurrentHashMap<String,String> map) {
    return map.values().toArray();
  }
}
