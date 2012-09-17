/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class PerformanceEvaluation2 {
  protected static final Log LOG = LogFactory
      .getLog(PerformanceEvaluation2.class.getName());

  /**
   * Enum for map metrics. Keep it out here rather than inside in the Map
   * inner-class so we can find associated properties.
   */
  protected static enum Counter {
    /** elapsed time */
    ELAPSED_TIME,
    /** number of rows */
    ROWS,
  }

  /**
   * Implementations can have their status set.
   */
  static interface Status {
    /**
     * Sets status
     * 
     * @param msg
     *          status message
     * @throws IOException
     */
    void setStatus(final String msg) throws IOException;
  }

  public static enum RunType {
    run_native, run_mapred, run_mapred2,
  }

  private Configuration conf;
  private int threads = 1;
  private int maps = 0;
  private String tableName = null;
  private RunType runType = RunType.run_native;

  /**
   * Constructor
   * 
   * @param c
   *          Configuration object
   */
  public PerformanceEvaluation2(final Configuration c) {
    this.conf = c;
  }

  public static class E1 {
    /**
     * MapReduce job that runs a performance evaluation client in each map task.
     */
    public static class EvaluationMapTask extends
        Mapper<ImmutableBytesWritable, Result, String, LongWritable> {
      private Test cmd;

      @Override
      protected void setup(Context context) throws IOException,
          InterruptedException {
        try {
          this.cmd = new Test(context.getConfiguration());
        } catch (Exception e) {
          throw new IllegalStateException(
              "Could not instantiate Test instance", e);
        }
      }

      @Override
      protected void map(ImmutableBytesWritable key, Result value,
          final Context context) throws IOException, InterruptedException {
        this.cmd.columnsQuery(value);
        context.getCounter(Counter.ROWS).increment(1);
        context.progress();
      }
    }

    public static class RegionScanInputFormat extends
        InputFormat<NullWritable, TableSplit> {

      public static class RegionScanRecordReader extends
          RecordReader<NullWritable, TableSplit> {
        private TableSplit value = null;

        public RegionScanRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
          initialize(split, context);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
          this.value = (TableSplit) split;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          if (null == this.value) {
            return false;
          }

          return true;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException,
            InterruptedException {
          return NullWritable.get();
        }

        @Override
        public TableSplit getCurrentValue() throws IOException,
            InterruptedException {
          TableSplit s = this.value;

          this.value = null;
          return s;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
          return 0.5f;
        }

        @Override
        public void close() throws IOException {
        }
      }

      @Override
      public List<InputSplit> getSplits(JobContext context) throws IOException,
          InterruptedException {
        TableInputFormat tif = new TableInputFormat();
        tif.setConf(context.getConfiguration());
        return tif.getSplits(context);
      }

      @Override
      public RecordReader<NullWritable, TableSplit> createRecordReader(
          InputSplit split, TaskAttemptContext context) throws IOException,
          InterruptedException {
        return new RegionScanRecordReader(split, context);
      }
    }

    public static class EvaluationMapTask2 extends
        Mapper<NullWritable, TableSplit, LongWritable, LongWritable> {

      private Test cmd;

      @Override
      protected void setup(Context context) throws IOException,
          InterruptedException {
        try {
          this.cmd = new Test(context.getConfiguration());
        } catch (Exception e) {
          throw new IllegalStateException("Could not instantiate PE instance",
              e);
        }
      }

      @Override
      protected void map(NullWritable key, TableSplit value,
          final Context context) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();
        // Evaluation task
        long recordCount = this.cmd.test(value);
        long elapsedTime = System.currentTimeMillis() - startTime;

        // Collect how much time the thing took. Report as map output and
        // to the ELAPSED_TIME counter.
        context.getCounter(Counter.ELAPSED_TIME).increment(elapsedTime);
        context.getCounter(Counter.ROWS).increment(recordCount);

        context.write(new LongWritable(1), new LongWritable(
            elapsedTime));
        context.progress();
      }
    }
  }

  /*
   * We're to run multiple clients concurrently. Setup a mapreduce job. Run one
   * map per client. Then run a single reduce to sum the elapsed times.
   * 
   * @param cmd Command to run.
   * 
   * @throws IOException
   */
  private long runTest(final Test cmd, List<TableSplit> splits)
      throws IOException, InterruptedException, ClassNotFoundException {
    if (this.runType.equals(RunType.run_native)) {
      return doMultipleClients(cmd, splits, this.threads);
    } else {
      // must be the map reduce, do some clean up
      FileSystem fs = FileSystem.get(conf);
      Path path = new Path("/tmp", "tempout");
      fs.delete(path, true);

      try {
        if (this.runType.equals(RunType.run_mapred)) {
          return doMapReduce(cmd, TableInputFormat.class,
              E1.EvaluationMapTask.class, path);
        }

        if (this.runType.equals(RunType.run_mapred2)) {
          return doMapReduce(cmd, E1.RegionScanInputFormat.class,
              E1.EvaluationMapTask2.class, path);
        }
      } finally {
        fs.deleteOnExit(path);
      }
    }
    return -1;
  }

  /*
   * Run all clients in this vm each to its own thread.
   * 
   * @param cmd Command to run.
   * 
   * @throws IOException
   */
  private long doMultipleClients(final Test cmd, final List<TableSplit> splits,
      final int nthread) throws IOException {

    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(nthread);
    final ThreadPoolExecutor services = new ThreadPoolExecutor(nthread,
        nthread, 10, TimeUnit.SECONDS, queue,
        new ThreadPoolExecutor.CallerRunsPolicy());
    final AtomicLong count = new AtomicLong(0);

    for (final TableSplit ts : splits) {
      services.submit(new Runnable() {

        @Override
        public void run() {
          try {
            long startTime = System.currentTimeMillis();
            long recordCount = runOneClient(cmd, ts);
            long elapsedTime = System.currentTimeMillis() - startTime;

            LOG.info("Finished " + Thread.currentThread().getName() + " in "
                + elapsedTime + "ms for " + recordCount + " rows");
            count.getAndAdd(recordCount);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }

    services.shutdown();
    try {
      services.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return count.get();
  }

  private long doMapReduce(final Test cmd,
      Class<? extends InputFormat> inputFormatClass,
      Class<? extends Mapper> mapperClass, Path path) throws IOException,
      ClassNotFoundException, InterruptedException {
    Job job = new Job(this.conf);
    job.setJarByClass(PerformanceEvaluation2.class);
    job.setJobName("HBase Performance Evaluation");

    job.setInputFormatClass(inputFormatClass);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(mapperClass);
    job.setReducerClass(LongSumReducer.class);

    job.setNumReduceTasks(1);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, path);

    TableMapReduceUtil.addDependencyJars(job);
    // Add a Class from the hbase.jar so it gets registered too.
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        org.apache.hadoop.hbase.util.Bytes.class);

    TableMapReduceUtil.initCredentials(job);

    job.waitForCompletion(true);
    return job.getCounters().findCounter(Counter.ROWS).getValue();
  }

  /*
   * A test. Subclass to particularize what happens per row.
   */
  static class Test {
    public static final String KEY_INPUT_TABLE = TableInputFormat.INPUT_TABLE;
    public static final String KEY_OPTION_CACHE_SIZE = "key_cache_size";
    public static final String KEY_OPTION_SCAN_COLS = TableInputFormat.SCAN_COLUMNS;
    public static final String KEY_OPTION_ROWS = "key_rows";
    public static final String KEY_OPTION_PERIOD = "key_report_period";

    protected Status status = new Status() {
      @Override
      public void setStatus(String msg) throws IOException {
        LOG.info(msg);
      }
    };

    protected int reportPeriod = 20000;
    protected int cacheSize = 1000;
    protected long maxRows = 0;
    protected String cols;
    protected Configuration conf = null;

    // protected HTable table;

    /**
     * Note that all subclasses of this class must provide a public contructor
     * that has the exact same list of arguments.
     * 
     * @throws IOException
     */
    Test(Configuration conf) throws IOException {
      this.reportPeriod = Integer.parseInt(conf.get(KEY_OPTION_PERIOD, "2000"));
      this.cols = conf.get(KEY_OPTION_SCAN_COLS);
      this.cacheSize = conf.getInt(KEY_OPTION_CACHE_SIZE, 1000);
      this.maxRows = conf.getLong(KEY_OPTION_ROWS, Long.MAX_VALUE);

      this.conf = conf;
    }

    private String generateStatus(final String sr, final long i) {
      return sr + "/" + i;
    }

    protected int getReportingPeriod() {
      return this.reportPeriod;
    }

    void testTakedown() throws IOException {
    }

    /*
     * Run test
     * 
     * @return record count scanned.
     * 
     * @throws IOException
     */
    long test(TableSplit split) throws IOException {
      try {
        return testTimed(split.getStartRow(), split.getEndRow(), this.maxRows);
      } finally {
        testTakedown();
      }
    }

    protected void addColumns(Scan scan, String columns) {
      String[] cols = columns.split(" ");
      for (String col : cols) {
        addColumn(scan, Bytes.toBytes(col));
      }
    }

    protected void addColumn(Scan scan, byte[] familyAndQualifier) {
      byte[][] fq = KeyValue.parseColumn(familyAndQualifier);
      if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
        scan.addColumn(fq[0], fq[1]);
      } else {
        scan.addFamily(fq[0]);
      }
    }

    public void columnsQuery(Result result) {
      result.getRow();
    }

    /**
     * Provides the scan range per key.
     */
    long testTimed(byte[] startKey, byte[] endKey, long limit) throws IOException {
      long count = 0;

      HTable table = new HTable(conf, conf.get(KEY_INPUT_TABLE));

      try {
        Scan scan = new Scan(startKey, endKey);
        scan.setCaching(this.cacheSize);
        this.addColumns(scan, this.cols);
        ResultScanner rs = table.getScanner(scan);

        while (true) {
          Result result = rs.next();

          if (null == result) {
            break;
          }

          byte[] key = result.getRow();

          if (status != null && count > 0
              && (count % getReportingPeriod()) == 0) {
            status.setStatus(generateStatus(Bytes.toStringBinary(key), count));
          }

          if (++count >= limit) {
            break;
          }
        }
      } finally {
        table.close();
      }
      return count;
    }
  }

  long runOneClient(final Test cmd, final TableSplit split) throws IOException {

    LOG.info("Start " + cmd + " from key: "
        + Bytes.toString(split.getStartRow()) + " to "
        + Bytes.toString(split.getEndRow()));
    long startTime = System.currentTimeMillis();

    long recordCount = cmd.test(split);
    long totalElapsedTime = System.currentTimeMillis() - startTime;

    LOG.info("Finished " + cmd + " in " + totalElapsedTime + "ms for "
        + recordCount + " rows");
    return recordCount;
  }

  private List<TableSplit> getTableSplits(HTable table) throws IOException {
    Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
    if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    List<TableSplit> splits = new ArrayList<TableSplit>(100);

    for (int i = 0; i < keys.getFirst().length; i++) {
      // determine if the given start an stop key fall into the region
      TableSplit split = new TableSplit(table.getTableName(),
          keys.getFirst()[i], keys.getSecond()[i], null);

      splits.add(split);
      LOG.debug("Split -> " + i + " -> " + split);
    }

    return splits;
  }

  private long runTest(final Test cmd) throws IOException,
      InterruptedException, ClassNotFoundException {
    try {
      HBaseAdmin admin = null;
      try {
        String tableName = this.conf.get(TableInputFormat.INPUT_TABLE);
        admin = new HBaseAdmin(this.conf);
        if (!admin.tableExists(tableName)) {
          LOG.error("Table :" + tableName + " doesn't exists, will exit.");
          return -1;
        }
        List<TableSplit> splits = getTableSplits(new HTable(new Configuration(
            conf), Bytes.toBytes(tableName)));
        this.maps = splits.size();

        return runTest(cmd, splits);
      } catch (Exception e) {
        LOG.error("Failed", e);
        return -1;
      }
    } finally {
    }
  }

  protected void printUsage() {
    printUsage(null);
  }

  protected void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() + " \\");
    System.err
        .println(" <--type=native|mapred|mapred2> [--rows=] --table=abc --cols=\"f:col1 f:col2\" [nThreads]");
    System.err.println();
  }

  public int doCommandLine(final String[] args) {
    // (but hopefully something not as painful as cli options).
    int errCode = -1;
    if (args.length < 1) {
      printUsage();
      return errCode;
    }

    String optionCols = null;
    String optionRow = String.valueOf(Long.MAX_VALUE);

    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-h") || cmd.startsWith("--h")) {
        printUsage();
        errCode = 0;
        break;
      }

      final String type = "--type=";
      if (cmd.startsWith(type)) {
        this.runType = RunType.valueOf("run_" + cmd.substring(type.length()));
        continue;
      }

      final String rows = "--rows=";
      if (cmd.startsWith(rows)) {
        optionRow = cmd.substring(rows.length());
        this.conf.set(Test.KEY_OPTION_ROWS, optionRow);
        
        continue;
      }

      final String table = "--table=";
      if (cmd.startsWith(table)) {
        tableName = cmd.substring(table.length());
        this.conf.set(TableInputFormat.INPUT_TABLE, this.tableName);
        
        continue;
      }

      final String family = "--cols=";
      if (cmd.startsWith(family)) {
        optionCols = cmd.substring(family.length());
        if (null != optionCols && !"".equals(optionCols)) {
          optionCols = optionCols.replaceAll("\"", "");
        }
        
        this.conf.set(TableInputFormat.SCAN_COLUMNS, optionCols == null ? "" : optionCols);
        continue;
      }

      if (this.runType == RunType.run_native) {
        try {
          this.threads = Integer.parseInt(cmd);
        } catch (Exception ignore) {
          this.threads = 0;
        }
      }
    }
    
    try {
      if (tableName == null || this.threads < 1) {
        printUsage("Please specify the table name or nthread");
        errCode = 1;
      } else {
        long startTime = System.currentTimeMillis();
        long recordCount = runTest(new Test(this.conf));
        long elapse = System.currentTimeMillis() - startTime;
        LOG.info("Time Elapsed: " + (elapse) + " milliseconds for scanning "
            + recordCount + " records.");
      }
    } catch (Exception e) {
      LOG.error("Error in running test", e);
      errCode = 2;
    } finally {
      LOG.info("Table Name:" + this.tableName);
      LOG.info("Type: " + this.runType);
      if (this.runType == RunType.run_native) {
        LOG.info("Threads: " + this.threads);
      } else {
        LOG.info("Maps: " + this.maps);
      }
    }

    return errCode;
  }

  /**
   * @param args
   */
  public static void main(final String[] args) {
    Configuration c = HBaseConfiguration.create();
    System.exit(new PerformanceEvaluation2(c).doCommandLine(args));
  }

  // public static void main(final String[] args) {
  // Configuration configuration = HBaseConfiguration.create();
  // configuration.clear();
  //
  // configuration.set("hbase.zookeeper.quorum", "haocheng-desktop"); // - Our
  // configuration.set("hbase.zookeeper.property.clientPort", "2181"); // - Port
  //
  // System.exit(new PerformanceEvaluation2(configuration).doCommandLine(args));
  // }
}
