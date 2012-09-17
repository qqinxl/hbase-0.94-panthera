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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author jhuang4
 *
 */
public class DumpSplitPoints extends Configured implements Tool {
  public static String NAME = "DumpSplitPoints";
  private HBaseAdmin hbAdmin;
  private Configuration cfg;
  private static Log LOG = LogFactory.getLog(DumpSplitPoints.class);
  
  public DumpSplitPoints(Configuration conf)throws Exception {
    super(conf);
    this.cfg = conf;
    this.hbAdmin = new HBaseAdmin(conf);
  }
  
  private int usage() {
    System.err.println("usage: " + NAME +
        " [-s <comma delimitted split point list>]" +
        " [-t <tablename> ]" +
        " outputFileName");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }
  
  private boolean doesTableExist(String tablename) throws Exception {
    return hbAdmin.tableExists(tablename);
  }
  
  private void dumpSplitPoints(List<ImmutableBytesWritable> splitpoints, String file, Job job) throws IOException{
    if (splitpoints.isEmpty()) {
      throw new IllegalArgumentException("No regions passed");
    }
    Configuration conf = job.getConfiguration();
    Path outputFile = new Path(file);
    FileSystem fs = outputFile.getFileSystem(conf);

    TreeSet<ImmutableBytesWritable> sorted = new TreeSet<ImmutableBytesWritable>(
        splitpoints);

    ImmutableBytesWritable first = sorted.first();
    if (first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
      sorted.remove(first);
    }

    if(sorted.size() == 0) {
      System.err.println("No valid split points are specified or HTable only has one region.");
    }
    
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
        outputFile, ImmutableBytesWritable.class, NullWritable.class);

    try {
      for (ImmutableBytesWritable startKey : sorted) {
        writer.append(startKey, NullWritable.get());
      }
    } finally {
      writer.close();
    }
  }
  
  List<ImmutableBytesWritable> getSplitPoints(HTable table) throws IOException {
    byte[][] byteKeys = table.getStartKeys();
    ArrayList<ImmutableBytesWritable> ret =
      new ArrayList<ImmutableBytesWritable>(byteKeys.length);
    for (byte[] byteKey : byteKeys) {
      ret.add(new ImmutableBytesWritable(byteKey));
    }
    return ret;
  }
  
  List<ImmutableBytesWritable> getSplitPoints(String[] splitpoints) {
    if(splitpoints == null || splitpoints.length == 0){
      return null;
    }
    ArrayList<ImmutableBytesWritable> ret =
        new ArrayList<ImmutableBytesWritable>(splitpoints.length);
    for (String  point : splitpoints) {
      ret.add(new ImmutableBytesWritable(point.getBytes()));
    }
    return ret;
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      usage();
      return -1;
    }
    String outputfile = null;
    Job job = new Job(getConf());
    List<ImmutableBytesWritable> splits = null;
    for(int i=0; i < args.length; ++i) {
      if ("-s".equals(args[i])) {
        String[] splitpoints = StringUtils.getStrings(args[++i]);
        splits = getSplitPoints(splitpoints);
      } else if ("-t".equals(args[i])) {
        String tableName = args[++i];
        if(doesTableExist(tableName)) {
          splits = getSplitPoints(new HTable(cfg, tableName));
        }
      } else {
        outputfile = args[i];
      }
    }
    
    if(outputfile == null || outputfile.equals("")) {
      System.err.println("Please define one output file name");
      return usage();
    }
    
    if(splits == null) {
      System.err.println("Please specify correct table name or split points");
      return usage();
    } 
    
    dumpSplitPoints(splits, outputfile, job);
    return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{
    // TODO Auto-generated method stub
    int ret = ToolRunner.run(new DumpSplitPoints(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

}
