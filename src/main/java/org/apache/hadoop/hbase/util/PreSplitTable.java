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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.dot.DotConstants;
import org.apache.hadoop.hbase.dot.DotUtil;
import org.apache.hadoop.hbase.dot.access.mapreduce.DotTsvImporterMapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONObject;

/**
 * To create a new Table according to split-points
 * 
 *
 */
public class PreSplitTable extends Configured implements Tool {

  public static String NAME = "PreSplitTable";
  private HBaseAdmin hbAdmin;
  private Configuration cfg;
  private static Log LOG = LogFactory.getLog(PreSplitTable.class);
  
  public PreSplitTable( Configuration conf ) throws Exception {
    super(conf);
    this.cfg = conf;
    this.hbAdmin = new HBaseAdmin(conf);
  }
  
  private void usage() {
    System.err.println("usage: " + NAME +
        " [splitpoints] " +
        "tablename");
  }
  
  private boolean doesTableExist(String tablename) throws Exception {
    return hbAdmin.tableExists(tablename);
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int ret = ToolRunner.run(new PreSplitTable(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    // TODO Auto-generated method stub
    if (args.length < 1) {
      usage();
      return -1;
    }

    String splitpoints = "";
    String tableName = null;
    if(args.length == 1)
      tableName = args[0];
    else if(args.length == 2){
      splitpoints = args[0];
      tableName = args[1];
    }
    
    boolean tableExists   = this.doesTableExist(tableName);
    if (!tableExists) {
      this.createTable(tableName,splitpoints);
    }
    else {
      LOG.warn("Table " + tableName + " already exists, no presplit works");
    }
    
    return 0;
  }
  
  private byte[][] getSplitPoints(String splitpoints) {
    String [] strs = StringUtils.getStrings(splitpoints);
    if(strs == null || strs.length == 0 ){
      // prepare split points from TotalOrderPatitionerFile
      Path dst = new Path(TotalOrderPartitioner.getPartitionFile(cfg));
      try {
        FileSystem fs = dst.getFileSystem(cfg);
        if(!fs.exists(dst)) {
          return null;
        }
        
        List<String> splitList = new ArrayList<String>();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, dst, cfg);
        ImmutableBytesWritable key = (ImmutableBytesWritable) reader.getKeyClass().newInstance();
        while(reader.next(key)){
          splitList.add(new String(key.get()));
        }
        
        if(splitList == null || splitList.isEmpty()) {
          return null;
        }
        strs = splitList.toArray(strs);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return null;
      }
    }
    
    byte[][] points = new byte[strs.length][];
    int i = 0;
    for (String p : strs) {
      points[i] = p.getBytes();
      i++;
    }
    return points;
  }
  
  private HColumnDescriptor configureHColumn(Configuration conf, HColumnDescriptor hcd) {
    hcd.setValue(HColumnDescriptor.DATA_BLOCK_ENCODING, 
        conf.get(HColumnDescriptor.DATA_BLOCK_ENCODING, HColumnDescriptor.DEFAULT_DATA_BLOCK_ENCODING));
    
    return hcd;
  }

  private void createTable(String tableName, String splitpoints) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);  
    
    desc = DotUtil.prepareDotTable(cfg, desc);
    
    String columns[] = cfg.getStrings(ImportTsv.COLUMNS_CONF_KEY);
    Map<byte[], Map<byte[], JSONObject>> schemas = DotUtil.genSchema(columns, desc);
    
    Set<String> cfSet = new HashSet<String>();
    for (String aColumn : columns) {
      if (TsvParser.ROWKEY_COLUMN_SPEC.equals(aColumn)) continue;
      // we are only concerned with the first one (in case this is a cf:cq)
      cfSet.add(aColumn.split(DotConstants.HBASE_DOT_COLUMNFAMILY_AND_DOC_SEPERATOR, 2)[0]);
    }
    for (String cf : cfSet) {
      HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(cf));
      hcd = configureHColumn(cfg, hcd);
      hcd = DotUtil.prepareDotColumn(cfg, hcd, desc, schemas);
      desc.addFamily(hcd);
    }
    
    
    byte[][] splits = getSplitPoints(splitpoints);
    if(splits != null) {
      hbAdmin.createTable(desc, splits);
    }
    else {
      hbAdmin.createTable(desc);
    }
 
  }

}