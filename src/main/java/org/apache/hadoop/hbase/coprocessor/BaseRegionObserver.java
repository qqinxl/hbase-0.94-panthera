/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

/**
 * An abstract class that implements RegionObserver.
 * By extending it, you can create your own region observer without
 * overriding all abstract methods of RegionObserver.
 */
public abstract class BaseRegionObserver implements RegionObserver {
  @Override
  public void start(CoprocessorEnvironment e) throws IOException { }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException { }

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) { }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) { }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> e,
      boolean abortRequested) { }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> e,
      boolean abortRequested) { }

  @Override
  public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
  }

  @Override
  public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
  }

  @Override
  public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r)
      throws IOException {
  }

  @Override
  public void preCompactSelection(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Store store, final List<StoreFile> candidates) throws IOException { }

  @Override
  public void postCompactSelection(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Store store, final ImmutableList<StoreFile> selected) { }

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final InternalScanner scanner) throws IOException {
    return scanner;
  }

  @Override
  public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, final Store store,
      final StoreFile resultFile) throws IOException {
  }

  @Override
  public void preGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final Result result)
    throws IOException {
  }

  @Override
  public void postGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final Result result)
      throws IOException {
  }

  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final List<KeyValue> results) throws IOException {
  }

  @Override
  public void postGet(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final List<KeyValue> results) throws IOException {
  }

  @Override
  public boolean preExists(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final boolean exists) throws IOException {
    return exists;
  }

  @Override
  public boolean postExists(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, boolean exists) throws IOException {
    return exists;
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, 
      final Put put, final WALEdit edit, final boolean writeToWAL) throws IOException {
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, 
      final Put put, final WALEdit edit, final boolean writeToWAL) throws IOException {
  }

  @Override
  public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e, 
      final Delete delete, final WALEdit edit, final boolean writeToWAL) throws IOException {
  }

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Delete delete, final WALEdit edit, final boolean writeToWAL) throws IOException {
  }

  @Override
  public boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Put put, final boolean result) throws IOException {
    return result;
  }

  @Override
  public boolean postCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Put put, final boolean result) throws IOException {
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Delete delete, final boolean result) throws IOException {
    return result;
  }

  @Override
  public boolean postCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Delete delete, final boolean result) throws IOException {
    return result;
  }

  @Override
  public Result preAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Append append) throws IOException {
    return null;
  }

  @Override
  public Result postAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Append append, final Result result) throws IOException {
    return result;
  }

  @Override
  public long preIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL) throws IOException {
    return amount;
  }

  @Override
  public long postIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL, long result)
      throws IOException {
    return result;
  }

  @Override
  public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Increment increment) throws IOException {
    return null;
  }

  @Override
  public Result postIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Increment increment, final Result result) throws IOException {
    return result;
  }

  @Override
  public RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Scan scan, final RegionScanner s) throws IOException {
    return s;
  }

  @Override
  public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Scan scan, final RegionScanner s) throws IOException {
    return s;
  }

  @Override
  public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final List<Result> results,
      final int limit, final boolean hasMore) throws IOException {
    return hasMore;
  }

  @Override
  public boolean postScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final List<Result> results, final int limit,
      final boolean hasMore) throws IOException {
    return hasMore;
  }

  @Override
  public void preScannerClose(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s) throws IOException {
  }

  @Override
  public void postScannerClose(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s) throws IOException {
  }

  @Override
  public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> env, HRegionInfo info,
      HLogKey logKey, WALEdit logEdit) throws IOException {
  }

  @Override
  public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
  }

  @Override
  public boolean preFilterRow(
      ObserverContext<RegionCoprocessorEnvironment> env, List<KeyValue> kvs,
      Filter filter) throws IOException {
    return false;
  }

  @Override
  public void postFilterRow(ObserverContext<RegionCoprocessorEnvironment> env,
      List<KeyValue> kvs, Filter filter) throws IOException {
  }

  @Override
  public boolean preFilterTransform(
      ObserverContext<RegionCoprocessorEnvironment> env, KeyValue kv,
      KeyValue[] returnedKV, Filter filter) throws IOException {
    return false;
  }

  @Override
  public KeyValue postFilterTransform(
      ObserverContext<RegionCoprocessorEnvironment> env, KeyValue kv,
      KeyValue returnedKV, Filter filter) throws IOException {
    return returnedKV;
  }

  @Override
  public boolean preFilterGetNextKeyHint(
      ObserverContext<RegionCoprocessorEnvironment> env, KeyValue kv,
      KeyValue[] returnedKV, Filter filter) throws IOException {
    return false;
  }

  @Override
  public KeyValue postFilterGetNextKeyHint(
      ObserverContext<RegionCoprocessorEnvironment> env, KeyValue kv,
      KeyValue returnedKV, Filter filter) throws IOException {
    return returnedKV;
  }

  @Override
  public boolean preFilterRowKey(
      ObserverContext<RegionCoprocessorEnvironment> env, byte[] buffer,
      int offset, int length, boolean result, Filter filter) throws IOException {
    return false;
  }

  @Override
  public boolean postFilterRowKey(
      ObserverContext<RegionCoprocessorEnvironment> env, byte[] buffer,
      int offset, int length, boolean result, Filter filter) throws IOException {
    return result;
  }

  @Override
  public boolean preFilterKeyValue(
      ObserverContext<RegionCoprocessorEnvironment> env, KeyValue kv,
      ReturnCode r, Filter filter) throws IOException {
    return false;
  }

  @Override
  public ReturnCode postFilterKeyValue(
      ObserverContext<RegionCoprocessorEnvironment> env, KeyValue kv,
      ReturnCode r, Filter filter) throws IOException {
    return r;
  }

  @Override
  public void preBulkLoadHFile(final ObserverContext<RegionCoprocessorEnvironment> ctx,
    List<Pair<byte[], String>> familyPaths) throws IOException {
  }

  @Override
  public boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
    List<Pair<byte[], String>> familyPaths, boolean hasLoaded) throws IOException {
    return hasLoaded;
  }
}
