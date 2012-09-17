package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;

/**
 * This is a Filter wrapper class which is used in the server side. Some filter
 * related hooks can be defined in this wrapper. The only way to create a
 * FilterWrapper instance is passing a client side Filter instance.
 * 
 */
public class FilterWrapper implements Filter {
  private static final Log LOG = LogFactory.getLog(FilterWrapper.class);
  Filter filter = null;
  RegionCoprocessorHost coprocessorHost = null;

  public Filter getFilterInstance() {
    return this.filter;
  }

  public FilterWrapper( Filter filter ) {
    if (null == filter) {
      // ensure the filter instance is not null
      throw new NullPointerException("Cannot create FilterWrapper with null");
    }

    if (filter instanceof FilterWrapper) {
      // copy constructor
      this.filter = ( (FilterWrapper) filter ).getFilterInstance();
      this.coprocessorHost = ( (FilterWrapper) filter ).getCoprocessorHost();
    } else {
      this.filter = filter;
    }
  }

  public void setCoprocessorHost(RegionCoprocessorHost coprocessorHost) {
    if (coprocessorHost != null) {
      this.coprocessorHost = coprocessorHost;
    }
  }
  
  public RegionCoprocessorHost getCoprocessorHost() {
    return this.coprocessorHost;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.filter.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.filter.readFields(in);
  }

  @Override
  public void reset() {
    this.filter.reset();
  }

  @Override
  public boolean filterAllRemaining() {
    // TODO add co-processor supporting if necessary
    return this.filter.filterAllRemaining();
  }

  @Override
  public boolean filterRow() {
    // Never happens
    assert false : "filterRow() can only be called inside FilterWrapper.";
    return this.filter.filterRow();
  }

  @Override
  public boolean hasFilterRow() {
    // TODO add co-processor supporting if necessary
    return this.filter.hasFilterRow();
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    KeyValue[] kv = new KeyValue[] { null };
    boolean bypass = false;

    if (this.coprocessorHost != null) {
      try {
        bypass = this.coprocessorHost.preFilterGetNextKeyHint(currentKV, kv,
            this.filter);
      } catch (IOException e) {
        // TODO to throws IOException
        throw new RuntimeException(e);
      }
    }

    if (!bypass) {
      kv[0] = this.filter.getNextKeyHint(currentKV);
    }

    try {
      return this.coprocessorHost != null ? this.coprocessorHost
          .postFilterGetNextKeyHint(currentKV, kv[0], this.filter) : kv[0];
    } catch (IOException e) {
      // TODO to throws IOException
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    boolean result = false;
    boolean bypass = false;
    if (this.coprocessorHost != null) {
      try {
        bypass = this.coprocessorHost.preFilterRowKey(buffer, offset, length,
            result, this.filter);
      } catch (IOException e) {
        // TODO to throws IOException
        throw new RuntimeException(e);
      }
    }

    if (!bypass) {
      result = this.filter.filterRowKey(buffer, offset, length);
    }

    try {
      return this.coprocessorHost != null ? this.coprocessorHost
          .postFilterRowKey(buffer, offset, length, result, this.filter)
          : result;
    } catch (IOException e) {
      // TODO to throws IOException
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    LOG.info("filterKeyValue");
    ReturnCode rc = ReturnCode.INCLUDE;
    boolean bypass = false;
    if (this.coprocessorHost != null) {
      try {
        bypass = this.coprocessorHost.preFilterKeyValue(v, rc, this.filter);
      } catch (IOException e) {
        // TODO to throws IOException
        throw new RuntimeException(e);
      }
    }

    if (!bypass) {
      rc = this.filter.filterKeyValue(v);
    }
    try {
      return this.coprocessorHost != null ? this.coprocessorHost
          .postFilterKeyValue(v, rc, this.filter) : rc;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    }
  }

  @Override
  public KeyValue transform(KeyValue v) {
    KeyValue[] kv = new KeyValue[] { null };
    boolean bypass = false;
    if (this.coprocessorHost != null) {
      try {
        bypass = this.coprocessorHost.preFilterTransform(v, kv, this.filter);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }
    }
    if (!bypass) {
      kv[0] = this.filter.transform(v);
    }
    try {
      return this.coprocessorHost != null ? this.coprocessorHost
          .postFilterTransform(v, kv[0], this.filter) : kv[0];
    } catch (IOException e) {
      // TODO to throws IOException
      throw new RuntimeException(e);
    }
  }

  @Override
  public void filterRow(List<KeyValue> kvs) {
    boolean bypass = false;
    if (this.coprocessorHost != null) {
      try {
        bypass = this.coprocessorHost.preFilterRow(kvs, this.filter);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    if (!bypass) {
      this.filter.filterRow(kvs);
      if (!kvs.isEmpty() && this.filter.filterRow()) {
        kvs.clear();
      }
    }

    if (this.coprocessorHost != null) {
      try {
        this.coprocessorHost.postFilterRow(kvs, this.filter);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    
  }

}
