# A Document Store for Better Query Processing on HBase #

####Why query processing on HBase####

In Hadoop based data warehousing systems (e.g., Hive) , the users can analyze their massive data using a high level query language based on the relational model; the queries are then automatically converted into a series of MapReduce jobs for data processing. By bringing the traditional database techniques to the Hadoop ecosystem, these systems make MapReduce much more accessible to mainstream users. 

Initially, these systems typically store their data in HDFS, a high throughput but batch-processing oriented distributed file system (e.g., see [this link](http://hadoopblog.blogspot.com/2011/04/data-warehousing-at-facebook.html) for an example of one of the largest Hive deployment in the world). On the other hand, in the last couple of years, increasingly more systems have transitioned to a (semi) realtime analytics system built around HBase (i.e., storing data in HBase and running MapReduce jobs directly on HBase tables for query processing), which make several new use cases possible:

* *Stream* new data into HBase in near realtime for processing (versus loading new data into the HDFS cluster in large batch, e.g., every few hours).

* Support *high-update rate* workloads (by directly updating existing data in HBase) to keep the warehouse always up to date.

* Allow every low latency, *online data serving* (e.g., to power an online web service).

#### Overheads of query processing on HBase ####

HBase is an open source implementation of BigTable, which provides very flexible schema support, and very low latency get/put of individual cells in the table. While HBase already has very effective MapReduce integration with its good scanning performance, query processing using MapReduce on HBase still has significant gaps compared to query processing on HDFS.

* *Space overheads*. To provide flexible schema support, physically HBase stores its table as a multi-dimensional map, where each cell (except the row key) is stored on disk as a key-value pair: (row_id, family:column, timestamp)  cell. On the other hand, a Hive table has a fixed relational model, and consequently HBase can introduce large space overheads (sometimes as large as 3x) compared to storing the same table in HDFS.

* *Query performance*. Query processing on HBase can be much (sometimes 3~5x) slower than that on HDFS due to various reasons. One of the reason is related to how HBase handles data accesses – HBase provides very good support for high concurrent read/write accesses; consequently, one needs to pay some amount of overheads (e.g., concurrency control) for each column read. On the other hand, data accesses in analytical query processing are predominantly read (with some append), and should preferably avoid the column read overheads.

#### A document store on HBase ####

We have implemented a *document store* on HBase, which greatly improve query processing on HBase by leveraging the relational model and read-mostly access patterns. The figure below illustrates the data model of the document store.

<img src="dot_data_model.jpg" alt="DOT Data Model" width="304" height="228" />

In the document store, a table can be declared as a *document-oriented table* (DOT) at the table creation time. Each row in DOT contains, in addition to the *row key*, a collection of documents (doc), and each document contains a collection of *fields*; in query processing, each column in a relational table is simply mapped to a field in some document.

Physically, each document is encoded using a serialization framework (such as Avro or Protocol Buffers), and its schema is stored separately (just once); consequently, the storage overheads can be greatly reduced. In addition, each document is mapped to an HBase column and is the unit for update; consequently the associated read overheads can be amortized across different fields in a document.
 
We have implemented DOT using HBase co-processors. When creating a DOT, the user is required to specify the schema and serializer (e.g., Avro) for each document in the table; the schema information is stored in table metadata by the `preCreateTable` co-processor. The users of DOT (e.g., a Hive query) can access individual fields in the document-oriented table in the same way as they access individual columns in a conventional HBase table – just specifying “doc.field” in place of “column qualifier” in `Get`/`Scan`/`Put`/`Delete` and `Filter` objects; the associated co-processors are responsible for dynamically handling the mapping of fields, documents and HBase columns. 

#### Experimental results ####

To evaluate the improvements of MapReduce based query processing brought by DOT, we created an 18-column in Hive (on HBase) and load ~567 million rows into the table. The table below compares sizes of the resulting tables; DOT achieves ~3x space reduction compared to normal HBase tables, and ~1.7x reduction compared to fast-diff ([HBase-4218](https://issues.apache.org/jira/browse/HBASE-4218)) encoded tables.

<table border="1">
<tr>
<td/>
<td>File Size (GB)</td>
</tr>
<tr>
<td>HBase</td>
<td>604</td>
</tr>
<tr>
<td>Fast-Diff</td>
<td>320</td>
</tr>
<tr>
<td>DOT</td>
<td>194</td>
</tr>
<tr>
<td>DOT+Fast-Diff</td>
<td>180</td>
</tr>
</table>

Next, the chart below compares the performance of loading data into the table using either bulk load or insert; and DOT achieves ~1.9x speedup for bulk load and 3~4x speedup for insert.

![data load perfromance](/path/to/img.jpg "Data Load Perfromance")
 
Finally, the chart below compares the performance of selecting various numbers of columns form the table:

`select count (col1, clo2, …, clon) from table`

and DOT achieves up-to 2x speedup.

![select perfromance](/path/to/img.jpg "Select Perfromance")
![select perfromance](/path/to/img.jpg "Select Perfromance")
   
Summary
The document store on HBase greatly improves the query processing capabilities on HBase (e.g., ~3x storage reduction and ~2x query speedup). We will contribute the implementation of DOT to the Apache Hadoop community under Project Panthera (<https://github.com/intel-hadoop/project-panthera>). Please refer to [HBase-6800](http://issues.apache.org/jira/browse/HBase-6800) to track our efforts to collaborate with the Hadoop community to get the idea reviewed and hopefully incorporated into Apache HBase.
