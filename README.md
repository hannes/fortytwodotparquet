# 42.parquet - A Zip Bomb for the Big Data Age

[Apache Parquet](https://parquet.apache.org) has become the de-facto standard for tabular data interchange. It is greatly superior to its scary cousin CSV by using a binary, columnar and *compressed* data representation. In addition, Parquet files comes with enough metadata so that files can be correctly interpreted without additional information. Most modern data tools and services support reading and writing Parquet files. 

However, Parquet files are not without their dangers: For example, corrupt files can crash readers that are not being very careful in interpreting internal offsets and such. But even perfectly valid files can be problematic and lead to crashes and service downtime as we will show below. 

A pretty well-known attack on naive firewalls and virus scanners is a [Zip Bomb](https://en.wikipedia.org/wiki/Zip_bomb), one famous example being [42.zip](https://www.unforgettable.dk), named so because of course [42 is the perfect number](https://en.wikipedia.org/wiki/42_(number)#The_Hitchhiker's_Guide_to_the_Galaxy) and the file is only 42 Kilobytes large. This perfectly-valid zip file has a bunch of other zip files in it, which again contain other zip files and so on. Eventually, if one would try to unpack all of that, you would end up with 4 Petabytes of data. Big Data indeed.

Parquet files support various methods to compress data. How big of a table can one create with a Parquet file that is only 42 Kilobytes large in the spirit of a zip bomb? Let's find out! For reasons of portability, we have implemented our own [Parquet reader and writers for DuckDB](https://duckdb.org/docs/data/parquet/overview.html). It is unavoidable to learn a great deal about the Parquet format when implementing it. 

A parquet file is made up of one or more row groups, which contain columns, which in turn contain so-called pages that contain the actual data in encoded format. Among other encodings, Parquet supports [dictionary encoding](https://en.wikipedia.org/wiki/Dictionary_coder), where we first have a page with a dictionary, followed by data pages that refer to the dictionary instead of containing plain values. This is more efficient for columns where long values such as categorical strings repeat often, because the dictionary references can be much smaller. 

Let's exploit that. We write a dictionary with a single value and refer to it over and over. In our example, we use a single 64 bit integer, the biggest possible value because why not. Then, we refer back to this dictionary entry using the `RLE_DICTIONARY` [run-length encoding](https://en.wikipedia.org/wiki/Run-length_encoding) specified in parquet. The [specified encoding](https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3) is a bit weird because for some reason it combines bit packing and run-length encoding but essentially we can use the biggest run-length possible which is `2^31-1`, a little over 2 Billion. Since the dictionary is tiny (one entry), the value we repeat is 0, referring to the only entry. Including its required metadata headers and footers (like all metadata in Parquet, this is encoded using [Thrift](https://thrift.apache.org)), this file is only 133 Bytes large. 133 Bytes to represent 2 Billion 8-Byte integers is not too bad, even if they're all the same.

But we can go up from there. Columns can contain multiple pages referring to *the same* dictionary, so we can just repeat our data page over and over, each time only adding 31 Bytes to the file, but 2 Billion values to the table the file represents. We can also use another trick to blow up the data size: As mentioned, Parquet files contain one or more row groups, those are stored in a Thrift footer at the end of the file. Each column in this row group contains byte offsets (`data_page_offset` and friends) into the file where the pages for the columns are stored.  Nothing keeps us from adding multiple row groups that *all refer to the same byte offset*, the one where we stored our slightly mischievous dictionary and data pages. Each row group we add logically repeats all the pages. Of course, adding row groups also requires metadata storage, so there is some sort of trade-off between adding pages (2 Billion values) and row groups (2x whatever other row group it duplicates). 

With some fiddling, we found that if we repeat the data page 1000 times and repeat the row group 290 times, we end up with [a Parquet file](https://github.com/hannes/fortytwodotparquet/raw/main/42.parquet) that is 42 Kilobytes large, yet contains *622 Trillion* values (622,770,257,630,000 to be exact). If one would materialize this table in memory, it would require over *4 Petabytes* of memory, finally a real example of [Big Data](https://motherduck.com/blog/big-data-is-dead/). 

We've made the [script that we use to generate this file available as well](https://github.com/hannes/fortytwodotparquet/blob/main/create-parquet-file.py), we hope it can be used to test Parquet readers better. We hope to have shown that Parquet files can possible be considered harmful and should certainly not be shoved into some pipeline without being extra careful.











