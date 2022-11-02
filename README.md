## Iceberg S3FileIO with corruption check

This is a modified version of S3FileIO from Iceberg 1.0.0 that performs a corruption check on Parquet files before
uploading them to S3. The actual check is done in `ParquetUtil.java`, which simply reads and decompresses the data
in the file. (The only code modified from Iceberg is `S3OutputStream.java` besides package naming.)

There are a couple of limitations. The first is that multipart upload is disabled so that the entire file is written
locally first. The second is that it relies on the file having a `.parquet` extension, to avoid checking metadata or
other non-Parquet files.

If you want to try it out, include this jar and set your catalog's FileIO implementation via
a property, e.g. `spark.sql.catalog.tabular.io-impl=io.tabular.iceberg.aws.s3.S3FileIO`.

Disclaimer: this was very lightly tested and not at scale so use at your own risk. This is meant 
more as an example of one way to add a corruption check.
