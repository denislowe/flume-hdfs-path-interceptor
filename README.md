# flume-hdfs-path-interceptor

## Overiew
This is a custom [Flume-ng](http://flume.apache.org/FlumeUserGuide.html) interceptor and is designed to parse each record within the log file and dynamically generate a HDFS file path based on the extracted element(s).

Flume interceptors are part of the overall flume plugin model allowing developers to modify, add or drop events 'in flight' as they pass through to the flume agent and then eventually to a flume sink eg HDFS.

This interceptor will need to be chained with a corresponding sink to handle the actual writing to HDFS.

For more information, see [Flume Interceptors](http://flume.apache.org/FlumeUserGuide.html#flume-interceptors).

## Example
A log file comprising the following pipe delimited data:

    custid-1|data|impression|2013-01-01|moredata|moredata|etc

Can be parsed by this interceptor and write the file to a dynamically generated path in HDFS based on the elements(s) with the file. 

Eg by extracting elements 0, 2 and 3 from the above log sample we can then generate a path as follows:

	hdfs://impression/custid-1/2013-01-01/filename.gz
	
## Usage

You need to determine the position(s) of the element(s) you wish to extract within the delimited record and assign this element to a 'header' variable.

The below configuration defines the custom interceptor and the mandatory delimiter and a comma seperated list of headers

	agent.sources.impressions.interceptors.hdfs-interceptor.type =	com.cognitivematch.flume.interceptor.HdfsPathInterceptor$Builder	agent.sources.impressions.interceptors.hdfs-interceptor.delimiter = \\t	agent.sources.impressions.interceptors.hdfs-interceptor.headers = 3:site,5:utc-time
The extracted 'header' variables will then be passed to the down stream sink for writing into HDFS (Using header varibles 'site' and 'utc-time')
	agent.sinks.impressions-s3-sink.type = hdfs	agent.sinks.impressions-s3-sink.channel = impressions-s3-channel	agent.sinks.impressions-s3-sink.hdfs.path = s3n://$S3_ACCESS_KEY:$S3_PRIVATE_KEY@$S3_AD_MATCH_ROOT_DIR/batch/impression/site=%{site}/day=%{utc-time}
	
## Compiling and Packaging
The following are prerequisites:
	
	Java JDK 1.6
	Apache Maven
	Flume-ng 1.3.1 or higher
	
The project can be setup and compiled using the following commands:

Eclipse Setup:

	mvn: eclipse:eclipse

compile test and package:

	mvn clean package	
## Deploying to Flume
### Pre Version 1.4.0
Copy the target/flume-hdfs-path-interceptor-1.0-SNAPSHOT.jar jar into $FLUME_HOME/lib

### Version 1.4.0 or above
copy the target/flume-hdfs-path-interceptor-1.0-SNAPSHOT.jar jar into  $FLUME_HOME/plugins.d/flume-hdfs-path-interceptor