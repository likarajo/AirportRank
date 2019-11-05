# AirportRank
Search for top airports based on the self implemented Page Rank values computed using their connections data

## Steps

### Upload files to Amazon S3

-	Create a bucket in Amazon S3

-	Upload the “airportrank_2.11-0.1.jar” to Amazon S3. 
The jar file is inside the target/scala-2.11 folder

-	Upload the “airport_data.csv” file to S3 bucket
s3://BUCKET/AirportData/airport_data.csv

### Run the jar file in Amazon EMR

-	Create a cluster in Amazon EMR having Spark 2.4.0, and add steps to be run

-	Add step to run class “AirportRank” with arguments ‘input_file_location’, ‘iterations’, and ‘output_directory_location’

### Verify the output files
The output of AirportRank gets stored in specified output folder as well as ‘stdout’

-	The stdout files can be accessed here: *LogURI/containers/application_id/container_id/stdout.gz* <br>
Navigate to Log URI > Navigate to Containers/ > Navigate to application_id/ > Navigate to container_id/ > View stdout.gz

 - A new folder named output is created under the specified output folder (passed as argument3).<br>
s3://<bucket>/AirportData/output<br>
Inside the output folder are the generated part-0000x files.
