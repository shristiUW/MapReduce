# MapReduce
 Inverted Indexing with Hadoop MapReduce
The program implements an inverted indexing algorithm using the Hadoop MapReduce paradigm. The primary goal is to efficiently process large datasets, creating an inverted index that maps keywords to the documents where they appear. Inverted indexing is a common technique used in information retrieval to speed up searches for specific terms in a collection of documents.
Mapper Class (Map):
- Purpose: Tokenizes input documents and emits key-value pairs for each keyword found in the
specified set of keywords.
- Configuration: Receives a set of keywords as configuration parameters.
- Processing: Reads each line from the input, extracts the filename, and emits key-value pairs for
keywords present in the line.
Reducer Class (Reduce):
- Purpose: Aggregates the intermediate key-value pairs produced by the Mapper to create an inverted
index.
- Processing:Counts the occurrences of each filename for a given keyword, sorts the results by count
in ascending order, and emits the final inverted index.
Main Method:
- Configuration Setup: Initializes Hadoop job configuration, specifying input and output formats,
mapper and reducer classes, and key-value types.
- Input and Output Paths:Sets the input and output paths based on command line arguments.
- Keyword Configuration: Passes the set of keywords as a configuration parameter to the job.
- Job Execution: Executes the Hadoop MapReduce job.
Execution:
- Input: A collection of documents.
- Output: Inverted index, where each keyword is associated with a list of filenames and their respective
counts.
- Performance Measurement: Records and prints the elapsed time for the MapReduce job.
Usage:
- The program is run from the command line with input and output paths and a list of keywords.
- Example command: `hadoop jar InversedIndexing.jar input output keyword1,keyword2,keyword3`
Overall Flow:
- The MapReduce job processes input documents, identifies occurrences of specified keywords, and produces an inverted index with keywords and associated filenames and counts.
