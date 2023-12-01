import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

public class InversedIndexing {

    // Mapper Class
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        // Set to store keywords passed as configuration
        private Set<String> keywords = new HashSet<>();

        // Method to configure the Mapper with keywords from the job configuration
        public void configure(JobConf job) {
            String[] keywordArray = job.get("keywords").split(",");
            keywords.addAll(Arrays.asList(keywordArray));
        }

        // Map method to process each input record and emit intermediate key-value pairs
        public void map(LongWritable docId, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            // Retrieve file information
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String line = value.toString();

            // Tokenize the line and emit key-value pairs for each keyword
            for (String token : line.split("\\s+")) {
                if (keywords.contains(token)) {
                    output.collect(new Text(token), new Text(fileName));
                }
            }
        }
    }

    // Reducer Class
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        // Reduce method to process intermediate key-value pairs and emit final output
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            // Map to store file names and their occurrence counts
            HashMap<String, Integer> fileCountMap = new HashMap<>();

            // Count occurrences of each file name for the given keyword
            while (values.hasNext()) {
                String fileName = values.next().toString();
                fileCountMap.put(fileName, fileCountMap.getOrDefault(fileName, 0) + 1);
            }

            // Sort file counts in ascending order
            List<java.util.Map.Entry<String, Integer>> sortedFileCounts = new ArrayList<>(fileCountMap.entrySet());
            sortedFileCounts.sort(Comparator.comparingInt(java.util.Map.Entry::getValue));

            // Build a string representation of the result and emit the final output
            StringBuilder resultBuilder = new StringBuilder();
            for (java.util.Map.Entry<String, Integer> entry : sortedFileCounts) {
                resultBuilder.append(entry.getKey()).append(" ").append(entry.getValue()).append(" ");
            }

            output.collect(key, new Text(resultBuilder.toString()));
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        // Record start time for performance measurement
        long startTime = System.currentTimeMillis();

        // Configuration setup for MapReduce job
        JobConf conf = new JobConf(InversedIndexing.class);
        conf.setJobName("inverseInd");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // Set input and output paths from command line arguments
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Set keywords from command line arguments as a configuration parameter
        conf.set("keywords", String.join(",", Arrays.copyOfRange(args, 2, args.length)));

        // Run the MapReduce job
        JobClient.runJob(conf);

        // Print elapsed time for performance measurement
        System.out.println("Elapsed time = " + (System.currentTimeMillis() - startTime) + " ms");
    }
}
