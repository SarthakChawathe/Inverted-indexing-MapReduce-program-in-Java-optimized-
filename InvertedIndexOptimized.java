import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text documentInfo = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String docID = tokenizer.nextToken();
            documentInfo.set(docID);

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
                if (!word.toString().isEmpty()) {
                    context.write(word, documentInfo);
                }
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> documentCountMap = new HashMap<>();

            for (Text val : values) {
                String docID = val.toString();
                documentCountMap.put(docID, documentCountMap.getOrDefault(docID, 0) + 1);
            }

            StringBuilder docCounts = new StringBuilder();
            for (Map.Entry<String, Integer> entry : documentCountMap.entrySet()) {
                docCounts.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
            }

            result.set(docCounts.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(InvertedIndexReducer.class); // Use reducer as combiner
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
