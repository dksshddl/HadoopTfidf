package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.Tfidf.TfidfMapper;
import hadoop.Tfidf.TfidfReducer;
import hadoop.WordCount.WordCountMapper;
import hadoop.WordCount.WordCountReducer;
import hadoop.Frequency.FrequencyMapper;
import hadoop.Frequency.FrequencyReducer;

public class HadoopTfidf {

    public static void main(String[] args) throws Exception {
        Path input = null;
        Path output = null;
        Configuration conf = new Configuration();
        // initial state
        if (args.length == 2) {
            // default mode
            conf.set("mode", TfidfReducer.AUGMENTED_FREQ);
            input = new Path(args[0]);
            output = new Path(args[1]);
        } else if (args.length == 3) {
            // selected mode
            if (!args[0].equals(TfidfReducer.AUGMENTED_FREQ) && !args[0].equals(TfidfReducer.BOOLEAN_FREQ)
                    && !args[0].equals(TfidfReducer.LOGSCALE_FREQ)) {
                System.out.println("Invalid Value : " + args[0]);
                System.out.println("Usage: hadoop.WordCount [mode] <input> <output>");
                System.out.println("    [mode] : boolean, logscale, augmented");
                System.exit(2);
            } else {
                conf.set("mode", args[0]);
                input = new Path(args[1]);
                output = new Path(args[2]);
            }
        } else {
            System.out.println("Usage: hadoop.WordCount [mode] <input> <output>");
            System.out.println("    [mode] : boolean, logscale, augmented");
            System.exit(2);
        }

        Job job1 = new Job(conf, "Word Frequence In Document");
        job1.setJarByClass(HadoopTfidf.class);
        job1.setMapperClass(FrequencyMapper.class);
        job1.setReducerClass(FrequencyReducer.class);
        job1.setCombinerClass(FrequencyReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        Path wordFreqOutput = new Path("/tmp/wordFreq");
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, wordFreqOutput);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(wordFreqOutput))
            hdfs.delete(wordFreqOutput, true);

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Word Counts");
        job2.setJarByClass(HadoopTfidf.class);
        job2.setMapperClass(WordCountMapper.class);
        job2.setReducerClass(WordCountReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        Path wordCounts = new Path("/tmp/wordCount");
        FileInputFormat.addInputPath(job2, wordFreqOutput);
        FileOutputFormat.setOutputPath(job2, wordCounts);

        if (hdfs.exists(wordCounts))
            hdfs.delete(wordCounts, true);

        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "Word in Corpus, TF-IDF");
        job3.setJarByClass(HadoopTfidf.class);
        job3.setMapperClass(TfidfMapper.class);
        job3.setReducerClass(TfidfReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, wordCounts);
        FileOutputFormat.setOutputPath(job3, output);

        FileSystem fs = input.getFileSystem(conf3);
        FileStatus[] stat = fs.listStatus(input);

        job3.setJobName(String.valueOf(stat.length));

        if (hdfs.exists(output))
            hdfs.delete(output, true);

        job3.waitForCompletion(true);
    }
}
