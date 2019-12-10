package hadoop.Frequency;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class FrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    // (document, each line contents) --> (word@document, 1)
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        StringTokenizer itr = new StringTokenizer(value.toString());
        StringBuilder stringBuilder = new StringBuilder();

        while (itr.hasMoreTokens()) {
            stringBuilder.append(itr.nextToken());
            stringBuilder.append("@");
            stringBuilder.append(fileName);
            word.set(stringBuilder.toString());
            context.write(word, one);
        }
    }
}
