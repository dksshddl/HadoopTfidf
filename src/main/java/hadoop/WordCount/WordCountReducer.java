package hadoop.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

    private IntWritable result = new IntWritable();

     // N = totalWordsIndoc = sum [word=n]) for each document
     // (document, word=n) --> ((word@document), (n/N))
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int N = 0;
        Map<String, Integer> tempCounter = new HashMap<String, Integer>();

        for (Text val : values) {
            String[] wordCounter = val.toString().split("="); // word, n
            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
            N += Integer.parseInt(val.toString().split("=")[1]);
        }

        for (String wordKey : tempCounter.keySet()) {
            context.write(new Text(wordKey + "@" + key.toString()), new Text(tempCounter.get(wordKey) + "/"
                    + N));
        }
    }
}
