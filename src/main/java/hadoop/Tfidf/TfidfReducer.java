package hadoop.Tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class TfidfReducer extends Reducer<Text, Text, Text, Text> {

    public static final String BOOLEAN_FREQ = "boolean";
    public static final String LOGSCALE_FREQ = "logscale";
    public static final String AUGMENTED_FREQ = "augmented";

    private static final DecimalFormat DF = new DecimalFormat("###.########");

    private DoubleWritable result = new DoubleWritable();
    private String param;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        param = conf.get("mode");
    }

    // D = total number of document in corpus. This can be passed by the driver as a constant
    // d = number of documents in corpus where the term appears. It is a counter over the reduced values for each term
    // TFIDF = n/N * log(D/d);
    // (term, document=n/N) --> ((word@document), d/D, (n/N), TFIDF)
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // get the number of documents indirectly from the file-system (stored in the job name on purpose)
        int D = Integer.parseInt(context.getJobName());
        // total frequency of this word
        int d = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        for (Text val : values) {
            String[] documentAndFrequencies = val.toString().split("=");
            d++;
            String term = documentAndFrequencies[0];
            String f = documentAndFrequencies[1];
            tempFrequencies.put(term, f);
        }
        for (String document : tempFrequencies.keySet()) {
            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/"); // n, N (1,1)

            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
            String n = wordFrequenceAndTotalWords[0];
            String N = wordFrequenceAndTotalWords[1];

            double tf = 0;
            if (param.equals(BOOLEAN_FREQ)) {
                tf = 1;
            } else if (param.equals(LOGSCALE_FREQ)) {
                tf = Math.log10(Double.parseDouble(n) + 1);
            } else if (param.equals(AUGMENTED_FREQ)) {
                tf = ((0.5 * Double.parseDouble(n)) / Double.parseDouble(N)) + 0.5;
            }


            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
            double idf = Math.abs((double) D / ((double) d));
            //given that log(10) = 0, just consider the term frequency in documents
            double tfIdf = d == D ? tf : tf * Math.log10(idf);

            context.write(new Text(key + "@" + document), new Text("[" + d + "/"
                    + D + " , " + n + "/"
                    + N + " , " + DF.format(tfIdf) + "]"));
        }
    }
}
