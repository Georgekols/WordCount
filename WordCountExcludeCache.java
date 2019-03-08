import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountExcludeCache
{

    // The mapper class
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {

        ArrayList<String> exWords = new ArrayList<>();

        public void setup(Context context) throws IOException {


            try
            {
                URI[] cacheFiles = context.getCacheFiles();
                String[] fn=cacheFiles[0].toString().split("#");
                String line;
                BufferedReader in = new BufferedReader(new FileReader(fn[0]));

                while ((line = in.readLine()) != null)
                {
                    exWords.add(line.trim());
                }
                in.close();
            }
            catch (Exception e)
            {
                System.err.println("Problems, problems: " + e);
            }
        }

        /**
         * Processes a line that is passed to it by writing a key/value pair to the context.
         *
         * @param index 	A link to the file index
         * @param value 	The line from the file
         * @param context 	The Hadoop context object
         */
        public void map(Object index, Text value, Context context) throws IOException, InterruptedException
        {

            String sentence = value.toString();
            sentence = sentence.replaceAll("[\\.$|,]", "");
            sentence = sentence.toLowerCase();

            StringTokenizer itr = new StringTokenizer(sentence);	// Tokenizer for line from file

            while (itr.hasMoreTokens())
            {
                String word = itr.nextToken();
                if(exWords.indexOf(word) == -1) {
                    // Write, i.e. emit word as key and 1 as value (IntWritable(1))
                    context.write(new Text(word), new IntWritable(1));
                }
            }
        }

//	    public void setup(Context context)
//	    {
//	    	System.out.println("Setting up mapper");
//	    }
//
//	    public void cleanup(Context context)
//	    {
//	    	System.out.println("Cleaning up mapper");
//	    }
    }

    // The Reducer class
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        /**
         * Reduces multiple data values for a given key into a single output for that key
         *
         * @param key 		The key that this particular reduce call will be operating on
         * @param values 	An array of values associated with the given key
         * @param context	The Hadoop context object
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;

            // Iterate through the values for the given key and sum them (essentially add
            // all the 1s, so count, except that it might be called more than once (or combined),
            // so must be sum, not ++1)
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            // Set value of result to sum
            IntWritable result = new IntWritable(sum);
            // Emit key and result (i.e word and count).
            context.write(key,result);
        }

//	    public void setup(Context context)
//	    {
//	    	System.out.println("Setting up reducer");
//	    }
//
//	    public void cleanup(Context context)
//	    {
//	    	System.out.println("Cleaning up reducer");
//	    }

    }

    //Main program to run
    /**
     * main program that will be run, including configuration setup
     *
     * @param args		Command line arguments
     */
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCountExcludeCache");

        job.setJarByClass(WordCountExcludeCache.class);
        
        job.addCacheFile(new URI("/users/gko/WordCount/exclude.txt#file"));


        // Set mapper class to TokenizerMapper defined above
        job.setMapperClass(TokenizerMapper.class);

        // Set combine class to IntSumReducer defined above
        job.setCombinerClass(IntSumReducer.class);

        // Set reduce class to IntSumReducer defined above
        job.setReducerClass(IntSumReducer.class);

        // Class of output key is Text
        job.setOutputKeyClass(Text.class);

        // Class of output value is IntWritable
        job.setOutputValueClass(IntWritable.class);

        // Input path is first argument when program called
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Output path is second argument when program called
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // waitForCompletion submits the job and waits for it to complete,
        // parameter is verbose. Returns true if job succeeds.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



