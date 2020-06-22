package com.cloudcomputing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.TreeSet;

public class OnTimeArrivalByAirportsQuery {

    private static final Log LOG = LogFactory.getLog(OnTimeArrivalByAirportsQuery.class);

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class QueryArrivalByAirportMapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Double, String>> countToWordMap =
                new TreeSet<Pair<Double, String>>();

        private final int numberOfTopWords = 10;
         private String source;
         private String dst;

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            source = conf.get("query.airport.from").trim();
            dst = conf.get("query.airport.to").trim();
        }


        @Override public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            Double arrivalDelay = Double.parseDouble(value.toString());
            String[] fromTo = key.toString().split(" ");
            //LOG.info("************** Key:"+ key.toString() + ", source=" +source + " item=0=" + fromTo[0] + " dst=" +dst +" item-1="+ fromTo[1]);
            // Check source and dst
            if(fromTo[0].trim().equals(source) && fromTo[1].trim().equals(dst)) {
                String[] strings = {key.toString(), value.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }

    }

    public static class QueryArrivalByAirportReducer extends org.apache.hadoop.mapreduce.Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {
        private TreeSet<Pair<Double, String>> countToWordMap =
                new TreeSet<Pair<Double, String>>();

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            // Avg delay

            for (TextArrayWritable val : values) {
                Text[] pair = (Text[]) val.toArray();
                String word = pair[0].toString();
                Double arrivalDelay = Double.parseDouble(pair[1].toString());

                context.write(new Text(word), new DoubleWritable(arrivalDelay));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("query.airport.from", args[2]);
        conf.set("query.airport.to", args[3]);
        Job jobB = Job.getInstance(conf, OnTimeArrivalByAirportsQuery.class.getName());
        jobB.setJarByClass(OnTimeArrivalByAirportsQuery.class);
        FileInputFormat.setInputPaths(jobB, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        // Set input and outclass
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        // Map output class
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(QueryArrivalByAirportMapper.class);
        jobB.setReducerClass(QueryArrivalByAirportReducer.class);
        jobB.setNumReduceTasks(1);

        int code = jobB.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}
