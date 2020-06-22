package com.cloudcomputing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.TreeSet;
import java.util.stream.Stream;

import static com.cloudcomputing.OnTimePerformanceMetadata.*;
import static com.cloudcomputing.OnTimePerformanceMetadata.DEPARTURE_DELAY;

/**
 * This uses airline_ontime data to determine on-time departure performance by airports
 */
public class OnTimeDepartureByAirportsQuery {
    /**
     * Second job map and reducer
     */
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

    public static class TopTenDepMapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Double, String>> countToWordMap =
                new TreeSet<Pair<Double, String>>();

        private final int numberOfTopWords = 10;

        private String queryAirport;

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            queryAirport = conf.get("query.airport");
        }

        @Override public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            Double arrivalDelay = Double.parseDouble(value.toString());
            String word = key.toString();

            String[] fromTo = key.toString().split(" ");
            //LOG.info("************** Key:"+ key.toString() + ", source=" +source + " item=0=" + fromTo[0] + " dst=" +dst +" item-1="+ fromTo[1]);
            // Check source and dst
            if(!fromTo[0].trim().equals(queryAirport)) {
                return;
            }

            // Hold top 10
            countToWordMap.add(new Pair<Double, String>(arrivalDelay, word));
            if (countToWordMap.size() > numberOfTopWords) {
                // Remove highest
                countToWordMap.remove(countToWordMap.last());
            }
        }

        @Override protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Double, String> item : countToWordMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopTenDepReducer extends org.apache.hadoop.mapreduce.Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {
        private TreeSet<Pair<Double, String>> countToWordMap =
                new TreeSet<Pair<Double, String>>();

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sort
            for (TextArrayWritable val : values) {
                Text[] pair = (Text[]) val.toArray();
                String word = pair[0].toString();
                Double arrivalDelay = Double.parseDouble(pair[1].toString());

                countToWordMap.add(new Pair<Double, String>(arrivalDelay, word));
                if (countToWordMap.size() > 10) {
                    countToWordMap.remove(countToWordMap.last());
                }
            }

//            Iterator<Pair<Long, String>> iterator = countToWordMap.descendingIterator();
//            // Put output
//            while(iterator.hasNext()) {
//                Pair<Long, String> item = iterator.next();
//                Text word = new Text(item.second);
//                LongWritable value = new LongWritable(item.first);
//                context.write(word, value);
//            }
            for (Pair<Double, String> item : countToWordMap) {
                Text word = new Text(item.second);
                DoubleWritable value = new DoubleWritable(item.first);
                context.write(word, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("query.airport", args[2]);

        // Second job
        Job jobB = Job.getInstance(conf, OnTimeDepartureByAirportsQuery.class.getName());
        jobB.setJarByClass(OnTimeDepartureByCarriers.class);
        FileInputFormat.setInputPaths(jobB, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        // Set input and outclass
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        // Map output class
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopTenDepMapper.class);
        jobB.setReducerClass(TopTenDepReducer.class);
        jobB.setNumReduceTasks(1);

        int code = jobB.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}
