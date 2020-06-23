package com.cloudcomputing;

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

public class BestFlightOnAGivenDateQuery {
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

    public static class BestFlightMapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Double, String>> countToWordMap =
                new TreeSet<Pair<Double, String>>();

        private final int numberOfTopWords = 1;

        private String from;
        private String am;
        private String to;
        private String date;

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            from = conf.get("query.airport.from");
            am = conf.get("query.amorpm");
            to = conf.get("query.airport.to");
            date = conf.get("query.date");
        }

        @Override public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String compositeKey = key.toString();
            String compositeValue = value.toString();

            String[] items = compositeKey.split(" ");
            //LOG.info("************** Key:"+ key.toString() + ", source=" +source + " item=0=" + fromTo[0] + " dst=" +dst +" item-1="+ fromTo[1]);
            // Check source and dst
            // TODO - compare from to
            if(!(items[0].trim().equals(from) && items[1].trim().equals(to) && items[2].trim().equals(date) && items[3].trim().equals(am))){
                return;
            }
            String[] valuesItems = compositeValue.split(" ");
            Double arrivalDelay = Double.parseDouble(valuesItems[3]);

            String word = compositeKey + " " + valuesItems[0] + " " + valuesItems[1] + " " + valuesItems[2];
            // Hold top value
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

    public static class BestFlightReducer extends org.apache.hadoop.mapreduce.Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {
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
                if (countToWordMap.size() > 1) {
                    countToWordMap.remove(countToWordMap.last());
                }
            }

            // Return best flight
            Pair<Double, String> item = countToWordMap.first();
            Text word = new Text(item.second);
            DoubleWritable value = new DoubleWritable(item.first);
            context.write(word, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("query.airport.from", args[2]);
        conf.set("query.airport.to", args[3]);
        conf.set("query.date", args[4]);
        conf.set("query.amorpm", args[5]);

        // Second job
        Job jobB = Job.getInstance(conf, com.cloudcomputing.BestFlightOnAGivenDateQuery.class.getName());
        jobB.setJarByClass(BestFlightOnAGivenDateQuery.class);
        FileInputFormat.setInputPaths(jobB, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        // Set input and outclass
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        // Map output class
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        // Map output class
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(BestFlightMapper.class);
        jobB.setReducerClass(BestFlightReducer.class);
        jobB.setNumReduceTasks(1);

        int code = jobB.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}
