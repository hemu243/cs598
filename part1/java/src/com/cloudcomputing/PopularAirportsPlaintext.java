package com.cloudcomputing;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import static com.cloudcomputing.OnTimePerformanceMetadata.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * This uses airline_ontime data to sum origin-destination airports
 */
public class PopularAirportsPlaintext {

    public static class Mapper
            extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(value.toString())
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= DESTINATION_AIRPORT)
                    .forEach(tokens -> {
                        // FROM
                        try {
                            word.set(tokens[ORIGIN_AIRPORT].replaceAll("\"", ""));
                            context.write(word, one);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                        // TO
                        try {
                            word.set(tokens[DESTINATION_AIRPORT].replaceAll("\"", ""));
                            context.write(word, one);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                    });
        }
    }

    public static class Reducer
            extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class TopPopularAirportMapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Long, String>> countToWordMap =
                new TreeSet<Pair<Long, String>>();

        private final int numberOfTopWords = 10;

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            Long count = Long.parseLong(value.toString());
            String word = key.toString();

            // Hold top 10
            countToWordMap.add(new Pair<Long, String>(count, word));
            if (countToWordMap.size() > numberOfTopWords) {
                countToWordMap.remove(countToWordMap.first());
            }
        }

        @Override protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Long, String> item : countToWordMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopPopularAirportReducer
            extends org.apache.hadoop.mapreduce.Reducer<NullWritable, TextArrayWritable, Text, LongWritable> {
        private TreeSet<Pair<Long, String>> countToWordMap =
                new TreeSet<Pair<Long, String>>();

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
                Long count = Long.parseLong(pair[1].toString());

                countToWordMap.add(new Pair<Long, String>(count, word));
                if (countToWordMap.size() > 10) {
                    countToWordMap.remove(countToWordMap.first());
                }
            }

            Iterator<Pair<Long, String>> iterator = countToWordMap.descendingIterator();
            // Put output
            while(iterator.hasNext()) {
                Pair<Long, String> item = iterator.next();
                Text word = new Text(item.second);
                LongWritable value = new LongWritable(item.first);
                context.write(word, value);
            }
//            for (Pair<Long, String> item : countToWordMap) {
//                Text word = new Text(item.second);
//                LongWritable value = new LongWritable(item.first);
//                context.write(word, value);
//            }
        }
    }

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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./w1/tmp");
        fs.delete(tmpPath, true);

        // First Job
        Job job = Job.getInstance(conf, "Airport flight Count");
        job.setJarByClass(PopularAirportsPlaintext.class);
        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tmpPath);
        job.waitForCompletion(true);

        // Second job
        Job jobB = Job.getInstance(conf, "Top Popular Airports");
        jobB.setJarByClass(PopularAirportsPlaintext.class);
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        // Set input and outclass
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        // Map output class
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopPopularAirportMapper.class);
        jobB.setReducerClass(TopPopularAirportReducer.class);
        jobB.setNumReduceTasks(1);

        int code = jobB.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}
