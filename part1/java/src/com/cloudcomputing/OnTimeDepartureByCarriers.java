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
/**
 * This uses airline_ontime data to determine on-time departure performance by airports
 */
public class OnTimeDepartureByCarriers {

    public static class MyMapper
            extends Mapper<Object, Text, TupleTextWritable, DoubleWritable> {
        private TupleTextWritable airportAndCarrier = new TupleTextWritable();
        private DoubleWritable departureDelay = new DoubleWritable();
        private String queryAirport;

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            queryAirport = conf.get("query.airport");
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Tokenize the content
            Stream.of(value.toString())
                    .map(line -> line.split(","))
                    .filter(tokens -> tokens.length >= DEPARTURE_DELAY)
                    .forEach(tokens -> {
                        try {
                            String carrierValue = tokens[CARRIER_ID].replaceAll("\"", "");
                            String airportValue = tokens[ORIGIN_AIRPORT].replaceAll("\"", "");
                            String delayValue = tokens[DEPARTURE_DELAY].replaceAll("\"", "");
                            // Skip empty values
                            if (delayValue.isEmpty()) {
                                return;
                            }
                            // Skip other airport too
                            if (!airportValue.equals(queryAirport)) {
                                return;
                            }
                            airportAndCarrier.setFirstKey(airportValue);
                            airportAndCarrier.setSecondKey(carrierValue);

                            departureDelay.set(Double.parseDouble(delayValue));
                            context.write(airportAndCarrier, departureDelay);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                    });
        }
    }

    public static class MyReducer
            extends Reducer<TupleTextWritable, DoubleWritable, TupleTextWritable, Text> {
        public void reduce(TupleTextWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = sum / count;
            context.write(key, new Text(String.format("%.2f", average)));
        }
    }


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

    public static class TopCarriesMapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Double, String>> countToWordMap =
                new TreeSet<Pair<Double, String>>();

        private final int numberOfTopWords = 10;

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
        }

        @Override public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            Double arrivalDelay = Double.parseDouble(value.toString());
            String word = key.toString();

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

    public static class TopCarriesReducer extends org.apache.hadoop.mapreduce.Reducer<NullWritable, TextArrayWritable, Text, DoubleWritable> {
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
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./w1/tmp");
        fs.delete(tmpPath, true);

        // Standard stuff
        Job job = Job.getInstance(conf, OnTimeDepartureByCarriers.class.getName());
        job.setJarByClass(OnTimeDepartureByCarriers.class);
        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(Reducer.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(TupleTextWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.waitForCompletion(true);

        // Second job
        Job jobB = Job.getInstance(conf, "Top Carries from a given airport");
        jobB.setJarByClass(OnTimeDepartureByCarriers.class);
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        // Set input and outclass
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        // Map output class
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopCarriesMapper.class);
        jobB.setReducerClass(TopCarriesReducer.class);
        jobB.setNumReduceTasks(1);

        int code = jobB.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}
