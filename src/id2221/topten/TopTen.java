package id2221.topten;

import java.io.IOException;
import java.util.*;

import com.sun.corba.se.spi.ior.Writeable;
import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import sun.reflect.generics.tree.Tree;

public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, Integer, Text> {
        // Stores a map of user reputation to the record
        TreeMap<Integer, List<Text>> repToRecordMap = new TreeMap<Integer, List<Text> >();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Map<String, String> xmlMap = transformXmlToMap(line);
            if (xmlMap == null) {
                return;
            }
            String id = xmlMap.get("Id");
            int reputation = Integer.parseInt( xmlMap.get("Reputation") );

            if( repToRecordMap.containsKey(reputation) ) {
                repToRecordMap.get(reputation).add(value);
            }
            else {
                List<Text> temp = new ArrayList<Text>();
                temp.add(value);
                repToRecordMap.put(reputation, temp);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            int counter =0;
            for (Map.Entry<Integer, List<Text>> entry : repToRecordMap.entrySet()) {
                if (entry.getValue() != null) {
                    for (Text text : entry.getValue()) {
                        if(counter >= 10 ) return;
                        context.write(entry.getKey() , text);
                        ++counter;
                    }
                }
            }
        }
    }

    public static class TopTenReducer extends TableReducer<Integer, Text, NullWritable> {
        // Stores a map of user reputation to the record
        private TreeMap<Integer, List<Text> > repToRecordMap = new TreeMap();

        public void reduce(Integer key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            if( repToRecordMap.containsKey(key) ) {
                repToRecordMap.get(key).add(values);
            }
            else {
                List<Text> temp = new ArrayList<Text>();
                temp.add(value);
                repToRecordMap.put(reputation, temp);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Job job = Job.getInstance(conf, "TopTen");
        job.setJarByClass(TopTen.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);

        TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
