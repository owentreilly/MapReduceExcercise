/*
 * Problem6.java
 * 
 * CS 460: Problem Set 4
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Problem6 {
    public static class MyMapper
      extends Mapper<Object, Text, Text, Text> 
    {     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      int numFriends = 0;
      
      String[] words = line.split(";");
      String id = words[0].split(",")[0];
      String[] friends = words[1].split(",");
      for (String friend : friends) {
        if(!friend.equals("")){
          numFriends++;
        }
      }
      String str = id + "," + numFriends;
      context.write(new Text("1"), new Text(str));
  }
}

    public static class MyReducer
      extends Reducer<Text, Text, Text, IntWritable> 
      
    {
      public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
  int max = 0;
  String id = "";
  for (Text val : values){
String str = val.toString();
String[] words = str.split(",");
int numFriends = Integer.parseInt(words[1]);

if(numFriends > max){
  max = numFriends;
id = words[0];
  }
  
}
    context.write(new Text(id), new IntWritable(max));
}
  }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 6");
        job.setJarByClass(Problem6.class);

        // Specifies the names of the first job's mapper and reducer classes.
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Sets the types for the keys and values output by the first reducer.
        /* CHANGE THE CLASS NAMES AS NEEDED IN THESE TWO METHOD CALLS */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Sets the types for the keys and values output by the first mapper.
        /* CHANGE THE CLASS NAMES AS NEEDED IN THESE TWO METHOD CALLS */
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Configure the type and location of the data being processed.
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Specify where the results should be stored.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
