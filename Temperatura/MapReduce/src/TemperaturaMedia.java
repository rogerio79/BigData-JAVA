import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TemperaturaMedia 
{
           
            public static void main (String[] args) throws Exception
            {
                        if (args.length != 2)
                        {
                               System.err.println("Favor informar os diretorios de entrada e saida dos dados");
                               System.exit(-1);
                        } 
                       
                     
                      Configuration conf = new Configuration();
                      Job job = Job.getInstance(conf, "temp media");
                      job.setJarByClass(TemperaturaMedia.class);
                       
                      FileInputFormat.addInputPath(job,new Path(args[0]));
                      FileOutputFormat.setOutputPath(job,new Path (args[1]));
                       
                      job.setMapperClass(MapTemperaturaMedia.class);
                      job.setReducerClass(ReduceTemperaturaMedia.class);
                       
                      job.setOutputKeyClass(IntWritable.class);
                      job.setOutputValueClass(FloatWritable.class);
                       
                      System.exit(job.waitForCompletion(true)?0:1);                                             
            }
}