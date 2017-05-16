import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class ReduceTemperaturaMedia extends Reducer <IntWritable, FloatWritable,IntWritable, IntWritable > {
	public void reduce(IntWritable key,  Iterable<FloatWritable> values, Context context) throws IOException,                                                       InterruptedException
            {          
	    int temperaturas = 0; 
            int cont = 0;
            
            for (IntWritable value : values) {
            	temperaturas += value.get();     
                	cont+=1;
            }
            context.write(key, new IntWritable(temperaturas/cont));
           
    }                                             
}
