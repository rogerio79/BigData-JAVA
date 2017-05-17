import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class ReduceTemperaturaMedia extends Reducer <IntWritable, FloatWritable,IntWritable, FloatWritable > {
	public void reduce(IntWritable key,  Iterable<FloatWritable> values, Context context) throws IOException,                                                       InterruptedException
            {          
			Float temperaturas = new Float(0);
			
			int contador = 0;
            
            
            for (FloatWritable value : values) {
            	temperaturas += value.get();
            	contador += 1;            	
            }
            try {
            	temperaturas = temperaturas / contador;
            } finally {
            	contador = 1;
            }
            
            context.write(key, new FloatWritable(temperaturas));
           
    }                                             
}
