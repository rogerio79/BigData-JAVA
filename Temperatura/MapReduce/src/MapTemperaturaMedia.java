import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class MapTemperaturaMedia extends Mapper <LongWritable, Text, IntWritable, FloatWritable>
{
           

           
		public void map(LongWritable key, Text value, Context context) throws                                                                  IOException,  InterruptedException
            {
						
						
	   					float temperatura;
	   					
                        String[] linha = value.toString().split(";"); //coloca cada campo em uma posicao no vetor linha                       
                                                                                                                     
                        String[] aux_ano = linha[1].toString().split("/"); //
                        
                        int mes = Integer.parseInt(aux_ano[1].toString()); 
                        
                                                
                        if (linha[4].length() > 0 && linha[4] != null)
                                    temperatura = Float.parseFloat(linha[4]);
                        else
                        			temperatura = Float.parseFloat(linha[5]);      
                       
                          
                       	context.write(new IntWritable(mes),new FloatWritable(temperatura));
 
            }
}
