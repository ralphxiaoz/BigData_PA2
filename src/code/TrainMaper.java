package code;

import java.io.BufferedReader;
import java.lang.Math;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import util.StringIntegerList;

import com.sun.org.apache.xml.internal.utils.URI;


public class TrainMaper {
	
	public static class TrainMapper extends Mapper<LongWritable, Text, Text, Text> {
		public static int count_total=0;//count the total number of the professions
		public static Map<String,String> name_profession = new HashMap<String,String>();//stroe the pair of <name,profession>
		
		/**
		 * Read in profession.txt here
		 * Put the name,profession pair in the map
		 * 
		 */
		protected void setup(Mapper<LongWritable,Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			Path[] localPath=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			File f =new File(localPath[0].toString());
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line= new String("");
			while((line=br.readLine())!= null)
			{
				int loca=line.indexOf(":");
				String name=line.substring(0, loca).trim();
				String profession=line.substring(loca+1).trim();
				name_profession.put(name, profession);
				//load name into set
			}
			br.close();
			super.setup(context);
			}
		/**
		 * Write the count of total profession number to the HDFS
		 */
		public void cleanup(Context context) throws IOException
		{
			FileSystem fs=FileSystem.get(context.getConfiguration());
			Path pt=new Path("/user/hadoop11/assignment2/count_total.txt");
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			br.write(Integer.toString(count_total));
			br.close();
			
		}
		
		/**
		 *  Input is the lemma_index from PA1. Key is offset of the entry, Text is like "name ,<lemma,times>,<lemma,time>...>.
		 *  Map the name from lemma_index with the profession. Output a bunch of <profession,<lemma,times>,<lemma,times>.....> 
		 *  The number it produce is decided by how many profession that name has in profession_train.txt
		 */
		@Override
		public void map(LongWritable articleId, Text indices, Context context) throws IOException,InterruptedException 
		{
			Set dict=new HashSet<String>();
			String indices_str=indices.toString();
			int key_index=indices_str.indexOf('<');
			String name;
			if(key_index>=0)
				 name=indices_str.substring(0, key_index);//get the key and clean it from indices
			else
				return;
			indices_str=indices_str.replace(name,"");
			name=name.trim();
			String profession=name_profession.get(name);
			if(profession==null)
				return;
			while (!profession.isEmpty())
			{
				count_total+=1;
				int loca=profession.indexOf(',');
				String temp;
				if (loca <0)  //if there is only 1 profession left in the string
				{
					temp=profession.trim();
					profession="";
				}
				else
				{
					temp=profession.substring(0,loca+1);
					profession=profession.replace(temp, ""); //Remove the profession we have got
					temp=temp.replace("," ,"");
					temp=temp.trim();
				}
				context.write(new Text(temp), new Text(indices_str));
				//key=profession value="<lemma,times>,<lemma,times>...."
			}
		}
	}

	public static class TrainReducer extends Reducer<Text, Text, Text, StringIntegerList> {
		static int count_total;
		public void setup(Context context) throws IOException
		{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path pt=new Path("/user/hadoop11/assignment2/count_total.txt");
			BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(pt)));
			String number=br.readLine();
			count_total=Integer.parseInt(number);
		}
		
		/**
		 * For every profession(as a key),count the frequency of all the lemmas in its value
		 * output a profession_lemma.txt, by the way attach the p(Y) with the key. So the output
		 * P(y)=count_this_profession/count_total
		 * format is like 
		 *   key=profession,P(Y)
		 *   value=<lemma,final_times>,<lemma,final_times>....
		 */
		@Override
		public void reduce(Text profession, Iterable<Text> word_freq, Context context)
				throws IOException, InterruptedException {
			Map<String,Integer> word_map=new HashMap<String,Integer>();
			int count_this_profession=0;
			for (Text it : word_freq)
			{
				String content=it.toString();
				if (content==null)
					return;
				count_this_profession+=1;
				while(!content.isEmpty())
				{
					
					int loca_begin=content.indexOf('<');
					int loca_end=content.indexOf('>');
					String tuple= content.substring(loca_begin+1, loca_end);
					int loca_mid=tuple.indexOf(',');
					String word= tuple.substring(0,loca_mid);
					int times=Integer.parseInt(tuple.substring(loca_mid+1, tuple.length()));
					content=content.replace("<"+tuple+">","");
					content=content.replaceFirst(",", "");
					if (word_map.get(word)!=null)
						word_map.put(word, word_map.get(word)+times);
					else
						word_map.put(word, times);
				}
			}
			//count the P(y) here change the formate of key
			StringIntegerList SIL=new StringIntegerList(word_map);
			double py=Math.log(count_this_profession/(double)count_total);
			if (Double.isInfinite(py))
				throw new RuntimeException("Py is Infinity! count_total="+count_total+"count_this_profession="+count_this_profession);
			profession=new Text(profession.toString()+','+Double.toString(py));
			context.write(profession, SIL);
		}
	}

	public static void main(String[] args) {
		// TODO: you should implement the Job Configuration and Job call
		// here
		Job job;
		try {
			job = new Job(new Configuration());		
			job.setJobName("Assignment2_training");
			job.setJarByClass(TrainMaper.class);
			job.setMapperClass(TrainMapper.class);
			job.setReducerClass(TrainReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(StringIntegerList.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			//job.addCacheFile(new Path("/home/jinfenglin/Downloads/profession_train.txt").toUri());
			//DistributedCache.addCacheFile(new Path("/home/jinfenglin/Downloads/profession_train.txt").toUri(), job.getConfiguration());
			DistributedCache.addCacheFile(new Path("/user/hadoop11/assignment2/profession_train.txt").toUri(), job.getConfiguration());
			
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.getConfiguration().set("mapreduce.job.queuename","hadoop11");
			job.waitForCompletion(true);
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
