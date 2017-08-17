package mrEclat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool{
	private static String dataBaseName;
	private static double relativeMinSupport;
	static int minSupport;
	private static String inputPath;
	private static String outputPath;
	private static int dataSize;
	private static int childJavaOpts;
	private static int mapperNum;
	private static int reducerNum;
	private static int groupNum;
	
	public static final String TOTAL_PART = "TotalPart";
	public static final String IT_PART = "ItemsetTidsetPart";
	public static final String PART = "part";
	
	private static ArrayList<Long> eachLeveltotalItemsetsNum = new ArrayList<Long>();
	private static ArrayList<Double> eachLevelRunningTime = new ArrayList<Double>();
	
	private static String[] moreParas;
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("minSupport", minSupport);
		conf.setInt("dataSize", dataSize);
		conf.set("mapreduce.map.java.opts", "-Xmx"+childJavaOpts+"M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx"+2*childJavaOpts+"M");
		conf.set("mapreduce.task.timeout", "6000000");
		conf.setInt("mapperNum", mapperNum);
		
		for( int k = 0; k < moreParas.length && moreParas.length >= 2; k+=2) {
			conf.set( moreParas[ k ], moreParas[ k+1 ] );			
		}
		
		try {
			//delete old output directory
			FileSystem fs = FileSystem.get(URI.create(outputPath),conf);
			if(fs.exists(new Path(outputPath)))
				fs.delete(new Path(outputPath), true);
			
			long startTime = System.currentTimeMillis();
			startFirst(conf);
			startSecond(conf);
			divide(outputPath + "/" + 2 + "/" + TOTAL_PART, conf);
			startThird(conf);
			
			fs.close();
			long endTime=System.currentTimeMillis();
			System.out.println(endTime - startTime);
			saveResult(endTime-startTime);
			
		}catch(Exception e) {
			e.printStackTrace();
	    	File file = new File("MREclat_" + dataBaseName + "_ResultOut");
			BufferedWriter br  = new BufferedWriter(new FileWriter(file, true));  // true means appending content to the file //here create a non-existed file
			br.write("MREclat Exception occurs at minimumSupport(relative) "  + relativeMinSupport);
			br.write("\n");
			br.flush();
			br.close();
		}
		
		return 0;
	}
	
	public void divide(String uri, Configuration conf) {
		HashMap<Integer,int[]> map = new HashMap<Integer,int[]>();
		//HashMap<Integer,Integer> item_w = new HashMap<Integer,Integer>();
		ArrayList<ItemWeight> item_w = new ArrayList<ItemWeight>();
		SequenceFile.Reader reader = null;
		FileSystem fs = null;
		try{
			fs =  FileSystem.get(conf);
			FileStatus[] fileStatus = fs.listStatus(new Path(uri));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			IntArrayWritable key = new IntArrayWritable();
			IntArrayWritable value = new IntArrayWritable();
			for(Path path : paths){
				reader = new SequenceFile.Reader(conf, Reader.file(path));
				while (reader.next(key, value)){
					int item = key.get()[0];
					if(map.containsKey(item)) {
						int[] v = map.get(item);
						v[0]++;
						v[1] += value.get()[0];
					}else {
						map.put(item, new int[] {1,value.get()[0]});
					}
				} 
			}
			double w = 0;
			for(Map.Entry<Integer,int[]> entry : map.entrySet()) {
				if(entry.getValue()[0] > 1) {
					w = Math.log10(entry.getValue()[0] - 1) + Math.log10(entry.getValue()[1]);
					item_w.add(new ItemWeight(entry.getKey(), w));
				}
			}
			
			map = null;
			item_w.sort(new Comparator<ItemWeight>() {
				@Override
				public int compare(ItemWeight arg0, ItemWeight arg1) {
					
					//return (arg0.weigth > arg1.weigth)? -1 : 1;
					return (arg0.weigth == arg1.weigth)? 0 : (arg0.weigth > arg1.weigth)? -1 : 1;
				}
			});
			
			saveItemGroup(outputPath + "/" + "groupNum", item_w, conf, groupNum);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(reader);	
		}
		
	}
	
	public void saveItemGroup(String uri, ArrayList<ItemWeight> item_w, Configuration conf, int N) {
		SequenceFile.Writer writer = null;
		int M = item_w.size();
		try {
			Path path = new Path(uri);
			//IntWritable key = new IntWritable();
			//IntWritable value = new IntWritable();
			
			writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(IntWritable.class));
			if(M <= N) {
				int i = 0;
				for(ItemWeight itemweight : item_w) {
					writer.append(new IntWritable(itemweight.item), new IntWritable(i++));
				}
			}else {
				int j = 0;
				double[] group = new double[N];
				ItemWeight IW;
				for(; j<N; j++) {
					IW = item_w.get(j);
					writer.append(new IntWritable(IW.item), new IntWritable(j));
					group[j] = IW.weigth;
				}
				for(;j<M; j++) {
					int smallIndex = getIndex(group);
					IW = item_w.get(j);
					writer.append(new IntWritable(IW.item), new IntWritable(smallIndex));
					group[smallIndex] += IW.weigth;
					
				}
			}
		
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if (writer != null) {
				IOUtils.closeStream(writer);
			}
		}
		
	}
	
	public int getIndex(double[] group) {
		int j=0;
		for(int i=0; i<group.length; i++) {
			if(group[i]<group[j])
				j=i;
		}
		return j;
		
	}
	
	public void startFirst(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		long startTime = System.currentTimeMillis();
		
		Path inpath = new Path(inputPath);
		Path outpath = new Path(outputPath + "/" + "1");
		Job job = Job.getInstance(conf, "MREclat_First");
		job.setJarByClass(Driver.class);
		
		job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(FirstMapper.class);
        //job.setMapperClass(FirstMapper_bak.class);
        //job.setCombinerClass(FirstCombiner.class);
        job.setReducerClass(FirstReducer.class);
        
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	   
	    job.setNumReduceTasks(reducerNum);
	
        FileInputFormat.addInputPath(job, inpath);
	    FileOutputFormat.setOutputPath(job, outpath);
      
        job.waitForCompletion(true);
        
        long endTime=System.currentTimeMillis(); 
        
        System.out.println(endTime - startTime);
     
        saveEveryJobResult((endTime - startTime), job);
        
	}
	
	public void startSecond(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		long startTime = System.currentTimeMillis();
		
		Path inpath = new Path(inputPath);
		Path outpath = new Path(outputPath + "/" + 2);
		Job job = Job.getInstance(conf, "MREclat_Second");
		job.setJarByClass(Driver.class);
		
		//conf.set("BFOutPath", outputPath + "/" + "InfreItem");
		saveAllToCache(outputPath + "/" + 1 +"/"+IT_PART, job);
		
		FileInputFormat.addInputPath(job, inpath);
	    FileOutputFormat.setOutputPath(job, outpath);
	    
	    job.setNumReduceTasks(reducerNum);
	    
	    //job.setInputFormatClass(SequenceFileInputFormat.class);
	    //job.setInputFormatClass(TextInputFormat.class);
	    job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
	    //job.setInputFormatClass(MyInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
        job.setMapperClass(SecondMapper.class);
        //job.setPartitionerClass(SecondPartition.class);
        job.setReducerClass(SecondReducer.class);
       
        job.setMapOutputKeyClass(IntArrayWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(IntArrayWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);
        
        //addNamedOutput(job, IT_PART, SequenceFileOutputFormat.class, IntArrayWritable.class, IntArrayWritable.class);

		//addNamedOutput(job, TOTAL_PART, SequenceFileOutputFormat.class, IntArrayWritable.class, IntWritable.class);
      
        job.waitForCompletion(true);
        //job.submit();
        long endTime=System.currentTimeMillis(); 
        
        System.out.println(endTime - startTime);
        
        saveEveryJobResult((endTime - startTime), job);
	}
	
	public void startThird(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		long startTime = System.currentTimeMillis();
		
		Path inpath = new Path(outputPath + "/" + 2 + "/" + IT_PART);
		Path outpath = new Path(outputPath + "/" + 3);
		Job job = Job.getInstance(conf, "MREclat_Third");
		job.setJarByClass(Driver.class);
		
		//conf.set("BFOutPath", outputPath + "/" + "InfreItem");
		saveAllToCache(outputPath + "/" + "groupNum", job);
		
		FileInputFormat.addInputPath(job, inpath);
	    FileOutputFormat.setOutputPath(job, outpath);
	    
	    job.setNumReduceTasks(reducerNum);
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    //job.setInputFormatClass(TextInputFormat.class);
	    //job.setInputFormatClass(MyInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
        job.setMapperClass(ThirdMapper.class);
        job.setPartitionerClass(ThirdPartition.class);
        job.setReducerClass(ThirdReducer.class);
       
        job.setOutputKeyClass(KeyWritable.class);
        job.setOutputValueClass(ValueWritable.class);
        
        job.waitForCompletion(true);
        long endTime=System.currentTimeMillis(); 
        
        System.out.println(endTime - startTime);
        
        saveEveryJobResult((endTime - startTime), job);
	}
	
	protected static void saveEveryJobResult(long time, Job job){		
		Counters counters = null;
		try {
			counters = job.getCounters();
		} catch (IOException e) {
			e.printStackTrace();
		}				
		eachLeveltotalItemsetsNum.add(counters.findCounter(MREclatCounter.TotalNum).getValue());
		eachLevelRunningTime.add(time/1000.0);	
	}
	
	public static void saveAllToCache(String dir, Job job) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(dir),job.getConfiguration());
		FileStatus[] stats = fs.listStatus(new Path(dir));
		for(FileStatus file : stats){
			job.addCacheFile(URI.create(file.getPath().toString()));
		}
	}
	
	public static void saveResult(long time) {
		try {
			BufferedWriter br = null;
			long TotalItemsetsNum = 0;
			double TotalJobsRunningTime = 0;
			int k = eachLeveltotalItemsetsNum.size();
			for(int j=0; j<k; j++) {
				TotalItemsetsNum += eachLeveltotalItemsetsNum.get(j);
				TotalJobsRunningTime += eachLevelRunningTime.get(j);
			}

			File resultFile = new File("MREclat_" + dataBaseName + "_ResultOut");
			
			//if( !resultFile.exists() ) {
				br  = new BufferedWriter(new FileWriter(resultFile, true));  
				br.write("algorithmName" + "\t" + "datasetName" + "\t" + "DBSize" + "\t" + "minSuppPercentage(relative)" + "\t" + "minSupp(absolute)" + "\t" + "ChildJavaOpts" + "\t" + "mapperNum" + "\t" + "reducerNum" + "\t" + "groupNum" + "\t"+ "TotalTime" + "\t");
		
				br.write("TotalItemsetsNum" + "\t" + "TotalJobsRunningTime" + "\t");
				
				for(int j=0; j<k; j++){
					br.write("Level" + (j+1) + "TotalItemsetsNum" + "\t" + "Level" + (j+1) + "JobRunningTime" + "\t");
				
				}
				
				for( int i = 0; i<moreParas.length&&moreParas.length > 1; i=i+2)  {
					br.write(moreParas[i] + "\t");
				}
				
				br.write("\n");
				
			/*} else {
				br  = new BufferedWriter(new FileWriter(resultFile, true));  // true means appending content to the file
			}*/
			
			
			br.write("MREclat" + "\t" + dataBaseName + "\t" + dataSize  + "\t" + relativeMinSupport*100.0 + "\t" +  minSupport + "\t" + childJavaOpts + "\t" + mapperNum + "\t" + reducerNum + "\t" + groupNum + "\t" + time/1000.0 + "\t");
			
			br.write(TotalItemsetsNum + "\t" + TotalJobsRunningTime + "\t");
		
			for(int j=0; j<k; j++) {	
				br.write(eachLeveltotalItemsetsNum.get(j) + "\t" +eachLevelRunningTime.get(j) + "\t");
				 	
			}
			
			for( int i = 1; i<moreParas.length&&moreParas.length > 1; i=i+2)  {
				br.write(moreParas[i] + "\t");
			}
			
			br.write("\n");
			br.flush();
			br.close();			
		} catch(IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public Driver(String[] args){//get parameters
		int numFixedParas = 9;
		int numMoreParas = args.length - numFixedParas;
		if(args.length < numFixedParas || numMoreParas % 2 != 0){
			System.out.println("The Number of the input parameters is Wrong!!");
			System.exit(1);
		}else{
			if(numMoreParas > 0 ){
				moreParas = new String[numMoreParas];
				System.arraycopy(args, numFixedParas, moreParas, 0, numMoreParas);
			} else {
				moreParas = new String [1] ;
			}
		}
		dataBaseName = args[0];
		relativeMinSupport = Double.valueOf(args[1]);
		minSupport = (int)Math.ceil(relativeMinSupport * Integer.valueOf(args[4]));
		inputPath = args[2];
		outputPath = args[3];
		dataSize = Integer.valueOf(args[4]);
		childJavaOpts = Integer.valueOf(args[5]);
		mapperNum = Integer.valueOf(args[6]);
		reducerNum = Integer.valueOf(args[7]);
		groupNum = Integer.valueOf(args[8]);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Driver(args), args);
		System.out.println(res);
	}

}
