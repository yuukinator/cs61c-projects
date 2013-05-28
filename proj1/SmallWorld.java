/*
  CS 61C Project1: Small World

  Name: Yuuki Ota
  Login: aq

  Name: Denise Doan
  Login: ar
 */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum dept for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE};    

    //Hashmap
    //    public static Array;

    public static ArrayList<String> globalOriginsList
	= new ArrayList<String>();

    // Example writable type
    public static class EValue implements Writable {
        public ValueUse use;
        public long value;

        public EValue(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public EValue() {
            this(ValueUse.EDGE, 0);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeUTF(use.name());
            out.writeLong(value);
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            use = ValueUse.valueOf(in.readUTF());
            value = in.readLong();
        }

        public void set(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public String toString() {
            return use.name() + ": " + value;
        }
    }

    public static class ELabel implements Writable {
	MapWritable distances;
	LongWritable mark;
	Text children;
	
	public ELabel(MapWritable distances,
		      long mark, Text children) {
	    this.distances = distances;
	    this.mark = new LongWritable(mark);
	    this.children = children;
	}

	public ELabel() {
	    this(new MapWritable(),0L, new Text());
	}

	public void write(DataOutput out) throws IOException {
	    distances.write(out);
	    mark.write(out);
	    children.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
	    distances.readFields(in);
	    mark.readFields(in);
	    children.readFields(in);
	}
	
	public void set(MapWritable distances,
			long mark, Text children) {
	    this.children = children;
	    this.distances = distances;
	    this.mark = new LongWritable(mark);
	}
	
	public MapWritable getDistances() {
	    return distances;
	}
	
	public long getMark() {
	    return mark.get();
	}
	
	public Text getChildren() {
	    return children;
	}
    }


    /* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap extends Mapper<LongWritable,
					  LongWritable, LongWritable, LongWritable> {
       
        /* Will need to modify to not loose any edges. */
        @Override
	public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
	    context.write(key, value);
	    context.getCounter(ValueUse.EDGE).increment(1);
	}
    }

    public static class LoaderReduce
	extends Reducer<LongWritable, LongWritable, LongWritable, ELabel> {
	public long denom;
	/* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
	@Override
	public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
                                        new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }
	public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context)
	    throws IOException, InterruptedException {
	    String children = "";
	    MapWritable distances
		= new MapWritable();
	    for (LongWritable v : values) {
		children = children + v.toString() + " ";
	    }
	    if (Math.random() < 1.0/denom) {
		distances.put(key, new LongWritable(0L));
		context.write(key,
			      new ELabel(distances, 0L, new Text(children)));
		globalOriginsList.add(key.toString());
	    } else {
		context.write(key, new ELabel(distances,
					      Long.MAX_VALUE, new Text(children)));
	    }
	}
    }

    public static class BFSMap extends Mapper<LongWritable, ELabel, LongWritable, ELabel> {
	public void map(LongWritable key, ELabel value, Context context)
	    throws IOException, InterruptedException {
	    if (value.getMark() <= globalOriginsList.size()) {
		value.set(value.getDistances(), value.getMark() + 1L,
		      value.getChildren());
		if (value.getChildren().toString().length() > 0) {
		    for (String child : ((value.getChildren()).toString()).split(" ")) {
			if (!child.equals("")) { 
			    LongWritable childLong = new LongWritable(Long.parseLong(child));
			    MapWritable distances = new MapWritable();
			    for (String origins : globalOriginsList) {
				LongWritable originsLong = new LongWritable(Long.parseLong(origins));
				if (value.getDistances().containsKey(originsLong)) {
				    Long dist = ((LongWritable) (value.getDistances()).get(originsLong)).get()
					+ 1L;
				    distances.put(originsLong, new LongWritable(dist));
				}	
			    }
			    context.write(childLong, new ELabel(distances,
								Long.MIN_VALUE, new Text()));
			}
		    }
		}	
	    } else {
		context.write(key, value);
	    }
	}
    }
	    
    public static class BFSReduce
	extends Reducer<LongWritable, LongWritable, LongWritable, ELabel> {
	public long denom;
	/* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
	@Override
	public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader
		    = new BufferedReader(new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();	
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }
	public void reduce(LongWritable key, Iterable<ELabel> values,
			   Context context)
	    throws IOException, InterruptedException {
	    MapWritable totalDistances 
		= new MapWritable();
	    String subChildren = "";
	    long currentMark = Long.MAX_VALUE;
	    for (String origin : globalOriginsList) {
		totalDistances.put(new LongWritable(Long.parseLong(origin)),
				   new LongWritable(Long.MAX_VALUE));
	    }
	    for (ELabel v : values) {
		for (String origin : globalOriginsList) {
		    LongWritable temp = new LongWritable(Long.parseLong(origin));
		    Long dist = ((LongWritable)((v.getDistances()).get(origin))).get();
		    if (dist < ((LongWritable)totalDistances.get(temp)).get()) {
			totalDistances.put(temp, new LongWritable(dist));
		    }
		}
		
		//for every ev, pull out the children, and add to list
		if (((v.getChildren()).toString()).length() > 0) {
		    subChildren = subChildren + (v.getChildren()).toString() + " ";
		}
		//for every ev, pull out the smallest marker number
		if (v.getMark() < currentMark) {
		    currentMark = v.getMark();
		}
	    }
	    context.write(key, new ELabel(totalDistances,
					    currentMark, new Text(subChildren)));
	}
    }
 
    public static class HistMap extends Mapper<LongWritable,
					  ELabel, LongWritable, LongWritable> {
       
        /* Will need to modify to not loose any edges. */
	public void map(LongWritable key, ELabel value, Context context)
                throws IOException, InterruptedException {
	    MapWritable distances = value.getDistances();
	    for (String origin : globalOriginsList) {
		context.write(new LongWritable(Long.parseLong(origin)), new LongWritable(1L));
	    }
	}
    }

    public static class HistReduce
	extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	public long denom;
	@Override
	public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
                                        new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }
	public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context)
	    throws IOException, InterruptedException {
	    long sum = 0L;
	    for (LongWritable value : values) {
                sum += value.get();
	    }
	    context.write(key, new LongWritable(sum));
	}
    }

    // Shares denom argument across the cluster via DistributedCache
    public static void shareDenom(String denomStr, Configuration conf) {
        try {
	    Path localDenomPath = new Path(DENOM_PATH + "-source");
	    Path remoteDenomPath = new Path(DENOM_PATH);
	    BufferedWriter writer = new BufferedWriter(
				    new FileWriter(localDenomPath.toString()));
	    writer.write(denomStr);
	    writer.newLine();
	    writer.close();
	    FileSystem fs = FileSystem.get(conf);
	    fs.copyFromLocalFile(true,true,localDenomPath,remoteDenomPath);
	    DistributedCache.addCacheFile(remoteDenomPath.toUri(), conf);
        } catch (IOException ioe) {
            System.err.println("IOException writing to distributed cache");
            System.err.println(ioe.toString());
        }
    }

    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Set denom from command line arguments
        shareDenom(args[2], conf);

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ELabel.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Example of reading a counter
        System.out.println("Read in " + 
                   job.getCounters().findCounter(ValueUse.EDGE).getValue() + 
                           " edges");

        // Repeats your BFS mapreduce
        int i=0;
        // Will need to change terminating conditions to respond to data
        while (i<1) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(ELabel.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(ELabel.class);

            job.setMapperClass(BFSMap.class);
            job.setReducerClass(BFSReduce.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(HistMap.class);
        job.setCombinerClass(HistReduce.class);
        job.setReducerClass(HistReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
