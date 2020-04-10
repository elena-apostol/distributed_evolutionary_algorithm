package ro.pub.ga.watchmaker.core;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.mahout.common.StringUtils;

import ro.pub.ga.watchmaker.utils.EvaluatedCandidateWritable;

public class DistributedFitnessPartitioner<T> extends Partitioner<LongWritable, EvaluatedCandidateWritable<T>> implements org.apache.hadoop.conf.Configurable{

	private Configuration conf;
	private Random rng;
	
	@Override
	public int getPartition(LongWritable key,
			EvaluatedCandidateWritable<T> value, int numPartitions) {
		
		return rng.nextInt(numPartitions);
	}

	@Override
	public void setConf(Configuration conf) {

		this.conf = conf;
		rng = StringUtils.fromString(conf.get(Constants.RANDOM_GENERATOR_PROPERTY));
	}

	@Override
	public Configuration getConf() {

		return conf;
	}


}
