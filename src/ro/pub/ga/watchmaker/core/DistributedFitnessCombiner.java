package ro.pub.ga.watchmaker.core;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import ro.pub.ga.watchmaker.utils.EvaluatedCandidateWritable;

public class DistributedFitnessCombiner<T> extends Reducer<LongWritable, EvaluatedCandidateWritable<T>, LongWritable, EvaluatedCandidateWritable<T>>{

	@Override
	protected void reduce(
			LongWritable arg0,
			Iterable<EvaluatedCandidateWritable<T>> arg1,
			Reducer<LongWritable, EvaluatedCandidateWritable<T>, LongWritable, EvaluatedCandidateWritable<T>>.Context arg2)
			throws IOException, InterruptedException {
		
		Iterator<EvaluatedCandidateWritable<T>> iterator = arg1.iterator();
		
		int n = 0;
		while(iterator.hasNext()) {
			
			arg2.write(arg0, iterator.next());
			n++;
		}
		
		System.out.println("[Combiner] Processed " + n + " individuals");
	}
}
