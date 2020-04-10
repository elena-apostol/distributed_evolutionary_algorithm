package ro.pub.ga.watchmaker.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.StringUtils;

public class EvaluatedCandidateWritable<T> implements WritableComparable<EvaluatedCandidateWritable<T>> {
	
	private T candidate;
	private double fitness;
	
	

	public EvaluatedCandidateWritable(T candidate, double fitness) {
		super();
		this.candidate = candidate;
		this.fitness = fitness;
	}
	
	public EvaluatedCandidateWritable() {
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		String stringifiedCandidate = StringUtils.toString(candidate);
		
		//Write the candidate and fitness value wrapped in Writable classes;
		Text writableCandidate = new Text(stringifiedCandidate);
		DoubleWritable doubleWritable = new DoubleWritable(fitness);
		
		writableCandidate.write(out);
		doubleWritable.write(out);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		
		//read the fields as saved on the FS
		Text writableCandidate = new Text();
		DoubleWritable doubleWritable = new DoubleWritable();
		
		writableCandidate.readFields(in);
		
		//extract the candidate
		candidate = (T)StringUtils.fromString(writableCandidate.toString());
		
		//extract the fitness value
		doubleWritable.readFields(in);
		fitness = doubleWritable.get();
	}
	
	@Override
	public int compareTo(EvaluatedCandidateWritable<T> arg0) {
		
		return (getFitness() == arg0.getFitness()) ? 0 : (getFitness() < arg0.getFitness() ? -1 : 1);
	}

	public T getCandidate() {
		return candidate;
	}

	public void setCandidate(T candidate) {
		this.candidate = candidate;
	}

	public double getFitness() {
		return fitness;
	}

	public void setFitness(double fitness) {
		this.fitness = fitness;
	}

	
}
