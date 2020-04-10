package ro.pub.ga.watchmaker.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.StringUtils;

public class AssignedCandidateWritable<T> implements WritableComparable<T> {

	private T candidate;
	private Long assignedSubpopulation;
	//should ?
	//Map<String, Object> data
	
	public AssignedCandidateWritable(T candidate, Long assignedSubpopulation) {
		super();
		this.candidate = candidate;
		this.assignedSubpopulation = assignedSubpopulation;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		String stringifiedCandidate = StringUtils.toString(candidate);
		String stringifiedAssignedSubpopulation = StringUtils.toString(assignedSubpopulation);
		
		Text writableCandidate = new Text(stringifiedCandidate);
		Text writableAssignedSubpopulation = new Text(stringifiedAssignedSubpopulation);
		
		writableCandidate.write(out);
		writableAssignedSubpopulation.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Text writableCandidate = new Text();
		Text writableAssignedSubpopulation = new Text();
		
		writableCandidate.readFields(in);
		writableAssignedSubpopulation.readFields(in);
		
		this.candidate = (T)StringUtils.fromString(writableCandidate.toString());
		this.assignedSubpopulation = (Long)StringUtils.fromString(writableAssignedSubpopulation.toString());
	}

	@Override
	public int compareTo(T o) {
		return 0;
	}
	
	public T getCandidate() {
		return candidate;
	}

	public void setCandidate(T candidate) {
		this.candidate = candidate;
	}

	public Long getAssignedSubpopulation() {
		return assignedSubpopulation;
	}

	public void setAssignedSubpopulation(Long assignedSubpopulation) {
		this.assignedSubpopulation = assignedSubpopulation;
	}

}
