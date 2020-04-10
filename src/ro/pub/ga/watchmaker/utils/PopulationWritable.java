package ro.pub.ga.watchmaker.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.common.StringUtils;
import org.uncommons.watchmaker.framework.EvaluatedCandidate;


public class PopulationWritable<T> implements WritableComparable<T> {
	
	private List<T> population;
	private Map<String, Object> data;
	
	public PopulationWritable() {
		
		this.population = new ArrayList<T>();
		data = new HashMap<String, Object>();
	}
	
	public PopulationWritable(List<T> population) {
		
		this.population = population;
		data = new HashMap<String, Object>();
	}
	
	/**
	 * @param population - the population of evaluated candidates
	 * @param differentiator - used to differentiate this constructor from the previous one
	 */
	public PopulationWritable(List<EvaluatedCandidate<T>> population, boolean differentiator) {
		
		this.population = new ArrayList<T>(population.size());
		
		for(EvaluatedCandidate<T> evCandidate: population) {
			
			this.population.add(evCandidate.getCandidate());
		}
		
		this.data = new HashMap<String, Object>();
	}
	
	public PopulationWritable(List<EvaluatedCandidate<T>> population, Map<String, Object> data, boolean differentiator) {
		
		this(population, differentiator);
		
		this.data = data;
	}
	
	public PopulationWritable(List<T> population, Map<String, Object> data) {
		
		this.population = population;
		this.data = data;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		String stringifiedPopulation = StringUtils.toString(population);
		String stringifiedData = StringUtils.toString(data);
		
		Text writablePopulation = new Text(stringifiedPopulation);
		Text writableData = new Text(stringifiedData);
		
		writablePopulation.write(out);
		writableData.write(out);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		
		Text writablePopulation = new Text();
		Text writableData = new Text();
		
		writablePopulation.readFields(in);
		writableData.readFields(in);
		
		this.population = (List<T>)StringUtils.fromString(writablePopulation.toString());
		this.data = (Map<String, Object>)StringUtils.fromString(writableData.toString());
	}

	@Override
	public int compareTo(T arg0) {
		
		return 0;
	}
	
	public void putDataValue(String key, Object value) {
		
		if(data == null) {
			
			data = new HashMap<String, Object>();
		}
		
		data.put(key, value);
	}
	
	public Object getDataValue(String key) {
		
		if(data == null) {
			
			return null;
		}
		
		return data.get(key);
	}
	
	public void addToPopulation(List<T> newIndividuals) {
		
		population.addAll(newIndividuals);
	}

	public List<T> getPopulation() {
		return population;
	}

	public void setPopulation(List<T> population) {
		this.population = population;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	@Override
	public String toString() {
		
		return population.toString();
	}
}
