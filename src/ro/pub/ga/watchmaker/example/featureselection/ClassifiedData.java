package ro.pub.ga.watchmaker.example.featureselection;

import java.util.ArrayList;
import java.util.List;

import org.uncommons.maths.binary.BitString;

public class ClassifiedData {

	private List<Double> data;
	
	private Integer classId;
	
	public ClassifiedData(List<Double> data, Integer classId) {
		super();
		this.data = data;
		this.classId = classId;
	}
	
	public ClassifiedData filter(BitString bitString) {
		List<Double> filteredData = new ArrayList<Double>();
		for(int i = 0 ; i < Math.min(data.size(), bitString.getLength()); i++) {
			if (bitString.getBit(i))
				filteredData.add(new Double(data.get(i).doubleValue()));
		}
		return new ClassifiedData(filteredData, new Integer(classId.intValue()));
	}
	
	public List<Double> getData() {
		return data;
	}
	
	public void setData(List<Double> data) {
		this.data = data;
	}
	
	public Integer getClassId() {
		return classId;
	}
	
	public void setClassId(Integer classId) {
		this.classId = classId;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Double d : data) {
			sb.append(d + " ");
		}
		return "[ClassifiedData] class " + classId + " data: " + sb.toString();
	}
}
