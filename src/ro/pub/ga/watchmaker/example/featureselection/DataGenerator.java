package ro.pub.ga.watchmaker.example.featureselection;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {

	int nrRecords;
	int duplicateFactor;
	String fileName;
	ArrayList<AttributeValueType> attributeValueTypes;
	
	public DataGenerator(int nrRecords, int duplicateFactor, String fileName) {
		this.nrRecords = nrRecords;
		this.duplicateFactor = duplicateFactor;
		this.fileName = fileName;
		
		attributeValueTypes = new ArrayList<DataGenerator.AttributeValueType>();
		//1
		attributeValueTypes.add(new ContinuousValues(30, 80, 0));
		//2
		ArrayList<Integer> values = new ArrayList<Integer>(); values.add(0); values.add(1);
		attributeValueTypes.add(new DiscreteValues(values));
		//3
		values = new ArrayList<Integer>(); values.add(1); values.add(2); values.add(3); values.add(4);
		attributeValueTypes.add(new DiscreteValues(values));
		//4
		attributeValueTypes.add(new ContinuousValues(100, 200, 0));
		//5
		attributeValueTypes.add(new ContinuousValues(150, 350, 0));
		//6
		values = new ArrayList<Integer>(); values.add(0); values.add(1);
		attributeValueTypes.add(new DiscreteValues(values));
		//7
		values = new ArrayList<Integer>(); values.add(0); values.add(1); values.add(2);
		attributeValueTypes.add(new DiscreteValues(values));
		//8
		attributeValueTypes.add(new ContinuousValues(90, 200, 0));
		//9
		values = new ArrayList<Integer>(); values.add(0); values.add(1);
		attributeValueTypes.add(new DiscreteValues(values));
		//10
		attributeValueTypes.add(new ContinuousValues((float)0.0, (float)3.4, 1));
		//11
		values = new ArrayList<Integer>(); values.add(1); values.add(2); values.add(3);
		attributeValueTypes.add(new DiscreteValues(values));
		//12
		values = new ArrayList<Integer>(); values.add(0); values.add(1); values.add(2); values.add(3);
		attributeValueTypes.add(new DiscreteValues(values));
		//13
		values = new ArrayList<Integer>(); values.add(3); values.add(6); values.add(7);
		attributeValueTypes.add(new DiscreteValues(values));
		//14
		values = new ArrayList<Integer>(); values.add(0); values.add(1); values.add(2);values.add(3); values.add(4);
		attributeValueTypes.add(new DiscreteValues(values));
	}
	
	private static final String COMMA_DELIMITER = ",";
	private static final String NEW_LINE_SEPARATOR = "\n";
	
	public void generateData() {
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(fileName);
			for(int i = 0; i < nrRecords; i++) {
				for(int j = 0; j < attributeValueTypes.size(); j++) {
					if (j == attributeValueTypes.size() - 1) {
						fileWriter.append(String.valueOf((int)attributeValueTypes.get(j).getValue()));
					}
					else {
						for(int k = 0; k < duplicateFactor; k++) {
							fileWriter.append(String.valueOf(attributeValueTypes.get(j).getValue()));
							fileWriter.append(COMMA_DELIMITER);
						}
					}
				}
				fileWriter.append(NEW_LINE_SEPARATOR);
			}
			System.out.println("CSV file was created successfully !!!");
		} catch (Exception e) {
			System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter !!!");
                e.printStackTrace();
			}
		}
	}
		
	public static interface AttributeValueType{
		static enum AttributeValueTypes{
			Continuous, Discrete
		};
		AttributeValueTypes getType();
		float getValue();
	}
	
	public static class ContinuousValues implements AttributeValueType {
		@Override
		public AttributeValueTypes getType() {
			return AttributeValueTypes.Continuous;
		}
		float min;
		float max;
		// 0 - integer ; 1 - float
		int valueType;
		
		public float getMin() {
			return min;
		}
		public float getMax() {
			return max;
		}
		public int getValueType() {
			return valueType;
		}
		public ContinuousValues(float min, float max, int valueType) {
			super();
			this.min = min;
			this.max = max;
			this.valueType = valueType;
		}
		@Override
		public float getValue() {
			Random rand = new Random();
			if (valueType == 0) {
			    int generatedValue = rand.nextInt(((int)max - (int)min) + 1) + (int)min;
			    return generatedValue;
			} else {
				float generatedValue = rand.nextFloat() * (max - min) + min;
				generatedValue = (float)((int)(generatedValue * 100) / (float)100);  
				return generatedValue;
			}
		}
	}

	public static class DiscreteValues implements AttributeValueType {
		@Override
		public AttributeValueTypes getType() {
			return AttributeValueTypes.Discrete;
		}
		List<Integer> possibleValues;
		public List<Integer> getPossibleValues() {
			return possibleValues;
		}
		public DiscreteValues(List<Integer> possibleValues) {
			super();
			this.possibleValues = possibleValues;
		}
		@Override
		public float getValue() {
			Random rand = new Random();
			int index = rand.nextInt(possibleValues.size());
			return possibleValues.get(index);
		}
	}
	
	//main
	public static void main(String [ ] args) {
		int nrRecords = Integer.parseInt(args[0]);
		int duplicateFactor = Integer.parseInt(args[1]);
		String fileName = args[2];
		
		DataGenerator dataGenerator = new DataGenerator(nrRecords, duplicateFactor, fileName);
		dataGenerator.generateData();
	}
}
