package ro.pub.ga.watchmaker.example.featureselection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ro.pub.ga.watchmaker.example.featureselection.NaiveBayesClassifier.FeatureSummary.FeatureType;

public class ClassifierFileParser {
	
	static final boolean file_hdfs = true;
	
	public ClassifierInput parseFile(String filePath) throws IOException {
		System.out.println("==================Reading file " + filePath);
		
		int numberOfFeatures = 0;
		HashMap<Integer, List<ClassifiedData>> trainingSet = new HashMap<Integer, List<ClassifiedData>>();
		HashMap<Integer, List<ClassifiedData>> testSet = new HashMap<Integer, List<ClassifiedData>>();
		
		BufferedReader br;
		
		if (file_hdfs) {
			Path pt=new Path(filePath);
		    FileSystem fs = FileSystem.get(new Configuration());
		    br =new BufferedReader(new InputStreamReader(fs.open(pt)));
		} else {
			br = new BufferedReader(new FileReader(filePath));
		}
		
		String line = "";
		String cvsSplitBy = ",";
		
		int total = 0;
		
		HashMap<Integer, List<ClassifiedData>> dataset = new HashMap<Integer, List<ClassifiedData>>();
		try {
			while ((line = br.readLine()) != null) {
				total++;
				String[] data = line.split(cvsSplitBy);
				List<Double> datavector = new ArrayList<Double>();
				for (int i = 0 ; i < data.length - 1; i++) {
					numberOfFeatures = data.length - 1;
					if (data[i].equals("?")) {
						datavector.add(new Double(0));
					} else {
						datavector.add(Double.parseDouble(data[i]));
					}
				}
				Integer classId = Integer.parseInt(data[data.length - 1]);
				List<ClassifiedData> list = dataset.get(classId);
				if (list == null) {
					list = new ArrayList<ClassifiedData>();
				}
				list.add(new ClassifiedData(datavector, classId));
				dataset.put(classId, list);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		
		//67% train and 33% test
		for(Map.Entry<Integer, List<ClassifiedData>> entry : dataset.entrySet()) {
			//primele 67% date le folosim pt training
			int nrTraining = 67 * entry.getValue().size()/ 100;
			List<ClassifiedData> trainingList = new ArrayList<ClassifiedData>(entry.getValue().subList(0, nrTraining));
			List<ClassifiedData> testList = new ArrayList<ClassifiedData>(entry.getValue().subList(nrTraining, entry.getValue().size()));
			trainingSet.put(entry.getKey(), trainingList);
			testSet.put(entry.getKey(), testList);
		}
		
		int numberOfOriginalFeatures = 13;
		int duplicateFactor = numberOfFeatures / numberOfOriginalFeatures;
		System.out.println("numberOfFetures " + numberOfFeatures + " duplicate " + duplicateFactor);
		ArrayList<FeatureType> featureTypes = new ArrayList<NaiveBayesClassifier.FeatureSummary.FeatureType>();
		for (int i = 0 ; i < numberOfOriginalFeatures; i++) {
			for (int j = 0; j < duplicateFactor; j++) {
				if (i == 1 || i == 2 || i == 5 || i == 6 || i == 8 ||
						i == 10 || i == 11 || i == 12) {
					featureTypes.add(FeatureType.Bernoulli);
				} else {
					featureTypes.add(FeatureType.Gaussian);
				}
			}
		}
		return new ClassifierInput(trainingSet, testSet, new Integer(numberOfFeatures), featureTypes);
	}
}
