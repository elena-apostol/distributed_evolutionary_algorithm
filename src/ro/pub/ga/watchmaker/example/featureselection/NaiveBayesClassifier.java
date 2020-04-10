package ro.pub.ga.watchmaker.example.featureselection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.uncommons.maths.binary.BitString;

//http://machinelearningmastery.com/naive-bayes-classifier-scratch-python/

public class NaiveBayesClassifier {
 
	HashMap<Integer, List<ClassifiedData>> trainingSet, testSet;
	ArrayList<FeatureSummary.FeatureType> featureTypes;
	
	HashMap<Integer, Integer> classNrOfInstancesTraining = new HashMap<Integer, Integer>();
	Integer totalNrOfInstances = 0;
	
	public NaiveBayesClassifier(HashMap<Integer, List<ClassifiedData>> trainingSet,
			HashMap<Integer, List<ClassifiedData>> testSet,
			ArrayList<FeatureSummary.FeatureType> featureTypes) {
		this.trainingSet = trainingSet;
		this.testSet = testSet;
		this.featureTypes = featureTypes;
	}
	
	public FeatureSummary summarizeGaussianFeature(List<ClassifiedData> data, int i) {
		double mean = 0, stdev = 0;
		for (ClassifiedData d : data) {
			mean += d.getData().get(i);
		}
		mean = mean / data.size();
		
		for (ClassifiedData d : data) {
			stdev += (mean - d.getData().get(i)) * (mean - d.getData().get(i));
		}
		
		stdev = Math.sqrt(stdev / (data.size()));
		return new FeatureSummary.GaussianFeatureSummary(mean, stdev);
	}
	
	public FeatureSummary summarizeBernoulliFeature(List<ClassifiedData> data, int i) {
		HashMap<Double, Integer> countMap = new HashMap<Double, Integer>();
		for (ClassifiedData d : data) {
			Double val = d.getData().get(i);
			Integer count = countMap.remove(val);
			if (count == null) {
				count = new Integer(0);
			}
			count = count + 1;
			countMap.put(val, count);
		}
		return new FeatureSummary.BernoulliFeatureSummary(countMap, data.size());
	}
	
	public FeatureSummary summarizeFeature(List<ClassifiedData> data, int i) {
		if (featureTypes.get(i) == FeatureSummary.FeatureType.Gaussian) {
			return summarizeGaussianFeature(data, i);
		} else {
			return summarizeBernoulliFeature(data, i);
		}
	}
	
	public List<FeatureSummary> summarizeClass(List<ClassifiedData> data) {
		if (data.size() == 0)
			return null;
		List<FeatureSummary> summary = new ArrayList<NaiveBayesClassifier.FeatureSummary>();
		for (int i = 0; i < data.get(0).getData().size(); i++) {
			FeatureSummary featureSummary = summarizeFeature(data, i);
			summary.add(featureSummary);
			//System.out.println("\t" + featureSummary);
		}
		return summary;
	}
	
	public HashMap<Integer, List<FeatureSummary>> trainClassifier() {
		HashMap<Integer, List<FeatureSummary>> summary = new HashMap<Integer, List<FeatureSummary>>();
		for (Map.Entry<Integer, List<ClassifiedData>> entry : trainingSet.entrySet()) {
			//System.out.println("For class " + entry.getKey());
			List<FeatureSummary> classSummary = summarizeClass(entry.getValue());
			summary.put(entry.getKey(), classSummary);
			// To add class probabilities
			classNrOfInstancesTraining.put(entry.getKey(), entry.getValue().size());
			totalNrOfInstances += entry.getValue().size();
			//System.out.println("For class " + entry.getKey() + " instances: " + classNrOfInstancesTraining.get(entry.getKey()));
		}
		//System.out.println("Total " + totalNrOfInstances);
		return summary;
	}
		
	private double calculateProbability(Double value, FeatureSummary summary) {
		if (summary.type == FeatureSummary.FeatureType.Bernoulli) {
			FeatureSummary.BernoulliFeatureSummary bernoulliFeatureSummary = (FeatureSummary.BernoulliFeatureSummary) summary;
			HashMap<Double, Integer> countMap = bernoulliFeatureSummary.countMap;
			Integer count = countMap.get(value);
			if (count == null) {
				return (double)1 / (double)bernoulliFeatureSummary.total;
			}
			return (double)count / (double)bernoulliFeatureSummary.total;
		} else {
			FeatureSummary.GaussianFeatureSummary gaussianFeatureSummary = (FeatureSummary.GaussianFeatureSummary) summary;
			double stdev = gaussianFeatureSummary.stdev;
			double mean = gaussianFeatureSummary.mean;
			
			if (stdev == 0) 
				System.out.println("stdev is 0");
			double exponent = Math.exp(-(Math.pow(value - mean, 2) / (2 * Math.pow(stdev, 2))));
			return (1 / (Math.sqrt(2 * Math.PI) * stdev)) * exponent;
		}
	}
	
	private double calculateClassProbability(Integer classId, List<FeatureSummary> summary, List<Double> inputData) {
		double probability = 1;
		for (int i = 0; i < inputData.size(); i++) {
			double p = calculateProbability(inputData.get(i), summary.get(i));
			probability *= p;
		}
		probability *= ((double)classNrOfInstancesTraining.get(classId) / totalNrOfInstances);
		return probability;
	}
	
	private Integer predict(HashMap<Integer, List<FeatureSummary>> summary, List<Double> inputData) {
		double bestProbability = 0;
		Integer predictedClass = 0;
		
		if (summary.isEmpty()) {
			System.out.println("ERROR summary empty");
			return -Integer.MAX_VALUE;
		}
		
		for (Map.Entry<Integer, List<FeatureSummary>> entry : summary.entrySet()) {
			double classProbability = calculateClassProbability(entry.getKey(), entry.getValue(), inputData);
			if (!Double.isNaN((double)classProbability) && classProbability > bestProbability) {
				bestProbability = classProbability;
				predictedClass = entry.getKey();
			}
		}
		return predictedClass;
	}
	
	public Score testClassifier(HashMap<Integer, List<FeatureSummary>> summary) {
		double a = 0, b = 0, c = 0, d = 0;
		
		double correct = 0, incorrect = 0;
		
		if (summary.size() != 2) {
			for (Map.Entry<Integer, List<ClassifiedData>> entry : testSet.entrySet()) {
				for (ClassifiedData data : entry.getValue()) {
					Integer classID = predict(summary, data.getData());
					if (classID.intValue() == data.getClassId().intValue()) {
						correct ++;
					} else {
						incorrect ++;
					}
				}
			}
			return new Score(correct, incorrect, 0, 0);
		} else {
			for (Map.Entry<Integer, List<ClassifiedData>> entry : testSet.entrySet()) {
				for (ClassifiedData data : entry.getValue()) {
					Integer classID = predict(summary, data.getData());
					if (classID.intValue() == data.getClassId().intValue()) {
						if (classID < 0)
							a ++;
						else 
							d ++;
					} else {
						if (data.getClassId() < 0)
							b ++;
						else
							c++;
					}
				}
			}
		}
		return new Score(a, b, c, d);
	}
	
	public double getAccuracy() {
		HashMap<Integer, List<FeatureSummary>> summary = trainClassifier();
		Score score = testClassifier(summary);
		return score.getAccuracy();
	}
	
	static abstract class FeatureSummary {
		static enum FeatureType {
			Bernoulli, Gaussian
		}
		
		public FeatureType type;
		
		public static class GaussianFeatureSummary extends FeatureSummary{
			double mean;
			double stdev;
			
			public GaussianFeatureSummary(double mean, double stdev) {
				super();
				this.mean = mean;
				this.stdev = stdev;
				this.type = FeatureType.Gaussian;
			}
			
			public String toString() {
				return "mean: " + mean + " stdev: " + stdev + "\n";
			}
		}
		
		public static class BernoulliFeatureSummary extends FeatureSummary{
			HashMap<Double, Integer> countMap = new HashMap<Double, Integer>();
			int total;
			
			public BernoulliFeatureSummary(HashMap<Double, Integer> countMap, int total) {
				super();
				this.countMap = countMap;
				this.total = total;
				this.type = FeatureType.Bernoulli;
			}
			
			public String toString() {
				StringBuilder sb = new StringBuilder();
				for (Map.Entry<Double, Integer> count : countMap.entrySet()) {
					sb.append(count.getKey() + " = " + count.getValue() + ", ");
				}
				return "[total]" + total + "map: " + sb.toString();
			}
		}
	}
	
	
	private static class Score {
		//‘a’ is the number of correctly classified negative instances
		double a;
		//‘b’ is the number of incorrectly classified positive instances
		double b;
		//‘c’ is the number of incorrectly classified negative instances
		double c;
		//‘d’ is the number of correctly classified positive instances
		double d;
		
		public Score(double a, double b, double c, double d) {
			super();
			this.a = a;
			this.b = b;
			this.c = c;
			this.d = d;
		}
		
		public double getAccuracy() {
			return (a + d) / (a + b + c + d);
		}
	}

	public static void main(String [] args) {
		ClassifierFileParser classifierFileParser = new ClassifierFileParser();
		ClassifierInput input;
		try {
			input = classifierFileParser.parseFile("generatedData");
			NaiveBayesClassifier naiveBayes = new NaiveBayesClassifier(input.trainingSet, input.testSet, input.featureTypes);
			System.out.println("-- Accuracy " + naiveBayes.getAccuracy());
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("double min value " + Double.MIN_VALUE);
	}
}
