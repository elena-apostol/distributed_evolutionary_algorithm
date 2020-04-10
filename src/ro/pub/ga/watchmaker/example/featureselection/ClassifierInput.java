package ro.pub.ga.watchmaker.example.featureselection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ro.pub.ga.watchmaker.example.featureselection.NaiveBayesClassifier.FeatureSummary.FeatureType;

public class ClassifierInput {

	Integer numberOfFeatures;
	HashMap<Integer, List<ClassifiedData>> trainingSet, testSet;
	ArrayList<FeatureType> featureTypes;
	
	public ClassifierInput(HashMap<Integer, List<ClassifiedData>> trainingSet, HashMap<Integer, List<ClassifiedData>> testSet,
			Integer numberOfFeatures, ArrayList<FeatureType> featureType) {
		this.trainingSet = trainingSet;
		this.testSet = testSet;
		this.numberOfFeatures = numberOfFeatures;
		this.featureTypes = featureType;
	}

	public void setTrainingSet(HashMap<Integer, List<ClassifiedData>> trainingSet) {
		this.trainingSet = trainingSet;
	}

	public void setTestSet(HashMap<Integer, List<ClassifiedData>> testSet) {
		this.testSet = testSet;
	}

	public HashMap<Integer, List<ClassifiedData>> getTrainingSet() {
		return trainingSet;
	}

	public HashMap<Integer, List<ClassifiedData>> getTestSet() {
		return testSet;
	}

	public Integer getNumberOfFeatures() {
		return numberOfFeatures;
	}
	
}
