package ro.pub.ga.watchmaker.example.featureselection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.uncommons.maths.binary.BitString;
import org.uncommons.watchmaker.framework.FitnessEvaluator;

import ro.pub.ga.watchmaker.example.featureselection.NaiveBayesClassifier.FeatureSummary.FeatureType;

public class ClassifierEvaluator implements FitnessEvaluator<BitString> {

	private ClassifierInput input;
	
	public ClassifierEvaluator(ClassifierInput inputSets) {
		this.input = inputSets;
	}
	
	@Override
	public double getFitness(BitString bitString, List<? extends BitString> arg1) {
		
		if(bitString.countSetBits() == 0) {
			System.out.println("[Oana]No bits set");
			return 0;
		}
		
		System.out.println("bitstring " + bitString.toString());
		// create input for NaiveBayes - parse selected features
		HashMap<Integer, List<ClassifiedData>> trainingSet = new HashMap<Integer, List<ClassifiedData>>();
		HashMap<Integer, List<ClassifiedData>> testSet = new HashMap<Integer, List<ClassifiedData>>();
		for(Map.Entry<Integer, List<ClassifiedData>> entry : input.trainingSet.entrySet()) {
			List<ClassifiedData> data = new ArrayList<ClassifiedData>();
			for (ClassifiedData d : entry.getValue())
				data.add(d.filter(bitString));
			//System.out.println("class " + entry.getKey() + " nr " + data.size());
			trainingSet.put(entry.getKey(), data);
		}
		
		for(Map.Entry<Integer, List<ClassifiedData>> entry : input.testSet.entrySet()) {
			List<ClassifiedData> data = new ArrayList<ClassifiedData>();
			for (ClassifiedData d : entry.getValue())
				data.add(d.filter(bitString));
			testSet.put(entry.getKey(), data);
		}
		
		ArrayList<FeatureType> filteredFeatureTypes = new ArrayList<NaiveBayesClassifier.FeatureSummary.FeatureType>();
		for(int i = 0 ; i < Math.min(input.featureTypes.size(), bitString.getLength()); i++) {
			if (bitString.getBit(i))
				filteredFeatureTypes.add(input.featureTypes.get(i));
		}
		
		NaiveBayesClassifier naiveBayesClassifier = new NaiveBayesClassifier(trainingSet, testSet, filteredFeatureTypes);
		return naiveBayesClassifier.getAccuracy() * (double)100;
	}

	@Override
	public boolean isNatural() {
		return true;
	}
}
