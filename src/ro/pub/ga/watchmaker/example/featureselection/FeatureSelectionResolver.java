package ro.pub.ga.watchmaker.example.featureselection;

import java.text.DecimalFormat;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mahout.ga.watchmaker.travellingsalesman.ProgressListener;
import org.uncommons.maths.binary.BitString;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.selection.RouletteWheelSelection;

import ro.pub.ga.watchmaker.example.clustering.ClusteringResolver;
import ro.pub.ga.watchmaker.example.featureselection.BitStringCrossoverOperator.CrossoverOperatorType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ElitismType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.EvolutionType;

public class FeatureSelectionResolver extends Configured  implements Tool {

	private static FitnessEvaluator<BitString> evaluator;
	private static Options options;

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new FeatureSelectionResolver(), args);
		System.exit(res);
	}

	public static void checkLogger() {

		Logger logger= LogManager.getLogger(ClusteringResolver.class);
		System.out.println(logger.getAllAppenders());
	}

	private static CommandLine parseCLIParameters(String[] args) {

		options = new Options();

		options.addOption("ps", true, "population size [mandatory]");
		options.addOption("el", true, "the number of top individuals to be passed from one generation" +
		"to the next(if not specified, all population is kept from one step to another)");
		options.addOption("randomElitism", false, "type of elitism (BestType default)");

		options.addOption("gn", true, "number of generations[mandatory]");
		options.addOption("i", true, "path to the input file containing the points data set[mandatory]");
		options.addOption("crossover", true, "crossover type (AND default) [options - AND, OR, XOR, SinglePoint");
		
		options.addOption("masterslaveislands", false, "perform evolution using masterslaveislands model(default)");
		options.addOption("micromacroislands", false, "perform evolution using distributed fitness evolution model");

		options.addOption("conf", true, "path to the hadoop configuration folder[mandatory]");
		options.addOption("s", true, "number of slaves");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {

			cmd = parser.parse(options, args);
		} catch (ParseException e) {

			System.out.println("Bad input!");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "FeatureSelection", options);
		}

		return cmd;
	}

	@Override
	public int run(String[] args) throws Exception {

		//Getting the runtime reference from system
		Runtime runtime = Runtime.getRuntime();
		int mb = 1024*1024;

		System.out.println("##### Heap utilization statistics [MB] #####");

		//Print used memory
		System.out.println("Used Memory:"
				+ (runtime.totalMemory() - runtime.freeMemory()) / mb);

		//Print free memory
		System.out.println("Free Memory:"
				+ runtime.freeMemory() / mb);

		//Print total available memory
		System.out.println("Total Memory:" + runtime.totalMemory() / mb);

		//Print Maximum available memory
		System.out.println("Max Memory:" + runtime.maxMemory() / mb);
		//----------------------------------------------

		CommandLine cmd = parseCLIParameters(args);
		if(cmd == null) {
			return -1;
		}

		Long startTime, endTime;
		Integer populationSize, elitism, generations, slaves;
		EvolutionType evolutionType = EvolutionType.MasterSlaveIslands;
		HelpFormatter formatter = new HelpFormatter();
		ElitismType elitismType = ElitismType.BestCandidates;
		CrossoverOperatorType crossoverType = CrossoverOperatorType.AND;

		if(!cmd.hasOption("ps") || !cmd.hasOption("gn") || !cmd.hasOption("i") || !cmd.hasOption("conf") || !cmd.hasOption("s")) {
			System.out.println("Mandatory options missing!");
			formatter.printHelp("FeatureSelection", options);
		}

		try {
			populationSize = Integer.parseInt(cmd.getOptionValue("ps"));		  
		} catch (Exception e) {
			System.out.println("Bad populations size! It should be an integer value.");
			formatter.printHelp("FeatureSelection", options);
			return -1;
		}

		try {
			if(cmd.hasOption("el")) {
				elitism = Integer.parseInt(cmd.getOptionValue("el"));
				try {
					if(cmd.hasOption("randomElitism")) {
						elitismType = ElitismType.RandomCandidates;
					} else {
						elitismType = ElitismType.BestCandidates;
					}
				} catch (Exception e) {
					formatter.printHelp("FeatureSelection", options);
					return -1;
				}
			}
			else {
				elitism = 0;
				elitismType = ElitismType.All;
			}
		} catch (Exception e) {
			System.out.println("Bad elitism value! It should be an integer value.");
			formatter.printHelp("FeatureSelection", options);
			return -1;
		}

		try {
			generations = Integer.parseInt(cmd.getOptionValue("gn"));		  
		} catch (Exception e) {

			System.out.println("Bad number of generations! It should be an integer value.");
			formatter.printHelp("FeatureSelection", options);
			return -1;
		}

		try {

			slaves = Integer.parseInt(cmd.getOptionValue("s"));		  
		} catch (Exception e) {

			System.out.println("Bad number of slaves! It should be an integer value.");
			formatter.printHelp("FeatureSelection", options);
			return -1;
		}

		if(cmd.hasOption("micromacroislands")) {
			evolutionType = EvolutionType.MicroMacroSubpopulations;
		}

		if(cmd.hasOption("crossover")) {
			String crossoverOption = cmd.getOptionValue("crossover");
			if (cmd.getOptionValue("crossover") == null)
				System.out.println("[Oana] crossover option is null");
			if(crossoverOption.toUpperCase().equals("OR")) {
				crossoverType = CrossoverOperatorType.OR;
			} else if (crossoverOption.toUpperCase().equals("XOR")) {
				crossoverType = CrossoverOperatorType.XOR;
			} else if (crossoverOption.toUpperCase().equals("SINGLEPOINT")) {
				crossoverType = CrossoverOperatorType.BitStringCrossover;
			}
		}
		
		ClassifierFileParser fileParser = new ClassifierFileParser();
		ClassifierInput input = fileParser.parseFile(cmd.getOptionValue("i"));
		
		NaiveBayesClassifier naiveBayes = new NaiveBayesClassifier(input.trainingSet, input.testSet, input.featureTypes);
		System.out.println("Naive Bayes initial accuracy " + naiveBayes.getAccuracy());
		
		evaluator = new ClassifierEvaluator(input);

		EvolutionaryFeatureSelectionResolver featureSelectionResolver = new EvolutionaryFeatureSelectionResolver(
				evaluator, new RouletteWheelSelection(),
				populationSize, elitism, elitismType, crossoverType, generations,
				input.getNumberOfFeatures(), true, true, true, evolutionType, slaves);

		startTime = System.currentTimeMillis();

		BitString result = featureSelectionResolver.selectFeatures(new ProgressListener() {
			@Override
			public void updateProgress(double percentComplete) {
				DecimalFormat df = new DecimalFormat();
				df.setMaximumFractionDigits(2);
				System.out.println("Progress: " + df.format(percentComplete * 100));
			}
		});

		endTime = System.currentTimeMillis();

		System.out.println(createResultString(featureSelectionResolver.getDescription(),
				result, evaluator.getFitness(result, null),
				endTime - startTime));

		return 0;
	}
	
	/**
	 * Helper method for formatting a result as a string for display.
	 */
	private static  String createResultString(String strategyDescription,
			BitString selectedFeatures,
			double fitness,
			long elapsedTime) {
		StringBuilder buffer = new StringBuilder(100);
		buffer.append('[');
		buffer.append(strategyDescription);
		buffer.append("]\n");

		buffer.append("Selected Features\n");
		buffer.append(selectedFeatures.toString());
		buffer.append('\n');

		buffer.append("Classifier accuracy ");
		buffer.append(String.valueOf(fitness));
		buffer.append("\n");

		buffer.append("(Search Time: ");
		double seconds = (double) elapsedTime / 1000;
		buffer.append(String.valueOf(seconds));
		buffer.append(" seconds)\n\n");

		return buffer.toString();
	}

}
