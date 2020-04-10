package ro.pub.ga.watchmaker.example.clustering;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ClusteringFileParserFactory {
	
	public enum ClusteringInputType{
		FarmInput
	};
	
	public static interface ClusteringFileParser<T> {
		public List<ArrayList<T>> parseFiles(String filesPath) throws IOException;
		public String getDescription();
		public String getPointsString(List<ArrayList<T>> points);
	}
	
	public static ClusteringFileParser getFileParser(ClusteringInputType type) {
		switch (type) {
		case FarmInput:
			return new FarmInputFileParser();
		default:
			return null;
		}
	}
	
	private static class FarmInputFileParser implements ClusteringFileParser<Float> {

		private ArrayList<SensorCharacteristics> sensorsCharacteristics = new ArrayList<ClusteringFileParserFactory.FarmInputFileParser.SensorCharacteristics>();
		String greenhouseName;
		
		@Override
		public List<ArrayList<Float>> parseFiles(String filesPath) throws IOException {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(filesPath));
		   
			List<ArrayList<Float>> points = new ArrayList<ArrayList<Float>>();
			
			int fileCount = 0;
			String line;
			//for (File file : (new File(filesPath)).listFiles()) {
			for (FileStatus fileStatus : status) {
				if(!fileStatus.getPath().getName().startsWith("input"))
					continue;
				ArrayList<Float> point = new ArrayList<Float>();
				fileCount++;
				System.out.println("Reading file " + fileStatus.getPath());
				BufferedReader in =
					new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
					//new BufferedReader(new FileReader(file.getAbsolutePath()));
				try {
					line = in.readLine();
					if (line == null) continue;
					// primul fisier seteaza si caracteristicile senzorilor ce trebuie respectate de 
					// celelalte fisiere
					if (fileCount == 1) {
						parseFirstFile(line);
					}
					JSONObject input = new JSONObject(line);
					JSONObject record = input.getJSONObject("record");
					JSONArray sdata = record.getJSONArray("sdata");
					// should be only one
					JSONObject data = sdata.getJSONObject(0);
					/*if (!greenhouseName.equals(data.getString("name"))) {
						throw new JSONException("Data not from the same greenhouse");
					}*/
					JSONArray sensors = data.getJSONArray("sensors");
					for (int i = 0; i < sensors.length(); i++) {
						String sensorType = sensors.getJSONObject(i).getString("stype");
						String sensorUnits = sensors.getJSONObject(i).getString("units");
						// check sensor
						if (!sensorsCharacteristics.get(i).equals(sensorType, sensorUnits)) {
							throw new JSONException("Data doesn not respect sensor characteristics");
						}
						point.add(new Float(sensors.getJSONObject(i).getDouble("value")));
					}
					points.add(point);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
			
			// write points to vectorWritable
			//writePointsToHDFSSequenceFileFormat(points);
			readSeqFile(filesPath);
			return points;
		}
		
		
		//WRITE WITH MAHOUT 0.9
		private void writePointsToHDFSSequenceFileFormat(List<ArrayList<Float>> points) throws IOException {
			System.out.println(">>>>>>>>>>>>>>>> Writing sequence file!!!!");
			SequenceFile.Writer writer;
			List<NamedVector> namedPointsVector = new ArrayList<NamedVector>();
			for (int i = 0; i < points.size(); i++) {
				ArrayList<Float> point = points.get(i);
				double[] doubleArray = new double[point.size()];
				for (int j = 0 ; j < point.size(); j++) {
				    doubleArray[j] = (double) point.get(j).doubleValue();
				}
				namedPointsVector.add( new NamedVector(new DenseVector(doubleArray), "point_" + i ));
			}
			
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			try {
			    Path path = new Path("output_sequence");
			    writer = SequenceFile.createWriter(conf,
			            Writer.file(path),
			            Writer.keyClass(Text.class),
			            Writer.valueClass(VectorWritable.class));

				VectorWritable vec = new VectorWritable();
				for(NamedVector vector : namedPointsVector){
					vec.set(vector);
					writer.append(new Text(vector.getName()), vec);
				}
				writer.close();

			} catch (Exception e) {
			    System.out.println("ERROR: "+e);
			}
		}
		
		// READ WITH MAHOUT 0.5
		//TODO - citeste outputul unei rulari mahout - pot procesa distanta toata pt centre si compara cu
		// fitness
		public void readSeqFile(String filesPath) {
			System.out.println("----------------------Reading seq files--------------------------");
			Configuration config = new Configuration();
			Path pathCenters = new Path(filesPath + "/part-r-00000");
			Path pathClusteredPoints = new Path(filesPath + "/part-m-00000");
			float totalDistance = 0;
			Hashtable<Integer, Vector> clusters = new Hashtable<Integer, Vector>();
			DistanceMeasure dm = null;
			System.out.println("----------------------Clusteres--------------------------");
			try {
				SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), pathCenters, config);
				Text key = new Text();
				Cluster value = new Cluster();
				while (reader.next(key, value)){
					// TODO - not really safe
					Integer clusterKey = Integer.parseInt(key.toString().substring(3, key.getLength()));
					System.out.println("Key " + key.toString() + " Value " + value.getCenter());
					clusters.put(clusterKey, value.getCenter());
					dm = value.getMeasure();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			System.out.println("----------------------Points--------------------------");
			try {
				SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), pathClusteredPoints, config);
				IntWritable key = new IntWritable();
				WeightedVectorWritable value = new WeightedVectorWritable();
				while (reader.next(key, value)){
					Vector cluster = clusters.get(new Integer(key.get()));
					//System.out.println("distance between " + cluster + " and " +
					//		value.getVector() + " is " + dm.distance(cluster, value.getVector()));
					totalDistance += dm.distance(cluster, value.getVector());
					//System.out.println("Key " + key.get());
					//System.out.println("Value " + value.getVector());
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("----------------------Total distance " + totalDistance + "--------------------------");
		}

		public static Float getDistance(Vector p1, Vector p2) {
			float distance = 0;
			for (int i = 0; i < p1.size(); i++) {
				distance += ((p1.get(i) - p2.get(i)) * (p1.get(i) - p2.get(i)));
			}
			return new Float(Math.sqrt(distance));
		}
		
		
		private void parseFirstFile(String string) {
			try {
				JSONObject input = new JSONObject(string);
				JSONObject record = input.getJSONObject("record");
				JSONArray sdata = record.getJSONArray("sdata");
				// should be only one
				JSONObject data = sdata.getJSONObject(0);
				greenhouseName = data.getString("name");
				JSONArray sensors = data.getJSONArray("sensors");
				for (int i = 0; i < sensors.length(); i++) {
					sensorsCharacteristics.add(
							new SensorCharacteristics(
									sensors.getJSONObject(i).getString("stype"),
									sensors.getJSONObject(i).getString("units")));
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
		private static class SensorCharacteristics {
			String type;
			String units;
			
			public SensorCharacteristics(String type, String units) {
				super();
				this.type = type;
				this.units = units;
			}
			
			public boolean equals(String type, String units) {
				return this.type.equals(type) && this.units.equals(units);
			}
		}

		@Override
		public String getDescription() {
			StringBuilder sb = new StringBuilder();
			sb.append("GreenhouseName: " + greenhouseName + "\nSensors:\n");
			for(SensorCharacteristics sc : sensorsCharacteristics) {
				sb.append("\tType: " + sc.type + ", Units: " + sc.units + "\n");
			}
			return sb.toString();
		}
		
		public String getPointsString(List<ArrayList<Float>> points) {
			StringBuilder sb = new StringBuilder();
			for (ArrayList<Float> point : points) {
				sb.append("\n\t");
				for (Float p : point) {
					sb.append(p + " ");
				}
			}
			return sb.toString();
		}
	}
}
