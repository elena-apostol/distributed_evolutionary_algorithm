package ro.pub.ga.watchmaker.example;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TSPFileParser {

  private final String USAGE_MSG = "Usage: java TSP population_size max_generations max_runs [file.tsp]";
  private final String FORMAT_MSG = "The command-line arguments have an incorrect format!";

  private int populationSize;
  private int maxGenerations;
  private int maxRuns;
  private boolean isTSPFileIn;
  private int graph [][];

  private String fileName;

  public TSPFileParser(String [] args) throws TSPException {

    if( args.length < 3 )
      throw new TSPException( USAGE_MSG );

    try {
      this.populationSize = Integer.parseInt(args[0]);
      this.maxGenerations = Integer.parseInt(args[1]);
      this.maxRuns = Integer.parseInt(args[2]);
    } catch(NumberFormatException e) {
      throw new TSPException( FORMAT_MSG );
    }

    if( args.length == 4 ) {
      this.fileName = args[3];
      this.graph = parseFile(this.fileName);
      this.isTSPFileIn = true;
    } else {
      this.fileName = null;
      this.graph = null;
      this.isTSPFileIn = false;
    }
  }

  public int getPopulationSize() { return this.populationSize; }
  public int getMaxGenerations() { return this.maxGenerations; }
  public int getMaxRuns() { return this.maxRuns; }
  public boolean isTSPFileIn() { return this.isTSPFileIn; }
  public int [][] getGraph() { return this.graph; }

  public static int [][] parseFile(String fileName) throws TSPException {

    // Supported file types
    int EUC_2D = 1;
    int GEO    = 2;

    Vector coords = new Vector();
    int fileType = 0;
    
    try {
    	
      //BufferedReader in = new BufferedReader(new FileReader(fileName));
      //modified to read from hdfs;
		Path pt=new Path(fileName);
	    FileSystem fs = FileSystem.get(new Configuration());
	    BufferedReader in=new BufferedReader(new InputStreamReader(fs.open(pt)));
      String line;
      boolean nodeCoordSection = false;
      
      while( (line = in.readLine()) != null ) {
	if( !line.equalsIgnoreCase("EOF") && !line.equalsIgnoreCase(" EOF")&& !line.equals("") ) {
	  if( !line.equalsIgnoreCase("NODE_COORD_SECTION") && !nodeCoordSection ) {
		  
	    if( line.trim().equalsIgnoreCase("EDGE_WEIGHT_TYPE : EUC_2D") ){
	      fileType = EUC_2D;
	    }
	    else if( line.trim().equalsIgnoreCase("EDGE_WEIGHT_TYPE : GEO") ) {
	    	    
	      fileType = GEO;
	    }
	    
	  } else if( line.equalsIgnoreCase("NODE_COORD_SECTION") ) {
	    nodeCoordSection = true;
	  } else { // All the numbers are in this part
	    
	    StringTokenizer strTok = new StringTokenizer(line, " \t");
	    try {
	      
	    	
	      strTok.nextToken(); // Discard the city number
	      if( fileType == EUC_2D || fileType == GEO ) {
		double x = Double.valueOf( strTok.nextToken() ).doubleValue();
		double y = Double.valueOf( strTok.nextToken() ).doubleValue();
		coords.addElement(new TSPCoordinate(x,y));
	      } else
		throw new TSPException( "Unrecognized file format!" );
	    } catch(NoSuchElementException e) {
	      throw new TSPException( "Could not parse file " + "'" + fileName + "'!" );
	    }
	  }
	}
      }
    } catch(FileNotFoundException e) {
      throw new TSPException( "File " + "'" + fileName + "'" + " not found in the current directory!" );
    } catch(IOException e) {
      throw new TSPException( "Could not read from file " + "'" + fileName + "'!" );
    }

    int graph [][] = new int[coords.size()][coords.size()];
    
    for(int i=0; i<graph.length; i++) {
      for(int j=0; j<=i; j++) {
        if( i == j )
          graph[i][j] = 0;
        else {
	  if( fileType == EUC_2D ) {
	    double dX = ((TSPCoordinate) coords.elementAt(i)).getX() - ((TSPCoordinate) coords.elementAt(j)).getX();
	    double dY = ((TSPCoordinate) coords.elementAt(i)).getY() - ((TSPCoordinate) coords.elementAt(j)).getY();
	    graph[i][j] = (int) Math.round( Math.sqrt( dX*dX + dY*dY ) );
	  } else if( fileType == GEO ) {
	    double deg = Math.floor( ((TSPCoordinate) coords.elementAt(i)).getX() );
	    double min = ((TSPCoordinate) coords.elementAt(i)).getX() - deg;
	    double latitudeI = Math.PI * ( deg + 5.0 * min / 3.0 ) / 180.0;
	    
	    deg = Math.floor( ((TSPCoordinate) coords.elementAt(i)).getY() );
	    min = ((TSPCoordinate) coords.elementAt(i)).getY() - deg;
	    double longitudeI = Math.PI * ( deg + 5.0 * min / 3.0 ) / 180.0;

	    deg = Math.floor( ((TSPCoordinate) coords.elementAt(j)).getX() );
	    min = ((TSPCoordinate) coords.elementAt(j)).getX() - deg;
	    double latitudeJ = Math.PI * ( deg + 5.0 * min / 3.0 ) / 180.0;

	    deg = Math.floor( ((TSPCoordinate) coords.elementAt(j)).getY() );
	    min = ((TSPCoordinate) coords.elementAt(j)).getY() - deg;
	    double longitudeJ = Math.PI * ( deg + 5.0 * min / 3.0 ) / 180.0;

	    double RRR = 6378.388;
	    double q1 = Math.cos( longitudeI - longitudeJ );
	    double q2 = Math.cos( latitudeI - latitudeJ );
	    double q3 = Math.cos( latitudeI + latitudeJ );
	    graph[i][j] = (int) Math.round( (RRR*Math.acos(0.5*((1.0+q1)*q2-(1.0-q1)*q3))+1.0) );
	  } else
	    throw new TSPException( "Unrecognized file format!" );
          graph[j][i] = graph[i][j];
        }

      }
    }

    return graph;
  }
}