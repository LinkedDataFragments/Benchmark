package org.insight_centre.citybench.main;

import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.rspengine.CqelsRspEngine;
import org.insight_centre.aceis.rspengine.CsparqlRspEngine;
import org.insight_centre.aceis.rspengine.RspEngine;
import org.insight_centre.aceis.rspengine.TpfQueryStreamerRspEngine;
import org.insight_centre.aceis.utils.test.PerformanceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class CityBench {
	private static final Map<String, RspEngine> rspEngines = new HashMap<>();
	static {
		registerRspEngine(new CqelsRspEngine());
		registerRspEngine(new CsparqlRspEngine());
		registerRspEngine(new TpfQueryStreamerRspEngine());
	}

	static void registerRspEngine(RspEngine rspEngine) {
		rspEngines.put(rspEngine.getId(), rspEngine);
	}

	private static final Logger logger = LoggerFactory.getLogger(CityBench.class);
	public static ConcurrentHashMap<String, SensorObservation> obMap = new ConcurrentHashMap<String, SensorObservation>();

	// HashMap<String, String> parameters;
	// Properties prop;

	public static void main(String[] args) {
		try {
			Properties prop = new Properties();
			// logger.info(Main.class.getClassLoader().);
			File in = new File("citybench.properties");
			FileInputStream fis = new FileInputStream(in);
			prop.load(fis);
			fis.close();
			// Thread.
			HashMap<String, String> parameters = new HashMap<String, String>();
			for (String s : args) {
				parameters.put(s.split("=")[0], s.split("=")[1]);
			}
			CityBench cb = new CityBench(prop, parameters);
			cb.startTest();
			// BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			// System.out.print("Please press a key to stop the server.");
			// reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (Exception e) {
			// logger.error(e.getMessage());
			e.printStackTrace();
			System.exit(0);
		}
	}

	public String dataset, ontology, streams;
	public long duration = 0; // experiment time in milliseconds
	public final RspEngine engine;
	public EventRepository er;
	public double frequency = 1.0;
	public static PerformanceMonitor pm;
	private List<String> queries;
	int queryDuplicates = 1;
	private Map<String, String> queryMap = new HashMap<String, String>();

	// public Map<String, String> getQueryMap() {
	// return queryMap;
	// }

	public double rate = 1.0; // stream rate factor
	public static ConcurrentHashMap<String, Object> registeredQueries = new ConcurrentHashMap<String, Object>();
	public static List startedStreamObjects = new ArrayList();
	private static String resultName;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	public Date start, end;
	public Set<String> startedStreams = new HashSet<String>();

	// private double rate,frequency
	// DatasetTDB
	/**
	 * @param prop
	 * @param parameters
	 *            Acceptable params: rates=(double)x, queryDuplicates=(int)y, duration=(long)z, startDate=(date in the
	 *            format of "yyyy-MM-dd'T'HH:mm:ss")a, endDate=b, frequency=(double)c. Start and end dates are
	 *            mandatory.
	 * @throws Exception
	 */
	public CityBench(Properties prop, HashMap<String, String> parameters) throws Exception {
		// parse configuration file
		// this.parameters = parameters;
		try {
			this.dataset = prop.getProperty("dataset");
			this.ontology = prop.getProperty("ontology");
			for(RspEngine engine : rspEngines.values()) {
				String directory = prop.getProperty(engine.getId() + "_query");
				engine.setQueryDirectory(directory);
			}
			this.streams = prop.getProperty("streams");
			if (this.dataset == null || this.ontology == null || this.streams == null)
				throw new Exception("Configuration properties incomplete.");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);

		}

		// parse parameters
		if (parameters.containsKey("query")) {
			this.queries = Arrays.asList(parameters.get("query").split(","));
		}
		if (parameters.get("queryDuplicates") != null) {
			queryDuplicates = Integer.parseInt(parameters.get("queryDuplicates"));
		}
		if (parameters.containsKey("engine")) {
			String engine = parameters.get("engine");
			this.engine = rspEngines.get(engine);
			if (this.engine == null) {
				throw new Exception(String.format("RSP Engine %s not supported.", engine));
			} else if (this.engine.getQueryDirectory() == null) {
				throw new Exception(String.format("Configuration properties incomplete, " +
						"required a directory property %s", this.engine.getId() + "_query"));
			}
		} else
			throw new Exception("RSP Engine not specified.");

		if (parameters.containsKey("rate")) {
			this.rate = Double.parseDouble(parameters.get("rate"));
		}
		if (parameters.containsKey("duration")) {
			String durationStr = parameters.get("duration");
			String valueStr = durationStr.substring(0, durationStr.length() - 1);
			if (durationStr.contains("s"))
				duration = Integer.parseInt(valueStr) * 1000;
			else if (durationStr.contains("m"))
				duration = Integer.parseInt(valueStr) * 60000;
			else
				throw new Exception("Duration specification invalid.");
		}
		if (parameters.containsKey("queryDuplicates"))
			this.queryDuplicates = Integer.parseInt(parameters.get("queryDuplicates"));
		if (parameters.containsKey("startDate"))
			this.start = sdf.parse(parameters.get("startDate"));
		else
			throw new Exception("Start date not specified");
		if (parameters.containsKey("endDate"))
			this.end = sdf.parse(parameters.get("endDate"));
		else
			throw new Exception("End date not specified");
		if (parameters.containsKey("frequency"))
			this.frequency = Double.parseDouble(parameters.get("frequency"));

		logger.info("Parameters loaded: engine - " + this.engine + ", queries - " + this.queries + ", rate - "
				+ this.rate + ", frequency - " + this.frequency + ", duration - " + this.duration + ", duplicates - "
				+ this.queryDuplicates + ", start - " + this.start + ", end - " + this.end);

		new File("result_log" + File.separator + this.engine.toString()).mkdir();
		this.resultName = this.engine + File.separator
				+ String.format("r=%s;f=%s;dup=%s;q=%s",
				this.rate, this.frequency, this.queryDuplicates, this.queries);
		this.engine.init(this.dataset);
		this.er = RDFFileManager.buildRepoFromFile(0);
	}

	public void deployQuery(String qid, String query) {
		try {
			this.engine.startStreamsFromQuery(this, query);
			this.engine.registerQuery(this, qid, query);

		} catch (ParseException e) {
			logger.info("Error parsing query: " + qid);
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getResultName() {
		return resultName;
	}

	/**
	 * parse the stream URIs in the queries and identify the stream file locations
	 * 
	 * @return list of stream files
	 */
	List<String> getStreamFileNames() {
		Set<String> resultSet = new HashSet<String>();
		for (Entry en : this.queryMap.entrySet()) {
			try {
				resultSet.addAll(this.getStreamFileNamesFromQuery(en.getValue() + ""));
			} catch (Exception e) {
				logger.error("Error trying to get stream files.");
				e.printStackTrace();
			}
		}
		List<String> results = new ArrayList<String>();
		results.addAll(resultSet);
		return results;

	}

	public List<String> getStreamFileNamesFromQuery(String query) throws Exception {
		Set<String> resultSet = new HashSet<String>();
		String[] streamSegments = query.trim().split("stream");
		if (streamSegments.length == 1)
			throw new Exception("Error parsing query, no stream statements found for: " + query);
		else {
			for (int i = 1; i < streamSegments.length; i++) {
				int indexOfLeftBracket = streamSegments[i].trim().indexOf("<");
				int indexOfRightBracket = streamSegments[i].trim().indexOf(">");
				String streamURI = streamSegments[i].substring(indexOfLeftBracket + 2, indexOfRightBracket + 1);
				logger.info("Stream detected: " + streamURI);
				resultSet.add(streamURI.split("#")[1] + ".stream");
			}
		}

		List<String> results = new ArrayList<String>();
		results.addAll(resultSet);
		return results;
	}

	private List<String> loadQueries() throws Exception {
		String qd = this.engine.getQueryDirectory();
		if (this.queries == null) {
			File queryDirectory = new File(qd);
			if (!queryDirectory.exists())
				throw new Exception("Query directory not exist. " + qd);
			else if (!queryDirectory.isDirectory())
				throw new Exception("Query path specified is not a directory.");
			else {
				File[] queryFiles = queryDirectory.listFiles();
				if (queryFiles != null) {
					for (File queryFile : queryFiles) {
						String qid = queryFile.getName().split("\\.")[0];
						String qStr = new String(Files.readAllBytes(java.nio.file.Paths.get(queryDirectory
								+ File.separator + queryFile.getName())));
						if (this.engine != null)
							qStr = this.engine.transformQuery(qid, qStr);
						this.queryMap.put(qid, qStr);
					}
				} else
					throw new Exception("Cannot find query files.");
			}
		} else {
			for (String qid : this.queries) {
				try {

					File queryFile = new File(qd + File.separator + qid);
					String qStr = new String(Files.readAllBytes(queryFile.toPath()));
					qid = qid.split("\\.")[0];
					if (this.engine != null)
						qStr = this.engine.transformQuery(qid, qStr);
					this.queryMap.put(qid, qStr);
				} catch (Exception e) {
					logger.error("Could not load query file.");
					e.printStackTrace();
				}
			}
		}
		// throw new Exception("Query directory not specified;");
		return null;
	}

	protected void startTest() throws Exception {
		// load queries from query directory, each file contains 1 query
		this.loadQueries();
		pm = new PerformanceMonitor(queryMap, duration, queryDuplicates, resultName, this);
		new Thread(pm).start();
		this.engine.startTests(this, queryMap, queryDuplicates);

	}
}
