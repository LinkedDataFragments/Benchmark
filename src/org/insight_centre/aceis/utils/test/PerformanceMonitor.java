package org.insight_centre.aceis.utils.test;

import com.csvreader.CsvWriter;
import com.google.common.collect.Lists;
import org.insight_centre.aceis.rspengine.RspEngine;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class PerformanceMonitor implements Runnable {
	private Map<String, String> qMap;
	private long duration;
	private int duplicates;
	private String resultName;
	private long start = 0;
	private ConcurrentHashMap<String, List<Long>> latencyMap = new ConcurrentHashMap<String, List<Long>>();
	private List<Double> serverMemories = Lists.newLinkedList();
	private List<Double> clientMemories = Lists.newLinkedList();
	private List<Double> serverCpus = Lists.newLinkedList();
	private List<Double> clientCpus = Lists.newLinkedList();
	private ConcurrentHashMap<String, Long> resultCntMap = new ConcurrentHashMap<String, Long>();
	private CsvWriter cw;
	private long resultInitTime = 0, lastCheckPoint = 0, globalInit = 0;
	private boolean stop = false;
	private List<String> qList;
	private final CityBench cityBench;
	private static final Logger logger = LoggerFactory.getLogger(PerformanceMonitor.class);

	public PerformanceMonitor(Map<String, String> queryMap, long duration, int duplicates, String resultName, CityBench cityBench)
			throws Exception {
		this.cityBench = cityBench;
		qMap = queryMap;
		this.duration = duration;
		this.resultName = resultName;
		this.duplicates = duplicates;
		File outputFile = new File("result_log" + File.separator + resultName + ".csv");
		if (outputFile.exists())
			throw new Exception("Result log file already exists.");
		cw = new CsvWriter(new FileWriter(outputFile, true), ',');
		cw.write("");
		qList = new ArrayList(this.qMap.keySet());
		Collections.sort(qList);

		for (String qid : qList) {
			latencyMap.put(qid, new ArrayList<Long>());
			resultCntMap.put(qid, (long) 0);
			cw.write("latency-" + qid);
		}
		// for (String qid : qList) {
		// cw.write("cnt-" + qid);
		// }
		cw.write("server-memory");
		cw.write("client-memory");
		cw.write("server-cpu");
		cw.write("client-cpu");
		cw.endRecord();
		// cw.flush();
		// cw.
		this.globalInit = System.currentTimeMillis();
	}

	public void run() {
		int minuteCnt = 0;
		while (!stop) {
			try {
				boolean shouldStop = ((System.currentTimeMillis() - this.globalInit) > 1.5 * duration)
						|| (duration != 0 && resultInitTime != 0 && (System.currentTimeMillis() - this.resultInitTime) > (30000 + duration));
				if (shouldStop || this.lastCheckPoint != 0 && (System.currentTimeMillis() - this.lastCheckPoint) >= 60000) {
					minuteCnt += 1;

					this.lastCheckPoint = System.currentTimeMillis();
					cw.write(minuteCnt + "");
					for (String qid : this.qList) {
						double latency = 0.0;
						for (long l : this.latencyMap.get(qid))
							latency += l;
						latency = (latency + 0.0) / (this.latencyMap.get(qid).size() + 0.0);
						cw.write(latency + "");

					}
					// for (String qid : this.qList)
					// cw.write((this.resultCntMap.get(qid) / (this.duplicates + 0.0)) + "");
					cw.write(averageAndClear(serverMemories) + "");
					cw.write(averageAndClear(clientMemories) + "");
					cw.write(averageAndClear(serverCpus) + "");
					cw.write(averageAndClear(clientCpus) + "");
					cw.endRecord();
					cw.flush();
					logger.info("Results logged.");

					// empty memory and latency lists
					for (Entry<String, List<Long>> en : this.latencyMap.entrySet()) {
						en.getValue().clear();
					}
				}

				Map<String, Double> currentLatency = new HashMap<String, Double>();
				for (String qid : this.qList) {
					double latency = 0.0;
					for (long l : this.latencyMap.get(qid))
						latency += l;
					latency = (latency + 0.0) / (this.latencyMap.get(qid).size() + 0.0);
					currentLatency.put(qid, latency);
				}

				// Map<String,Long> currentResults=new HashMap<String>

				// ConcurrentHashMap<String, SensorObservation> obMapBytes = CityBench.obMap;
				// double obMapBytes = 0.0;
				// try {
				// ByteArrayOutputStream baos = new ByteArrayOutputStream();
				// ObjectOutputStream oos = new ObjectOutputStream(baos);
				// oos.writeObject(CityBench.obMap);
				// oos.close();
				// obMapBytes = (0.0 + baos.size());
				// } catch (Exception e) {
				// e.printStackTrace();
				// }
				// long listerObIdListBytes = 0;
				// for (Object listener : CityBench.registeredQueries.values()) {
				//
				// if (listener instanceof CQELSResultListener) {
				// for (String obid : ((CQELSResultListener) listener).capturedObIds)
				// listerObIdListBytes += obid.getBytes().length;
				// } else {
				// for (String obid : ((CSPARQLResultObserver) listener).capturedObIds)
				// listerObIdListBytes += obid.getBytes().length;
				// }
				// }
				// long listenerResultListBytes = 0;
				// for (Object listener : CityBench.registeredQueries.values()) {
				//
				// if (listener instanceof CQELSResultListener) {
				// for (String result : ((CQELSResultListener) listener).capturedResults)
				// listenerResultListBytes += result.getBytes().length;
				// } else {
				// for (String result : ((CSPARQLResultObserver) listener).capturedResults)
				// listenerResultListBytes += result.getBytes().length;
				// }
				// }
				System.gc();
				RspEngine e = cityBench.engine;
				double serverUsedMB = e.getServerMemoryUsage() / 1024.0 / 1024.0;
				double clientUsedMB = e.getClientMemoryUsage() / 1024.0 / 1024.0;
				double serverCpu = e.getServerCpu();
				double clientCpu = e.getClientCpu();
				// double overhead = (obMapBytes + listerObIdListBytes + listenerResultListBytes) / 1024.0 / 1024.0;
				this.serverMemories.add(serverUsedMB);
				this.clientMemories.add(clientUsedMB);
				this.serverCpus.add(serverCpu);
				this.clientCpus.add(clientCpu);
				logger.info(String.format("Current performance: L - %s, Cnt: %s, SMem - %s, SCpu - %s, CMem: %s, CCpu: %s",
						currentLatency, this.resultCntMap,
						serverUsedMB, serverCpu,
						clientUsedMB, clientCpu));// + ", monitoring overhead - " + overhead);

				if (shouldStop) {
					this.cw.flush();
					this.cw.close();
					logger.info("Stopping after " + (System.currentTimeMillis() - this.globalInit) + " ms.");
					this.cleanup();
					logger.info("Experiment stopped.");
					System.exit(0);
				}

				Thread.sleep(5000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.exit(0);
	}

	protected static double averageAndClear(List<Double> list) {
		double value = 0.0;
		for (double m : list) {
			value += m;
		}
		value = value / ((double) list.size());
		list.clear();
		return value;
	}

	private void cleanup() {
		cityBench.engine.destroy(cityBench);
		this.stop = true;
		System.gc();
	}

	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}

	public synchronized void addResults(String qid, Map<String, Long> results, int cnt) {
		if (this.resultInitTime == 0) {
			this.resultInitTime = System.currentTimeMillis();
			this.lastCheckPoint = System.currentTimeMillis();
		}
		qid = qid.split("-")[0];
		for (Entry en : results.entrySet()) {
			String obid = en.getKey().toString();
			long delay = (long) en.getValue();
			this.latencyMap.get(qid).add(delay);
		}
		this.resultCntMap.put(qid, this.resultCntMap.get(qid) + cnt);
	}
}
