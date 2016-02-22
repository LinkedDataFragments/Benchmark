package org.insight_centre.aceis.io.streams.querystreamer;

import com.csvreader.CsvReader;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.TrafficReportService;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.DataWrapper;
import org.insight_centre.aceis.observations.AarhusTrafficObservation;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.observations.WeatherObservation;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class QueryStreamerAarhusWeatherStream extends QueryStreamerSensorStream {
	private static final Logger logger = LoggerFactory.getLogger(QueryStreamerAarhusWeatherStream.class);
	EventDeclaration ed;
	private Date startDate = null, endDate = null;
	CsvReader streamData;
	String txtFile;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	public QueryStreamerAarhusWeatherStream(QueryStreamerEndpoint endpoint, String uri, String txtFile, EventDeclaration ed, Date start, Date end)
			throws IOException {
		super(endpoint, uri);
		this.startDate = start;
		this.endDate = end;
		this.txtFile = txtFile;
		streamData = new CsvReader(String.valueOf(txtFile));
		this.ed = ed;
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();
	}

	@Override
	protected SensorObservation createObservation(Object objData) {
		try {
			// CsvReader streamData = (CsvReader) data;
			int hum = Integer.parseInt(streamData.get("hum"));
			double tempm = Double.parseDouble(streamData.get("tempm"));
			double wspdm = Double.parseDouble(streamData.get("wspdm"));
			Date obTime = sdf.parse(streamData.get("TIMESTAMP"));
			WeatherObservation wo = new WeatherObservation(tempm, hum, wspdm, obTime);
			logger.debug(ed.getServiceId() + ": streaming record @" + wo.getObTimeStamp());
			wo.setObId("AarhusWeatherObservation-" + (int) Math.random() * 1000);
			// this.currentObservation = wo;
			DataWrapper.waitForInterval(currentObservation, wo, startDate, getRate());
			this.currentObservation = wo;
			return wo;
		} catch (NumberFormatException | IOException | ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected List<IdentifiableStatement> getStatements(SensorObservation wo) throws NumberFormatException, IOException {
		List<IdentifiableStatement> identifiableStatements = new LinkedList<>();
		if (ed != null) {
			List<Statement> statements = Lists.newLinkedList();
			for (String s : ed.getPayloads()) {
				Model m = ModelFactory.createDefaultModel();
				Resource observation = m
						.createResource(RDFFileManager.defaultPrefix + wo.getObId() + UUID.randomUUID());
				// wo.setObId(observation.toString());
				CityBench.obMap.put(observation.toString(), wo);
				observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
				Resource serviceID = m.createResource(ed.getServiceId());
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
						m.createResource(s.split("\\|")[2]));
				Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
				if (s.contains("Temperature"))
					observation.addLiteral(hasValue, ((WeatherObservation) wo).getTemperature());
				else if (s.toString().contains("Humidity"))
					observation.addLiteral(hasValue, ((WeatherObservation) wo).getHumidity());
				else if (s.toString().contains("WindSpeed"))
					observation.addLiteral(hasValue, ((WeatherObservation) wo).getWindSpeed());
				statements.addAll(m.listStatements().toList());
			}
			identifiableStatements.add(new IdentifiableStatement(ed.getServiceId().split("#")[1], statements));
		}
		return identifiableStatements;
	}

	public void run() {
		logger.info("Starting sensor stream: " + this.getUri() + " " + this.startDate + ", " + this.endDate);
		try {
			// Reads csv document for traffic metadata
			boolean completed = false;
			while (streamData.readRecord() && !stop) {
				Date obTime = sdf.parse(streamData.get("TIMESTAMP").toString());
				logger.debug("Reading data: " + streamData.toString());
				if (this.startDate != null && this.endDate != null) {
					if (obTime.before(this.startDate) || obTime.after(this.endDate)) {
						logger.debug(this.getUri() + ": Disgarded observation @" + obTime);
						continue;
					}
				}
				SensorObservation data = this.createObservation(streamData);
				List<IdentifiableStatement> stmts = this.getStatements(data);
				try {
					if (completed) {
						logger.info("My mission completed: " + this.getUri());
						Thread.sleep(sleep);
						continue;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();

				}
				handleStatements(stmts);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			logger.info("Stream Terminated: " + this.getUri());
			this.stop();
		}
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

}