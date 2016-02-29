package org.insight_centre.aceis.io.streams.querystreamer;

import com.csvreader.CsvReader;
import com.google.common.collect.Lists;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.DataWrapper;
import org.insight_centre.aceis.observations.PollutionObservation;
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

public class QueryStreamerAarhusPollutionStream extends QueryStreamerSensorStream {
	private static final Logger logger = LoggerFactory.getLogger(QueryStreamerAarhusPollutionStream.class);
	EventDeclaration ed;
	private Date startDate = null, endDate = null;
	CsvReader streamData;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");

	public QueryStreamerAarhusPollutionStream(QueryStreamerEndpoint endpoint, String uri, String txtFile, EventDeclaration ed, Date start, Date end)
			throws IOException {
		super(endpoint, uri);
		streamData = new CsvReader(String.valueOf(txtFile));
		this.ed = ed;
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();
		this.startDate = start;
		this.endDate = end;
	}

	@Override
	protected PollutionObservation createObservation(Object objData) {
		try {
			// CsvReader streamData = (CsvReader) data;
			int ozone = Integer.parseInt(streamData.get("ozone")), particullate_matter = Integer.parseInt(streamData
					.get("particullate_matter")), carbon_monoxide = Integer.parseInt(streamData.get("carbon_monoxide")), sulfure_dioxide = Integer
					.parseInt(streamData.get("sulfure_dioxide")), nitrogen_dioxide = Integer.parseInt(streamData
					.get("nitrogen_dioxide"));
			Date obTime = sdf.parse(streamData.get("timestamp"));
			PollutionObservation po = new PollutionObservation(0.0, 0.0, 0.0, ozone, particullate_matter,
					carbon_monoxide, sulfure_dioxide, nitrogen_dioxide, obTime);
			// logger.debug(ed.getServiceId() + ": streaming record @" + po.getObTimeStamp());
			po.setObId("AarhusPollutionObservation-" + (int) Math.random() * 10000);
			DataWrapper.waitForInterval(currentObservation, po, startDate, getRate());
			this.currentObservation = po;
			return po;
		} catch (NumberFormatException | IOException | ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected List<IdentifiableStatement> getStatements(SensorObservation so) throws NumberFormatException, IOException {
		List<IdentifiableStatement> identifiableStatements = new LinkedList<>();
		if (ed != null) {
			Model m = ModelFactory.createDefaultModel();
			List<Statement> statements = Lists.newLinkedList();
			for (String s : ed.getPayloads()) {
				Resource observation = m
						.createResource(RDFFileManager.defaultPrefix + so.getObId() + UUID.randomUUID());
				// so.setObId(RDFFileManager.defaultPrefix + observation.toString());
				CityBench.obMap.put(observation.toString(), so);
				observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));

				Resource serviceID = m.createResource(ed.getServiceId());
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
						m.createResource(s.split("\\|")[2]));
				Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
				observation.addLiteral(hasValue, ((PollutionObservation) so).getApi());
			}
			statements.addAll(m.listStatements().toList());
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
				Date obTime = sdf.parse(streamData.get("timestamp"));
				logger.debug("Reading data: " + streamData.toString());
				if (this.startDate != null && this.endDate != null) {
					if (obTime.before(this.startDate) || obTime.after(this.endDate)) {
						logger.debug(this.getUri() + ": Disgarded observation @" + obTime);
						continue;
					}
				}
				PollutionObservation data = this.createObservation(streamData);
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