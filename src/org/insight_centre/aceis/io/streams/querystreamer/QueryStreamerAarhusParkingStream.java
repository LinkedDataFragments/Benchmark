package org.insight_centre.aceis.io.streams.querystreamer;

import com.csvreader.CsvReader;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.DataWrapper;
import org.insight_centre.aceis.observations.AarhusParkingObservation;
import org.insight_centre.aceis.observations.SensorObservation;
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

public class QueryStreamerAarhusParkingStream extends QueryStreamerSensorStream {
	private static final Logger logger = LoggerFactory.getLogger(QueryStreamerAarhusParkingStream.class);
	EventDeclaration ed;
	private Date startDate = null, endDate = null;
	CsvReader streamData;
	String txtFile;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss");

	public QueryStreamerAarhusParkingStream(QueryStreamerEndpoint endpoint, String uri, String txtFile, EventDeclaration ed, Date start, Date end)
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
			int vehicleCnt = Integer.parseInt(streamData.get("vehiclecount")), id = Integer.parseInt(streamData
					.get("_id")), total_spaces = Integer.parseInt(streamData.get("totalspaces"));
			String garagecode = streamData.get("garagecode");
			Date obTime = sdf.parse(streamData.get("updatetime"));
			AarhusParkingObservation apo = new AarhusParkingObservation(total_spaces - vehicleCnt, garagecode, "", 0.0,
					0.0);
			apo.setObTimeStamp(obTime);
			// logger.info("Annotating obTime: " + obTime + " in ms: " + obTime.getTime());
			apo.setObId("AarhusParkingObservation-" + id);
			logger.debug(ed.getServiceId() + ": streaming record @" + apo.getObTimeStamp());
			DataWrapper.waitForInterval(this.currentObservation, apo, this.startDate, getRate());
			this.currentObservation = apo;
			return apo;
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			logger.error("ed parse error: " + ed.getServiceId());
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected List<IdentifiableStatement> getStatements(SensorObservation so) throws NumberFormatException, IOException {
		List<IdentifiableStatement> identifiableStatements = new LinkedList<>();

		Model m = ModelFactory.createDefaultModel();
		Resource observation = m.createResource(RDFFileManager.defaultPrefix + so.getObId() + UUID.randomUUID());
		CityBench.obMap.put(observation.toString(), so);
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		Resource serviceID = m.createResource(ed.getServiceId());
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
				m.createResource(ed.getPayloads().get(0).split("\\|")[2]));
		Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
		observation.addLiteral(hasValue, ((AarhusParkingObservation) so).getVacancies());
		identifiableStatements.add(new IdentifiableStatement(ed.getServiceId().split("#")[1], m.listStatements().toList()));
		return identifiableStatements;
	}

	public void run() {
		logger.info("Starting sensor stream: " + this.getUri() + " " + this.startDate + ", " + this.endDate);
		try {
			// Reads csv document for traffic metadata
			boolean completed = false;
			while (streamData.readRecord() && !stop) {
				Date obTime = sdf.parse(streamData.get("updatetime"));
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