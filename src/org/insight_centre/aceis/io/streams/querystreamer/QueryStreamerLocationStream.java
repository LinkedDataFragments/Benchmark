package org.insight_centre.aceis.io.streams.querystreamer;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.observations.AarhusParkingObservation;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.aceis.observations.WeatherObservation;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class QueryStreamerLocationStream extends QueryStreamerSensorStream {
	private static final Logger logger = LoggerFactory.getLogger(QueryStreamerLocationStream.class);
	private String txtFile;
	private EventDeclaration ed;

	public QueryStreamerLocationStream(QueryStreamerEndpoint endpoint, String uri, String txtFile, EventDeclaration ed)
			throws IOException {
		super(endpoint, uri);
		this.txtFile = txtFile;
		this.ed = ed;
	}

	@Override
	protected SensorObservation createObservation(Object data) {
		String str = data.toString();
		String userStr = str.split("\\|")[0];
		String coordinatesStr = str.split("\\|")[1];
		SensorObservation so = new SensorObservation();
		so.setFoi(userStr);
		// so.setServiceId(this.getURI());
		so.setValue(coordinatesStr);
		so.setObTimeStamp(new Date());
		so.setObId("UserLocationObservation-" + (int) Math.random() * 10000);
		// return so;
		this.currentObservation = so;
		return so;
	}

	@Override
	protected List<IdentifiableStatement> getStatements(SensorObservation so) throws NumberFormatException, IOException {
		List<IdentifiableStatement> identifiableStatements = new LinkedList<>();
		String userStr = so.getFoi();
		String coordinatesStr = so.getValue().toString();
		Model m = ModelFactory.createDefaultModel();
		double lat = Double.parseDouble(coordinatesStr.split(",")[0]);
		double lon = Double.parseDouble(coordinatesStr.split(",")[1]);
		Resource serviceID = m.createResource(ed.getServiceId());
		Resource observation = m.createResource(RDFFileManager.defaultPrefix + so.getObId() + UUID.randomUUID());
		CityBench.obMap.put(observation.toString(), so);
		observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));
		Resource coordinates = m.createResource();
		coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLatitude"), lat);
		coordinates.addLiteral(m.createProperty(RDFFileManager.ctPrefix + "hasLongitude"), lon);
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
				m.createResource(ed.getPayloads().get(0).split("\\|")[2]));
		observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
		// fake fixed foi
		observation
				.addProperty(
						m.createProperty(RDFFileManager.ssnPrefix + "featureOfInterest"),
						m.createResource("http://iot.ee.surrey.ac.uk/citypulse/datasets/aarhusculturalevents/culturalEvents_aarhus#context_do63jk2t8c3bjkfb119ojgkhs7"));

		observation.addProperty(m.createProperty(RDFFileManager.saoPrefix + "hasValue"), coordinates);
		identifiableStatements.add(new IdentifiableStatement(ed.getServiceId().split("#")[1], m.listStatements().toList()));
		return identifiableStatements;
	}

	public void run() {
		logger.info("Starting sensor stream: " + this.getUri());
		try {
			if (txtFile.contains("Location")) {
				BufferedReader reader = new BufferedReader(new FileReader(txtFile));
				String strLine;
				while ((strLine = reader.readLine()) != null && (!stop)) {
					handleStatements(this.getStatements(this.createObservation(strLine)));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			logger.info("Stream Terminated: " + this.getUri());
			this.stop();
		}
	}

}