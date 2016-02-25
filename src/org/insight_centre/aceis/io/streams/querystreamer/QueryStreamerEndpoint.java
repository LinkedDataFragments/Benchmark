package org.insight_centre.aceis.io.streams.querystreamer;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Acts as a proxy to the LDF server in which new data can be inserted.
 * @author Ruben Taelman
 */
public class QueryStreamerEndpoint {

    private final int insertPort;
    private final HttpClient httpclient = HttpClients.createDefault();
    private final Queue<String> buffer = new LinkedList<>();

    public QueryStreamerEndpoint(int insertPort) {
        this.insertPort = insertPort;
    }

    /**
     * Buffer the given triple to the server for insertion.
     * @param s The subject.
     * @param p The predicate.
     * @param o The object.
     * @param g The graph
     */
    public synchronized void stream(Resource s, Resource p, RDFNode o, String g) {
        String line = String.format("<%s> <%s> %s", s.toString(), p.toString(),
                o.isLiteral() ? String.format("\"%s\"", o.toString().replaceAll("\"", "'")) : String.format("<%s>", o.toString()));
        if(g != null) {
            line += String.format(" <%s>", g);
        }
        line += ".";
        buffer.add(line);
    }

    /**
     * Send the buffer to the server for insertion.
     * @param timeInitial The initial buffer timestamp, can be -1.
     * @param timeFinal The final buffer timestamp.
     * @param streamId The name id for the stream that will be flushed.
     */
    public synchronized void flush(long timeInitial, long timeFinal, String streamId) {
        String triple;
        StringBuilder sb = new StringBuilder();
        while((triple = buffer.poll()) != null) {
            sb.append(triple);
            sb.append("\n");
        }
        String body = sb.toString();
        if(!body.isEmpty()) {
            try {
                postTriples(getUrl(timeInitial, timeFinal, streamId), body);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected String getUrl(long timeInitial, long timeFinal, String streamId) {
        return String.format("http://localhost:%s/?initial=%s&final=%s&streamId=%s", insertPort, timeInitial, timeFinal, streamId);
    }

    protected void postTriples(String url, String body) throws IOException {
        HttpPost httppost = new HttpPost(url);

        // Request parameters and other properties.
        httppost.setEntity(new StringEntity(body));

        //Execute and get the response.
        HttpResponse response = httpclient.execute(httppost);
        HttpEntity entity = response.getEntity();

        if (entity != null) {
            try(InputStream instream = entity.getContent()) {
                // do nothing useful
            }
        }
    }

    public void insertStaticData(String graph, Model model) {
        Iterator<Statement> it = model.listStatements();
        while(it.hasNext()) {
            Statement stmnt = it.next();
            stream(stmnt.getSubject(), stmnt.getPredicate(), stmnt.getObject(), graph);
        }
        flush(-1, -1, "STATIC");
    }

    public void insertStaticData(Dataset dataset) {
        insertStaticData(null, dataset.getDefaultModel());
        Iterator<String> namesIt = dataset.listNames();
        while(namesIt.hasNext()) {
            String modelName = namesIt.next();
            insertStaticData(modelName, dataset.getNamedModel(modelName));
        }
    }
}
