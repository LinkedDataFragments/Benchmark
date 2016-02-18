package org.insight_centre.aceis.io.streams.querystreamer;

import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Acts as a proxy to the LDF server in which new data can be inserted.
 * @author Ruben Taelman
 */
public class QueryStreamerEndpoint {

    private final HttpClient httpclient = HttpClients.createDefault();
    private final Queue<String> buffer = new LinkedList<>();

    /**
     * Buffer the given triple to the server for insertion.
     * @param s The subject.
     * @param p The predicate.
     * @param o The object.
     *
     */
    public synchronized void stream(Resource s, Resource p, RDFNode o) {
        buffer.add(String.format("<%s> <%s> %s.", s.toString(), p.toString(),
                o.isLiteral() ? String.format("\"%s\"", o.toString()) : String.format("<%s>", o.toString())));
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
        // TODO: change base uri
        return String.format("http://localhost:4000/?initial=%s&final=%s&streamId=%s", timeInitial, timeFinal, streamId);
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

}
