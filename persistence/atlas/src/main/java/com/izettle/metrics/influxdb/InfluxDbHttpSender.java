package com.izettle.metrics.influxdb;

import com.google.common.base.Strings;
import com.izettle.metrics.influxdb.utils.TimeUtils;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * An implementation of InfluxDbSender that writes to InfluxDb via http.
 */
public class InfluxDbHttpSender extends InfluxDbBaseSender {

  private final URL url;
  // The base64 encoded authString.
  private final String authStringEncoded;
  private final int connectTimeout;
  private final int readTimeout;

  /**
   * Creates a new http sender given connection details.
   *
   * @throws Exception exception while creating the influxDb sender(MalformedURLException)
   */
  public InfluxDbHttpSender(Configuration configuration)
      throws Exception {
    super(configuration);

    String endpoint = new URL(configuration.getProtocol(), configuration.getHost(),
        configuration.getPort(), "/write").toString();
    String queryDb = String
        .format("db=%s", URLEncoder.encode(configuration.getDatabase(), "UTF-8"));
    String queryPrecision = String
        .format("precision=%s", TimeUtils.toTimePrecision(configuration.getTimePrecision()));
    this.url = new URL(endpoint + "?" + queryDb + "&" + queryPrecision);

    if (Strings.isNullOrEmpty(configuration.getAuth())) {
      this.authStringEncoded = "";
    } else {
      this.authStringEncoded = Base64.encodeBase64String(configuration.getAuth().getBytes(UTF_8));

    }

    this.connectTimeout = configuration.getConnectTimeout();
    this.readTimeout = configuration.getReadTimeout();
  }


  @Override
  public int writeData(byte[] line) throws Exception {
    final HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("POST");
    if (authStringEncoded != null && !authStringEncoded.isEmpty()) {
      con.setRequestProperty("Authorization", "Basic " + authStringEncoded);
    }
    con.setDoOutput(true);
    con.setConnectTimeout(connectTimeout);
    con.setReadTimeout(readTimeout);

    OutputStream out = con.getOutputStream();
    try {
      out.write(line);
      out.flush();
    } finally {
      out.close();
    }

    int responseCode = con.getResponseCode();

    // Check if non 2XX response code.
    if (responseCode / 100 != 2) {
      throw new IOException(
          "Server returned HTTP response code: " + responseCode + " for URL: " + url
              + " with content :'"
              + con.getResponseMessage() + "'");
    }
    return responseCode;
  }
}
