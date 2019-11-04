package com.izettle.metrics.influxdb;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of InfluxDbSender that uses UDP Connection.
 * <p>
 * Warning: This class uses non encrypted UDP connection to connect to the remote host.
 */
public class InfluxDbUdpSender extends InfluxDbBaseSender {

  private static final TimeUnit UDP_TIME_PRECISION = TimeUnit.NANOSECONDS;

  private final String hostname;
  private final int port;
  private final int socketTimeout;
  private DatagramSocket udpSocket;

  public InfluxDbUdpSender(Configuration configuration) {
    super(configuration);
    this.hostname = configuration.getHost();
    this.port = configuration.getPort();
    this.socketTimeout = configuration.getConnectTimeout();
  }

  @Override
  public int writeData(byte[] line) throws Exception {
    createSocket();

    udpSocket.send(new DatagramPacket(line, line.length, InetAddress.getByName(hostname), port));

    return 0;
  }

  private void createSocket() throws IOException {
    if (udpSocket == null) {
      udpSocket = new DatagramSocket();
      udpSocket.setSoTimeout(socketTimeout);
    }
  }
}
