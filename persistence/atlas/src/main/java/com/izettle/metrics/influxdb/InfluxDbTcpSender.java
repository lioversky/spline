package com.izettle.metrics.influxdb;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of InfluxDbSender that uses TCP Connection.
 *
 * Warning: This class uses non encrypted TCP connection to connect to the remote host.
 */
public class InfluxDbTcpSender extends InfluxDbBaseSender {

    private static final int NUM_OF_RETRIES = 2;
    private static final TimeUnit TCP_TIME_PRECISION = TimeUnit.NANOSECONDS;
    private final String hostname;
    private final int port;
    private final int socketTimeout;
    private Socket tcpSocket;

    /**
     * Creates an instance of [[InfluxDbTcpSender]]
     */
    public InfluxDbTcpSender(Configuration configuration) {
        super(configuration);
        this.hostname = configuration.getHost();
        this.port = configuration.getPort();
        this.socketTimeout = configuration.getConnectTimeout();

    }

    @Override
    public int writeData(byte[] line) throws Exception {
        retryConnect(false);

        for (int i = 1; i <= NUM_OF_RETRIES; i++) {
            try {
                OutputStream outputStream = tcpSocket.getOutputStream();
                outputStream.write(line);
                outputStream.flush();

                return 0;
            } catch (IOException e) {
                // In case of exception, force reconnect
                retryConnect(true);
            }
        }

        return 0;
    }

    /**
     * Creates a new socket to the server
     *
     * @param force Whether to force reconnect
     * @throws IOException On connection error (Server is not alive, not responding, etc')
     */
    private void retryConnect(boolean force) throws IOException {
        if (force || tcpSocket == null) {

            if (tcpSocket != null) {
                tcpSocket.close();
                tcpSocket = null;
            }

            tcpSocket = new Socket(hostname, port);
            tcpSocket.setSoTimeout(socketTimeout);
        }

    }

}
