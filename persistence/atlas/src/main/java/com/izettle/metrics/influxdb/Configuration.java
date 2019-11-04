package com.izettle.metrics.influxdb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/8/30
 */
public class Configuration implements Serializable {

  /**
   * the connect timeout
   */
  private int connectTimeout = 1500;

  /**
   * the read timeout
   */
  private int readTimeout = 1500;

  /**
   * the time precision of the metrics
   */
  private TimeUnit timePrecision = TimeUnit.MILLISECONDS;
  private String protocol = "http";

  /**
   * the influxDb hostname
   */
  private String host = "localhost";

  /**
   * the influxDb http port
   */
  private int port = 8086;

  private String prefix = "";

  private Map<String, String> tags = new HashMap<>();

  private ImmutableMap<String, ImmutableSet<String>> fields = ImmutableMap.of(
      "timers",
      ImmutableSet.of("p50", "p75", "p95", "p99", "p999", "m1_rate"),
      "meters",
      ImmutableSet.of("m1_rate"));

  /**
   * the influxDb database to write to
   */
  private String database = "";

  /**
   * the authorization string to be used to connect to InfluxDb, of format username:password
   */
  private String auth = "";

  public Configuration(String protocol, String host, int port, String database) {
    this.protocol = protocol;
    this.host = host;
    this.port = port;
    this.database = database;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  public void setReadTimeout(int readTimeout) {
    this.readTimeout = readTimeout;
  }

  public TimeUnit getTimePrecision() {
    return timePrecision;
  }

  public void setTimePrecision(TimeUnit timePrecision) {
    this.timePrecision = timePrecision;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public ImmutableMap<String, ImmutableSet<String>> getFields() {
    return fields;
  }

  public void setFields(
      ImmutableMap<String, ImmutableSet<String>> fields) {
    this.fields = fields;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getAuth() {
    return auth;
  }

  public void setAuth(String auth) {
    this.auth = auth;
  }

  @Override
  public String toString() {
    return String
        .format("Configuration[host=%s, port=%d, database=%s, protocol=%s, tags=%s]", host, port,
            database, protocol, tags.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, database, protocol);
  }

  @Override
  public boolean equals(Object obj) {
    Configuration other = (Configuration) obj;
    if (this.host == null || other.host == null) {
      if (this.host != other.host) {
        return false;
      }
    } else {
      if (!this.host.equals(other.host)) {
        return false;
      }
    }
    if (this.database == null || other.database == null) {
      if (this.database != other.database) {
        return false;
      }
    } else {
      if (!this.database.equals(other.database)) {
        return false;
      }
    }
    if (this.protocol == null || other.protocol == null) {
      if (this.protocol != other.protocol) {
        return false;
      }
    } else {
      if (!this.protocol.equals(other.protocol)) {
        return false;
      }
    }
    if (this.port != other.port) {
      return false;
    }

    return true;
  }
}
