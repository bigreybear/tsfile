package org.apache.tsfile.exps.conf;

import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {

  private static final String FILE_NAME = "config.properties";
  public String prjDir;
  public final Properties properties;

  private ConfigLoader() {
    properties = new Properties();

    try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(FILE_NAME)) {
      if (is != null) {
        properties.load(is);
        System.out.println("Loaded configuration from classpath resource");
      } else {
        throw new RuntimeException("Configuration file not found in classpath: " + FILE_NAME);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Failed to load configuration", ex);
    }
  }

  private static class Holder {
    private static final ConfigLoader INSTANCE = new ConfigLoader();
  }

  public static ConfigLoader getInstance() {
    return Holder.INSTANCE;
  }

  public String getProperty(String pn) {
    return getInstance().properties.getProperty(pn);
  }

  public static void main(String[] args) {
    System.out.println(String.format("Try config file: %s",
        ConfigLoader.getInstance().getProperty("verifyID")));
  }
}
