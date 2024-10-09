package org.apache.tsfile.exps.conf;

import jdk.nashorn.internal.runtime.regexp.joni.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Properties;

public class ConfigLoader {

  private static final String FILE_NAME = "config.properties";
  public String prjDir;
  public final Properties properties;

  private ConfigLoader() throws IOException, URISyntaxException {
    properties = new Properties();


    // 首先尝试从当前工作目录加载
    String externalPath = Paths.get(FILE_NAME).toAbsolutePath().toString();

    File tf = new File(externalPath);
    // System.out.println(String.format("Trying to read config from %s", tf.getAbsolutePath()));

    try (FileInputStream fis = new FileInputStream(externalPath)) {
      properties.load(fis);
      System.out.println("Loaded configuration from external file: " + tf.getAbsoluteFile());
    } catch (FileNotFoundException e) {

      URL u = ConfigLoader.class.getClassLoader().getResource(FILE_NAME);
      try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(FILE_NAME)) {
        if (is != null) {
          properties.load(is);
          System.out.println(String.format("Loaded configuration from %s", u.toURI().getPath()));
        } else {
          throw new RuntimeException("Configuration file not found in classpath: " + FILE_NAME);
        }
      } catch (Exception ex) {
        throw new RuntimeException("Failed to load configuration", ex);
      }
    }
  }

  private static class Holder {
    private static final ConfigLoader INSTANCE;

    static {
      try {
        INSTANCE = new ConfigLoader();
      } catch (IOException | URISyntaxException e) {
        System.out.println("Initiating failed.");
        throw new RuntimeException(e);
      }
    }
  }

  public static ConfigLoader getInstance() {
    return Holder.INSTANCE;
  }

  public static String getProperty(String pn) {
    return getInstance().properties.getProperty(pn);
  }

  public static void main(String[] args) {
    System.out.println(String.format("Try config file"));
    for (String name : ConfigLoader.getInstance().properties.stringPropertyNames()) {
      System.out.println(String.format("%s:%s", name, getProperty(name)));
    }
  }
}
