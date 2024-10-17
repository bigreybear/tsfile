package seart.exception;

import java.nio.charset.StandardCharsets;

public class PrefixPropertyException extends UnsupportedOperationException{
  public PrefixPropertyException(byte[] key){
    super("PrefixProperty is violated for:" + new String(key, StandardCharsets.UTF_8));
  }
}
