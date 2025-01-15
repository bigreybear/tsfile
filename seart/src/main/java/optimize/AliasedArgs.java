package optimize;

public enum AliasedArgs {
  MTree,
  ART,
  OLD_CDM,
  NEW_CDM,
  BLANK;

  public String getBasicArg() {
    switch (this) {
      case ART:
        return " -mt fdm -merge -ms full -oneTree";
      case MTree:
        return " -mt hash";
      case NEW_CDM:
        return " -mt ncdm -merge -ms full -oneTree";
      case OLD_CDM:
        return " -mt cdm -merge -ms full -oneTree";
      case BLANK:
        return "";
      default:
        throw new UnsupportedOperationException();
    }
  }
}
