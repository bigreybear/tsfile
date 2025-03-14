package optimize;

public enum AliasedArgs {
  MTree,
  ART,
  OLD_CDM,
  NEW_CDM,
  G_SPC_H,
  G_TIM_H,
  G_SPC_S,
  G_TIM_S,
  G_MIX_S,
  G_MIX_H;

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
      default:
        throw new UnsupportedOperationException();
    }
  }
}
