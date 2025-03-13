package optimize.util;

public class JavaGrammarValidator {

  public static void main(String[] args) {
    Object[] arr = new Object[10];
    arr[0] = 1;
    arr[2] = 123.3f;
    arr[3] = 12112.33d;
    double e = (double) arr[3];
    System.out.println(arr[0]);
  }
}
