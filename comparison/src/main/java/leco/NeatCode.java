package leco;

public class NeatCode {

  public boolean solution(int[] num) {
    int gas = num[0];
    int idx = 0;
    if (num.length < 2) return true;
    while (gas > 0) {
      gas--;
      idx++;
      if (idx + num[idx] > gas) {
        gas = num[idx];
      }
      if (gas + idx >= num.length - 1) {
        return true;
      }
    }

    return false;
  }


  public static void main(String[] args) {
    NeatCode nc = new NeatCode();
    boolean b = nc.solution(new int[]{1,1,2,2,0,1,1});
    System.out.println(b);
  }
}
