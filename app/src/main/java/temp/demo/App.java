package temp.demo;

public class App {
  public static void main(String args[]) {
    SecondMsg secondMsg = SecondMsg.newBuilder().setBlah(7).build();
    System.out.println(secondMsg);
  }
}
