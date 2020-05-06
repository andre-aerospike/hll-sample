
public class Main {
	public static void main(String[] args) {
		try
		{
			SimpleCountingHLL ex1 = new SimpleCountingHLL();
			ex1.Run();
		}
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
	}
}
