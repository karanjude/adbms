package applicationCache;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Date;

public class StartCOSAR extends Thread {
	private String cmd = "";
	private String filename = "";

	public StartCOSAR (String inputcmd)
	{
		cmd = inputcmd;
	}

	public StartCOSAR (String inputcmd, String outputfile)
	{
		cmd = inputcmd;
		filename = outputfile;
	}


	public void run()
	{
		String line;
		try {
			Process p = Runtime.getRuntime().exec(cmd);
			BufferedReader input = new BufferedReader (new InputStreamReader(p.getInputStream()));

			FileWriter fout = null;
			if( !filename.equals("") )
			{
				fout = new FileWriter(filename, true);
				fout.write("\r\n\r\n" + new Date().toString() + "\r\n");
			}


			while ( (line = input.readLine()) != null) {
				System.out.println(line);
				//				if( fout != null )
				//				{
				//					fout.write(line + "\r\n");
				//				}
			}
			input.close();

			if( fout != null )
			{
				fout.flush();
				fout.close();
			}

			//p.waitFor(); //Causes this thread to wait until COSAR terminates
		}
		catch (Exception err) {
			err.printStackTrace(System.out);
		}
	}
}

