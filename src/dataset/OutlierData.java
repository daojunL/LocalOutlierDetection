package finalProject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class OutlierData1 {
public static void main(String[] args) {
		
		try {
			FileOutputStream out = new FileOutputStream("Data1.txt" );
			Random rand = new Random();
			
			int numOfPoints = 100;
		    int x = rand.nextInt(100);
		    int y = rand.nextInt(100);
		    double x1;
		    double y1;
		    int numofOutlierPoints = 100;
		    int outlierX;
		    int outlierY;
		    double xo;
		    double yo;
		    
		    // generate gaussian clusters 
			for(int i = 0; i < 10; i++){
				for(int j = 0; j < 10; j++) {
					for(int k = 0; k < numOfPoints; k++) {
						x1 = x + rand.nextGaussian(); 
						y1 = y + rand.nextGaussian();
						out.write((x1+","+y1).getBytes());
					    out.write("\r\n".getBytes()); 
				    }
					y = y + 100;

				}	
				x = x + 100;
				y = rand.nextInt(100);
			}
			
			// Generate outlier points. 
			for(int i = 0; i < numofOutlierPoints; i++){
				outlierX = rand.nextInt(1000);
				outlierY = rand.nextInt(1000);
				xo = outlierX * rand.nextDouble();
				yo = outlierY * rand.nextDouble();
				out.write((xo+","+yo).getBytes());
				out.write("\r\n".getBytes());
			}
		    out.close();
		        
		}catch(IOException e) {
			System.out.print("Exception");
		}
	}
}
