package maestros;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class binning extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {

        try{
            int value = (Integer)input.get(0);
			if ( value <= 500) {
				return "0 - 500";
			}
			else if ( value <= 1000) {
				return "501 - 1000";
			}
			else {
				return "1000+";
			}
        }catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }
  }
  
  /*Commands to compile
  mkdir maestros
  javac -cp /usr/lib/pig/pig.jar:`hadoop classpath` binning.java
  cp binning.class maestros
  jar -cf maestros.jar maestros
  */