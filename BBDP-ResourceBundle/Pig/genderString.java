package maestros;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class genderString extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {

        try{
            String str = (String)input.get(0);
			if( str.equals("F") ) {
				return "Female";
			}
			else {
				return "Male";
			}
        }catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }
  }
  
  /*Commands to compile
  mkdir maestros
  javac -cp /usr/lib/pig/pig.jar:`hadoop classpath` genderString.java
  cp genderString.class maestros
  jar -cf maestros.jar maestros
  */