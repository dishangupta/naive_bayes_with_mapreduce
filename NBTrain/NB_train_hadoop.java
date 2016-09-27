import java.util.Vector;





public class NB_train_hadoop {

	

	
	public Vector<String> getDocTokens (String doc) {
		
		return Utils.tokenizeDoc(doc);
	}

	public Vector<String> getDocLabels (String firstToken) {
		Vector<String> labels = new Vector<String>();		
		
		if (firstToken.contains("CCAT"))
			labels.add("CCAT");
		if (firstToken.contains("ECAT"))
			labels.add("ECAT");
		if (firstToken.contains("GCAT"))
			labels.add("GCAT");
		if (firstToken.contains("MCAT"))
			labels.add("MCAT");
		
		return labels;	
	
	}

	

}
