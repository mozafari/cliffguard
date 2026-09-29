package edu.umich.robustopt.vertica;

import edu.umich.robustopt.dbd.DesignParameters;

public class VerticaDesignParameters extends DesignParameters {

	public final VerticaDesignMode designMode;
	
	public VerticaDesignParameters (VerticaDesignMode designMode) {
		this.designMode = designMode;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public String toString() {
		return "" + designMode;
	}
	
}
