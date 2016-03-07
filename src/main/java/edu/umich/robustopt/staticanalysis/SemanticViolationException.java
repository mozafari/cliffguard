package edu.umich.robustopt.staticanalysis;

public class SemanticViolationException extends RuntimeException {

	public SemanticViolationException(String msg) {
		super(msg);
	}
	
	public SemanticViolationException() {
		super();
	}
}
