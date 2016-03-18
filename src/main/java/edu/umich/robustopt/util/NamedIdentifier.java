package edu.umich.robustopt.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import edu.umich.robustopt.vertica.VerticaProjectionStructure;

public class NamedIdentifier extends Pair<String, String> implements Serializable {
	private static final long serialVersionUID = 6743141106266099811L;

	public NamedIdentifier(String first, String second) {
		super(first, second);
	}
	
	public String getQualifiedName() {
		return first + "." + second;
	}
	
	@Override
	public String toString() {
		return getQualifiedName();
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException {
		NamedIdentifier ni = new NamedIdentifier("barzan", "mozafari");
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File("/tmp/test")));
		oos.writeObject(ni);
		oos.close();
		
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(new File("/tmp/test")));
		NamedIdentifier ret = (NamedIdentifier) in.readObject();
		in.close();

		System.out.println(ret);
		

	}
}
