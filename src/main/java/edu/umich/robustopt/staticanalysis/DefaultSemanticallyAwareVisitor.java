package edu.umich.robustopt.staticanalysis;

import java.util.Map;
import java.util.Stack;

import com.relationalcloud.tsqlparser.loader.Schema;
import com.relationalcloud.tsqlparser.visitors.recursive.ASTContext;
import com.relationalcloud.tsqlparser.visitors.recursive.DefaultRecursiveVisitor;

public abstract class DefaultSemanticallyAwareVisitor extends
		DefaultRecursiveVisitor {
	
	protected final Stack<ASTContext> astContextStack = new Stack<ASTContext>();
	protected final Stack<SelectContext> selectContextStack = new Stack<SelectContext>();
	protected final Map<String, Schema> schemas;
	
	protected DefaultSemanticallyAwareVisitor(Map<String, Schema> schemas) {
		this.schemas = schemas;
	}
	
	protected ASTContext currentASTContext() {
		return astContextStack.peek();
	}

	protected SelectContext currentSelectContext() {
		return selectContextStack.peek();
	}
	
	@Override
	public void pushASTContext(ASTContext c) {
		astContextStack.push(c);
	}
	
	@Override
	public void popASTContext() {
		astContextStack.pop();
	}
}
