package edu.umich.robustopt.vertica;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.umich.robustopt.dblogin.QueryPlanParser;

public class VerticaQueryPlanParser extends QueryPlanParser {
	private Pattern projectionNamePattern = Pattern.compile("Projection: robustopt\\.([^\n]*)");
	private Pattern projectionBaseNamePattern = Pattern.compile("Projection: robustopt\\.(.*)_node");

	/*
Access Path:
+-SELECT  LIMIT 10 [Cost: 227K, Rows: 10] (PATH ID: 0)
|  Output Only: 10 tuples
| +---> GROUPBY HASH [Cost: 227K, Rows: 7] (PATH ID: 1)
| |      Aggregates: min(lineitem.L_EXTENDEDPRICE)
| |      Group By: lineitem.L_SHIPMODE, lineitem.L_SHIPINSTRUCT
| |      Output Only: 10 tuples
| | +---> STORAGE ACCESS for lineitem [Cost: 141K, Rows: 60M] (PATH ID: 2)
| | |      Projection: robustopt.proj_2500001_node0001
| | |      Materialize: lineitem.L_SHIPINSTRUCT, lineitem.L_SHIPMODE, lineitem.L_EXTENDEDPRICE
| | |      Filter: (lineitem.L_RETURNFLAG <= 'R')

	 */
	public List<String> searchForPhysicalStructureNamesInCanonicalExplainOutput(String text) {
		String textString = "Access Path:\n"+
"+-SELECT  LIMIT 10 [Cost: 227K, Rows: 10] (PATH ID: 0)\n"+
"|  Output Only: 10 tuples\n"+
"| +---> GROUPBY HASH [Cost: 227K, Rows: 7] (PATH ID: 1)\n"+
"| |      Aggregates: min(lineitem.L_EXTENDEDPRICE)\n"+
"| |      Group By: lineitem.L_SHIPMODE, lineitem.L_SHIPINSTRUCT\n"+
"| |      Output Only: 10 tuples\n"+
"| | +---> STORAGE ACCESS for lineitem [Cost: 141K, Rows: 60M] (PATH ID: 2)\n"+
"| | |      Projection: robustopt.proj_2500001_node0001\n"+
"| | |      Projection: robustopt.proj_2500001_node0002\n"+
"| | |      Materialize: lineitem.L_SHIPINSTRUCT, lineitem.L_SHIPMODE, lineitem.L_EXTENDEDPRICE\n"+
"| | |      Filter: (lineitem.L_RETURNFLAG <= 'R')\n"+
"| | |      Projection: robustopt.proj_2500001_node0003";
		
		
		Matcher m = projectionNamePattern.matcher(text);
		List<String> projNames = new ArrayList<String>();
		while (m.find())
			projNames.add(m.group(1));
		return projNames;
	}
	
	public List<String> searchForPhysicalStructureBaseNamesInCanonicalExplainOutput(String canonicalExplainOutput) {				
		Matcher m = projectionBaseNamePattern.matcher(canonicalExplainOutput);
		List<String> projNames = new ArrayList<String>();
		while (m.find())
			projNames.add(m.group(1));
		return projNames;
	}

	public List<String> searchForPhysicalStructureNamesInRawExplainOutput(String rawExplainOutput) {
		String canonicalQueryPlan = extractCanonicalQueryPlan(rawExplainOutput);
		List<String> projectionNamesUsedInThePlan = searchForPhysicalStructureNamesInCanonicalExplainOutput(canonicalQueryPlan);
		return projectionNamesUsedInThePlan;
	}

	public List<String> searchForPhysicalStructureBaseNamesInRawExplainOutput(String rawExplainOutput) {
		String canonicalQueryPlan = extractCanonicalQueryPlan(rawExplainOutput);
		List<String> projectionBaseNamesUsedInThePlan = searchForPhysicalStructureBaseNamesInCanonicalExplainOutput(canonicalQueryPlan);
		return projectionBaseNamesUsedInThePlan;
	}

	
/*
------------------------------ 
QUERY PLAN DESCRIPTION: 
------------------------------

Optimizer Directives
----------------------
AvoidUsingProjections=robustopt.proj_1250001_node0001,robustopt.proj_1500001_node0001


explain select min(col99) from wide100

Access Path:
+-GROUPBY NOTHING [Cost: 258K, Rows: 1] (PATH ID: 1)
|  Aggregates: min(wide100.col99)
| +---> STORAGE ACCESS for wide100 [Cost: 250K, Rows: 41M] (PATH ID: 2)
| |      Projection: public.WIDE100_super
| |      Materialize: wide100.col99


------------------------------ 
----------------------------------------------- 
PLAN: BASE QUERY PLAN (GraphViz Format)
----------------------------------------------- 
digraph G {
graph [rankdir=BT, label = "BASE QUERY PLAN\nQuery: explain select min(col99) from wide100\n\nAll Nodes Vector: \n\n  node[0]=v_wide_node0001 (initiator) Up\n", labelloc=t, labeljust=l ordering=out]
0[label = "Root \nOutBlk=[UncTuple]", color = "green", shape = "house"];
1[label = "NewEENode \nOutBlk=[UncTuple]", color = "green", shape = "box"];
2[label = "GroupByPipe: 0 keys\nAggs:\n  min(wide100.col99)\nUnc: Integer(8)", color = "green", shape = "box"];
3[label = "StorageUnionStep: WIDE100_super\nUnc: Integer(8)", color = "purple", shape = "box"];
4[label = "GroupByPipe: 0 keys\nAggs:\n  min(wide100.col99)\nUnc: Integer(8)", color = "brown", shape = "box"];
5[label = "ScanStep: WIDE100_super\ncol1 (not emitted)\ncol99\nUnc: Integer(8)", color = "brown", shape = "box"];
1->0 [label = "V[0]",color = "black"];
2->1 [label = "0",color = "blue"];
3->2 [label = "0",color = "blue"];
4->3 [label = "0",color = "blue"];
5->4 [label = "0",color = "blue"];
}

 */

	
	
	public String extractCanonicalQueryPlan(String queryPlan) {
		String noGraphViz = queryPlan.substring(0, queryPlan.indexOf("PLAN: BASE QUERY PLAN (GraphViz Format)"));
		String noCostRows = noGraphViz.replaceAll("\\[[^\\]]*\\]", "[?]");
		String noPathId = noCostRows.replaceAll("\\(PATH ID:[^\\)]*\\)", "(?)");
		String noDirectives = noPathId.replaceAll("Optimizer Directives[\\s]*\n", "");
		noDirectives = noDirectives.replaceAll("AvoidUsingProjections[^\\n]*\n", "");
		String noEmptyLine = noDirectives.replaceAll("[\\s]*\n", "\n");
		String noDash = noEmptyLine.replaceAll("[\\s]*--*\n", "");
		return noDash;
	}
	

	public long extractTotalCostsFromRawPlan(String queryPlan) {
		String noGraphViz = queryPlan.substring(0, queryPlan.indexOf("PLAN: BASE QUERY PLAN (GraphViz Format)"));
		//TODO:
		//Now use noGraphViz and use regular expressions to extract all the cost estimates, add them up. Also, make sure you test your function with several queries manually to make sure it works correctly!
		long theSumOfAllCosts = 0;
		
		String regex="Cost: (\\w+),";
		Pattern pattern=Pattern.compile(regex);
		Matcher matcher=pattern.matcher(noGraphViz);
	    List<Long> costs = new ArrayList<Long>();
		 
		while(matcher.find()){
			long intCost;
	        String cost = matcher.group(1);
	        if (cost.matches("\\d+\\D")) {
	        	cost = cost.split("\\D", 2)[0];
	        	intCost = Integer.parseInt(cost) * 1000;
	        } else {
	        	intCost = Integer.parseInt(cost);
	        }
	        costs.add(intCost);
		}
		for (long i : costs) {
			theSumOfAllCosts += i;
		}
		return theSumOfAllCosts;
	}
	
	
	public static void main(String[] args) {
		String textString = "Access Path:\n"+
"+-SELECT  LIMIT 10 [Cost: 227K, Rows: 10] (PATH ID: 0)\n"+
"|  Output Only: 10 tuples\n"+
"| +---> GROUPBY HASH [Cost: 227K, Rows: 7] (PATH ID: 1)\n"+
"| |      Aggregates: min(lineitem.L_EXTENDEDPRICE)\n"+
"| |      Group By: lineitem.L_SHIPMODE, lineitem.L_SHIPINSTRUCT\n"+
"| |      Output Only: 10 tuples\n"+
"| | +---> STORAGE ACCESS for lineitem [Cost: 141K, Rows: 60M] (PATH ID: 2)\n"+
"| | |      Projection: robustopt.proj_2500001_node0001_node4\n"+
"| | |      Projection: robustopt.proj_2500002_node0002\n"+
"| | |      Materialize: lineitem.L_SHIPINSTRUCT, lineitem.L_SHIPMODE, lineitem.L_EXTENDEDPRICE\n"+
"| | |      Filter: (lineitem.L_RETURNFLAG <= 'R')\n"+
"| | |      Projection: robustopt.proj_2500003_node0003";
		VerticaQueryPlanParser vQueryPlanParser =  new VerticaQueryPlanParser();
		List<String> baseNames = vQueryPlanParser.searchForPhysicalStructureBaseNamesInCanonicalExplainOutput(textString);
		List<String> names = vQueryPlanParser.searchForPhysicalStructureNamesInCanonicalExplainOutput(textString);
		for (String s : baseNames)
			System.out.println("basename = " + s);
		for (String s : names)
			System.out.println("names = " + s);

		String queryPlan = " ------------------------------\n" +				
"QUERY PLAN DESCRIPTION: \n"+
"------------------------------\n"+
"\n"+
"Optimizer Directives\n"+
"----------------------\n"+
"AvoidUsingProjections=robustopt.proj_1250001_node0001,robustopt.proj_1500001_node0001\n"+
"\n"+
"\n"+
"explain select min(col99) from wide100\n"+
"\n"+
"Access Path:		\n"+
" +-STORAGE ACCESS for wide100 [Cost: 52, Rows: 41M (10K RLE)] (PATH ID: 1)\n"+
" +-STORAGE ACCESS for wide100 [Cost: 52, Rows: 41M (10K RLE)] (PATH ID: 2) \n"+
" +-STORAGE ACCESS for wide100 [Cost: 52, Rows: 41M (10K RLE)] (PATH ID: 3)  \n"+
" |  Projection: robustopt.proj_1750029_node0001\n"+
" |  Materialize: wide100.col10\n"+
"\n" +
"\n" +
" ------------------------------ \n"+
" ----------------------------------------------- \n"+
" PLAN: BASE QUERY PLAN (GraphViz Format)\n"+
" ----------------------------------------------- \n"+
" digraph G {\n"+
" graph [rankdir=BT, label = \"BASE QUERY PLAN\nQuery: explain select col10 from wide100;\n\nAll Nodes Vector: \n\n  node[0]=v_wide_node0001 (initiator) Up, labelloc=t, labeljust=l ordering=out]\n"+
" 0[label = \"Root \nOutBlk=[UncTuple]\", color = \"green\", shape = \"house\"];\n"+
" 1[label = \"NewEENode \nOutBlk=[RLETuple]\", color = \"green\", shape = \"box\"];\n"+
" 2[label = \"StorageUnionStep: proj_1750029_node0001\nRLE: Integer(8)\", color = \"purple\", shape = \"box\"];\n"+
" 3[label = \"ScanStep: proj_1750029_node0001\ncol10 RLE\nRLE: Integer(8)\", color = \"brown\", shape = \"box\"];\n"+
" 1->0 [label = \"V[0]\",color = \"black\"];\n"+
" 2->1 [label = \"0\",color = \"blue\"];\n"+
" 3->2 [label = \"0\",color = \"blue\"];\n"+
" }\n";
		System.out.println(vQueryPlanParser.extractTotalCostsFromRawPlan(queryPlan));
		String canonicalPlan = vQueryPlanParser.extractCanonicalQueryPlan(queryPlan);
		System.out.println(canonicalPlan);
		
	}

	
}
