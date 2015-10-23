package edu.umich.robustopt.microsoft;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.umich.robustopt.clustering.WeightedQuery;
import edu.umich.robustopt.common.BLog.LogLevel;
import edu.umich.robustopt.dbd.*;
import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.microsoft.MicrosoftDatabaseLoginConfiguration;
import edu.umich.robustopt.metering.ExperimentCache;
import edu.umich.robustopt.physicalstructures.PhysicalDesign;
import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.util.Timer;
import edu.umich.robustopt.vertica.VerticaDesignParameters;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TWhereClause;
import gudusoft.gsqlparser.nodes.mssql.TMssqlThrowSqlNode;
import gudusoft.gsqlparser.stmt.TCreateViewSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

public class MicrosoftDesigner extends DBDesigner {

	private static final String inputPath = "C:\\robust_opt_input"; // fixed for now
	private static final String outputPath = "C:\\robust_opt_output"; // fixed for now

	private static final String remoteInputPath = "C$\\robust_opt_input"; // fixed for now
	private static final String remoteOutputPath = "C$\\robust_opt_output"; // fixed for now

	private String recommendationOutputPath;
	private String recommendationFilename;
	private String psExecPath = "C:\\PSTools\\PsExec.exe";
	private String pathToStatisticsScript;
	private String statisticsFilename;

	private MicrosoftDatabaseLoginConfiguration testLogin;

	transient private long secondsSpentDesigning = 0;
	transient private long numberOfActualDesigns = 0;

	public MicrosoftDesigner(LogLevel verbosity,
			DatabaseLoginConfiguration databaseLogin, String pathToStatisticsScript,
			ExperimentCache experimentCache) throws Exception {
		super(verbosity, databaseLogin, experimentCache);

		this.testLogin = (MicrosoftDatabaseLoginConfiguration)databaseLogin;
		this.pathToStatisticsScript = pathToStatisticsScript;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("MicrosoftDesigner Main");

		MicrosoftDatabaseLoginConfiguration testLogin = new MicrosoftDatabaseLoginConfiguration(false, "alias", "10.175.210.72",
				1433, "AdventureWorksEmpty", "sa", "asdf1234!", "Administrator", "Cliffguard1", "EMPTY-DB");

		try {
			MicrosoftDesigner designer = new MicrosoftDesigner(LogLevel.DEBUG, testLogin, null, null);
			MicrosoftDesignParameters parameters = new MicrosoftDesignParameters(MicrosoftDesignAddMode.IDX_IV, 
					MicrosoftDesignKeepMode.NONE, MicrosoftDesignOnlineOption.OFF);

			// sample queries
			List<WeightedQuery> queries = new ArrayList<WeightedQuery>();
			String sql1 = "SELECT Name, ProductNumber, ListPrice AS Price FROM Production.Product WHERE ProductLine = 'R' AND DaysToManufacture < 4 ORDER BY Name ASC;";
			String sql2 = "SELECT ProductModelID, AVG(ListPrice) AS 'Average List Price' FROM Production.Product WHERE ListPrice > $1000 GROUP BY ProductModelID ORDER BY ProductModelID;";
			String sql3 = "SELECT SalesQuota, SUM(SalesYTD) 'TotalSalesYTD', GROUPING(SalesQuota) AS 'Grouping' FROM Sales.SalesPerson GROUP BY SalesQuota WITH ROLLUP;";
			String sql4 = "SELECT CustomerID, OrderDate, SubTotal, TotalDue FROM Sales.SalesOrderHeader WHERE SalesPersonID = 35 ORDER BY OrderDate";
			double weight1 = 1.5;
			double weight2 = 2.0;
			
			WeightedQuery query1 = new WeightedQuery(sql1, weight1);
			WeightedQuery query2 = new WeightedQuery(sql2, weight2);
			WeightedQuery query3 = new WeightedQuery(sql3, weight1);
			WeightedQuery query4 = new WeightedQuery(sql4, weight2);

			queries.add(query1);
			queries.add(query2);
			queries.add(query3);
			queries.add(query4);

			System.out.println("Start finding physical design..");
			MicrosoftPhysicalDesign design = (MicrosoftPhysicalDesign)designer.findDesignByWeightedQueries(queries, parameters);

			// print all name of physical designs (index & indexed views)
			List<PhysicalStructure> structureList = design.getPhysicalStructuresAsList();
			for (PhysicalStructure p : structureList) {
				if (p instanceof MicrosoftIndex) {
					MicrosoftIndex i = (MicrosoftIndex)p;
					System.out.println("Index: " + i.getIndexName());
				} else if (p instanceof MicrosoftIndexedView) {
					MicrosoftIndexedView v = (MicrosoftIndexedView)p;
					System.out.println("Indexed View: " + v.getIndexName());
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void SetPsExecPath(String path) {
		this.psExecPath = path;
	}

	@Override
	public PhysicalDesign findDesignByWeightedQueries(
			List<WeightedQuery> weightedQueries,
			DesignParameters designParameters) throws Exception {

		MicrosoftDesignParameters microsoftDesignParameters;
		if (designParameters==null) {
			log.error("Invalid DesignParameters: null, using default parameter");
			microsoftDesignParameters = new MicrosoftDesignParameters(MicrosoftDesignAddMode.IDX_IV, 
					MicrosoftDesignKeepMode.NONE, MicrosoftDesignOnlineOption.OFF);
		}
		else if (!(designParameters instanceof MicrosoftDesignParameters)) {
			log.error("Invalid DesignParameters: expected MicrosoftDesignParameters but received " + 
					designParameters.getClass().getCanonicalName() + ", " + designParameters + ", using default parameter");
			microsoftDesignParameters = new MicrosoftDesignParameters(MicrosoftDesignAddMode.IDX_IV, 
					MicrosoftDesignKeepMode.NONE, MicrosoftDesignOnlineOption.OFF);
		} else {
			microsoftDesignParameters = (MicrosoftDesignParameters) designParameters;
		}
		
		return findDesign(weightedQueries, microsoftDesignParameters);
	}

	@Override
	public String reportStatistics() {
		return String.format("Number of Actual Designs = %d, Minutes spent designing = %d", numberOfActualDesigns, secondsSpentDesigning / 60);
	}

	private PhysicalDesign findDesign(List<WeightedQuery> weightedQueries, 
			MicrosoftDesignParameters designParameters) throws Exception {

		Set<WeightedQuery> uniqueWeightedQueries = WeightedQuery.consolidateWeightedQueies(weightedQueries);
		weightedQueries = new ArrayList<WeightedQuery>(uniqueWeightedQueries); 

		if (experimentCache!=null && experimentCache.getDesignByWeight(weightedQueries)!=null){
			log.status(LogLevel.DEBUG, "design loaded from cache instead of DBD");
			return experimentCache.getDesignByWeight(weightedQueries);
		}
		Timer t = new Timer();
		int startLogIndex = log.getNextIndex();
		
		createInputOutputDirectories();
		String inputXMLFilename = generateInputXML(weightedQueries);

        if (inputXMLFilename == null) {
        	return null;
        }

        // copy generated input XML file to test server with an empty DB.
        copyInputXMLToRemoteServer(inputXMLFilename);

        // copy DB statistics script to the empty DB and apply.
        // DY: Currently, Empty DB should be set up manually.
//        if (pathToStatisticsScript != null) {
//        	if (copyStatistics(pathToStatisticsScript) && statisticsFilename != null) {
//        		importStatistics();
//        	}
//        }

        // drop all indexes & views.
		log.status(LogLevel.DEBUG, "Cleaning all view & indexes before DTA...");
		resetTest(testLogin);
		resetTestEmpty(testLogin);

        // run DTA remotely on test (emptyDB).
		log.status(LogLevel.DEBUG, "Running DTA remotely...");
        runExternalProcess(getRemoteDTAArguments(inputXMLFilename, designParameters));

        // copy recommendation from remote to local.
        copyRecommendationFromRemote();

        // apply recommended physical design to test server.
//        applyPhysicalDesignRemotely(testLogin, recommendationOutputPath);
        // apply recommended physical design to local empty server.
        applyPhysicalDesign(testLogin, recommendationOutputPath);

        // retrieve 'applied' physical design from test server.
        PhysicalDesign physicalDesign = retrievePhysicalDesignFromDatabase(testLogin, recommendationOutputPath);

        // drop all indexes & views.
		log.status(LogLevel.DEBUG, "Cleaning all view & indexes after DTA...");
		resetTest(testLogin);
		resetTestEmpty(testLogin);
		String algorithmOutput = log.getMessagesFromIndex(startLogIndex);
		secondsSpentDesigning += t.lapSeconds();
		++numberOfActualDesigns;

        if (experimentCache != null) {
        	experimentCache.cacheDesignByWeightedQueries(weightedQueries, physicalDesign, secondsSpentDesigning, algorithmOutput);
        }
        
		return physicalDesign;
	}

	private boolean createInputOutputDirectories() throws Exception
	{
		// create local input/output directories
		File inputDir = new File(inputPath);
		File outputDir = new File(outputPath);

		if (!inputDir.exists() && !inputDir.mkdirs()) {
		}
		if (!outputDir.exists() && !outputDir.mkdirs()) {
		}

		// create remote input/output directories
		List<String> arguments = new ArrayList<String>();
		
		arguments.add(psExecPath);
		arguments.add("-accepteula");

		arguments.add("\\\\" + testLogin.getDBhost());
		arguments.add("-u");
		arguments.add(testLogin.getWindowsUsername());
		arguments.add("-p");
		arguments.add(testLogin.getWindowsPassword());
		arguments.add("-s");
		arguments.add("cmd");
		arguments.add("/c");
		arguments.add("mkdir");
		arguments.add(inputPath);
		runExternalProcess(arguments);

		arguments.clear();

		arguments.add(psExecPath);
		arguments.add("-accepteula");
		arguments.add("-i");
		arguments.add("\\\\" + testLogin.getDBhost());
		arguments.add("-u");
		arguments.add(testLogin.getWindowsUsername());
		arguments.add("-p");
		arguments.add(testLogin.getWindowsPassword());
		arguments.add("-s");
		arguments.add("cmd");
		arguments.add("/c");
		arguments.add("mkdir");
		arguments.add(outputPath);
		runExternalProcess(arguments);
		
		return true;
	}

	private String generateInputXML(List<WeightedQuery> weightedQueries) throws Exception {

		File tempDir = new File(inputPath);
		
		if (!tempDir.exists() && !tempDir.mkdirs()) {
                throw new Exception("Failed to create an input XML file for Microsoft Database Tuning Advisor: failed to create temp directory");
		}

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
		String timestamp = dateFormat.format(new Date());
		String filename = String.format("dta_workload_%s.xml", timestamp);

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		
		Document doc = db.newDocument();
		Element DTAXML = doc.createElement("DTAXML");
		doc.appendChild(DTAXML);

		Element DTAInput = doc.createElement("DTAInput");
		DTAXML.appendChild(DTAInput);

		Element Server = doc.createElement("Server");
		DTAInput.appendChild(Server);

		Element serverName = doc.createElement("Name");
		serverName.appendChild(doc.createTextNode("localhost"));
		Server.appendChild(serverName);

		Element Database = doc.createElement("Database");
		Server.appendChild(Database);

		Element databaseName = doc.createElement("Name");
		databaseName.appendChild(doc.createTextNode(testLogin.getDBname()));
		Database.appendChild(databaseName);

		Element Workload = doc.createElement("Workload");
		DTAInput.appendChild(Workload);

		for (WeightedQuery q : weightedQueries) {
			String query = q.query;
			double weight = q.weight;

			Element EventString = doc.createElement("EventString");
			EventString.setAttribute("Weight", Double.toString(weight));
			EventString.appendChild(doc.createTextNode(query));

			Workload.appendChild(EventString);
		}

		// write the content into xml file
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer t = tf.newTransformer();

		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(inputPath + File.separator + filename));

		t.setOutputProperty(OutputKeys.INDENT, "yes");
		t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

		t.transform(source, result);

		log.status(LogLevel.DEBUG, "An input workload XML file has been successfully created.");

		return filename;
	}

	private boolean copyInputXMLToRemoteServer(String xmlName) throws Exception {

		List<String> arguments = new ArrayList<String>();

		arguments.add("robocopy"); // robocopy command for copying
		arguments.add(inputPath); // local source.
		arguments.add(String.format("\\\\%s", testLogin.getDBhost() + File.separator + remoteInputPath)); // remote destination.
		arguments.add(xmlName);

		runExternalProcess(arguments);

		return true;
	}

	private boolean copyRecommendationFromRemote() throws Exception {
		
		List<String> arguments = new ArrayList<String>();

		arguments.add("robocopy"); // robocopy command for copying
		arguments.add(String.format("\\\\%s", testLogin.getDBhost() + File.separator + remoteOutputPath)); // remote destination.
		arguments.add(outputPath); // local destination.
		arguments.add(recommendationFilename);

		runExternalProcess(arguments);

		return true;
	}

	private int runExternalProcess(List<String> arguments) throws Exception {
		ProcessBuilder pb = new ProcessBuilder(arguments);
		pb.redirectErrorStream(true);
		Process p = pb.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line;
		while ((line = br.readLine()) != null)
		{
			log.status(LogLevel.VERBOSE, line);
		}
		return p.waitFor();
	}


	private List<String> getRemoteDTAArguments(String xmlName, MicrosoftDesignParameters parameters) {

		List<String> arguments = new ArrayList<String>();

		arguments.add(psExecPath);
		arguments.add("\\\\" + testLogin.getDBhost());
		arguments.add("-u");
		arguments.add(testLogin.getWindowsUsername());
		arguments.add("-p");
		arguments.add(testLogin.getWindowsPassword());
		arguments.add("-accepteula");
		arguments.add("-i");

		// dta command.
		arguments.add("dta");

		// server name.
		arguments.add("-S");
		arguments.add("localhost");

		// username
		arguments.add("-U");
		arguments.add(testLogin.getDBuser());
		
		// password
		arguments.add("-P");
		arguments.add(testLogin.getDBpasswd());

		// DB name
		arguments.add("-d");
		arguments.add(testLogin.getDBname());

		// use trusted connection
		arguments.add("-E");

		// input file
        arguments.add("-ix");
        arguments.add(inputPath + File.separator + xmlName);

        // max storage size required by physical structures
//        arguments.add("-B");
//        arguments.add("131072"); // in MB (i.e. 128G)

		// session name
		arguments.add("-s");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
		String strDate = dateFormat.format(new Date());
		String sessionName = String.format("dta_%s", strDate);
		arguments.add(sessionName);

		// recommendation output
		arguments.add("-of");
		recommendationFilename = String.format("rec_%s.sql", strDate);
		String recommendationName = outputPath + File.separator + recommendationFilename;
		recommendationOutputPath = recommendationName;
		arguments.add(recommendationName);

		// summary output
		arguments.add("-ox");
		String summaryName = String.format(outputPath + File.separator + "summary_%s.xml", strDate);
		arguments.add(summaryName);

		// report output
		arguments.add("-or");
		String reportName = String.format(outputPath + File.separator + "report_%s.xml", strDate);
		arguments.add(reportName);

		// PDS add option
		arguments.add("-fa");
		String PDSAddOption;
		switch(parameters.designAddMode) {
		case IDX_IV:
			PDSAddOption = "IDX_IV";
			break;
		case IDX:
			PDSAddOption = "IDX";
			break;
		case IV:
			PDSAddOption = "IV";
			break;
		case NCL_IDX:
			PDSAddOption = "NCL_IDX";
			break;
		default:
			PDSAddOption = "IDX_IV";
			break;
		}
		arguments.add(PDSAddOption);

		// PDS keep option
		arguments.add("-fk");
		String PDSKeepOption;
		switch(parameters.designKeepMode) {
		case ALL:
			PDSKeepOption = "ALL";
			break;
		case NONE:
			PDSKeepOption = "NONE";
			break;
		case CL_IDX:
			PDSKeepOption = "CL_IDX";
			break;
		case IDX:
			PDSKeepOption = "IDX";
			break;
		case ALIGNED:
			PDSKeepOption = "ALIGNED";
			break;
		default:
			PDSKeepOption = "NONE";
			break;
		}
		arguments.add(PDSKeepOption);
		return arguments;
	}

	private boolean applyPhysicalDesign(DatabaseLoginConfiguration databaseLogin, String physicalDesignPath) throws Exception {

		List<String> arguments = new ArrayList<String>();

		arguments.add(psExecPath);
		arguments.add("\\\\" + databaseLogin.getDBhost());
		arguments.add("-accepteula");
		arguments.add("-i");
		
		arguments.add("sqlcmd");

		// SQL Server name
		arguments.add("-S");
		arguments.add("localhost");

		// DB name
		arguments.add("-d");
		arguments.add(databaseLogin.getDBname());
		
		// username
		arguments.add("-U");
		arguments.add(databaseLogin.getDBuser());
		
		// password
		arguments.add("-P");
		arguments.add(databaseLogin.getDBpasswd());

		// input physical design script
		arguments.add("-i");
		arguments.add(physicalDesignPath);
		runExternalProcess(arguments);
		
		return true;
	}

	// drop all indexes and indexed views (in shell database)
	private boolean resetTestEmpty(DatabaseLoginConfiguration login) {

		Connection conn = null;
		Statement stmt = null;

		try
		{
			conn = DriverManager.getConnection(
					String.format("jdbc:sqlserver://%s:%s;databasename=%s", 
							login.getDBhost(), login.getDBport(), login.getDBname()), 
							login.getDBuser(), login.getDBpasswd());

			if (conn != null) {
				log.status(LogLevel.DEBUG, "JDBC connection successful");
			}

			// drop indexes first.
			String sql = "SELECT t.name as table_name, s.name as schema_name, ind.name as index_name " +
					"FROM sys.indexes ind " +
					"INNER JOIN sys.tables t ON ind.object_id = t.object_id " +
					"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
					"WHERE t.is_ms_shipped = 0 AND ind.type > 0 and ind.is_primary_key = 0 and ind.is_unique_constraint = 0";

			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);

			List<String> tableNames = new ArrayList<String>();
			List<String> schemaNames = new ArrayList<String>();
			List<String> indexNames = new ArrayList<String>();

			while (rs.next()) {
				tableNames.add(rs.getString("table_name"));
				schemaNames.add(rs.getString("schema_name"));
				indexNames.add(rs.getString("index_name"));
			}
			rs.close();

			for (int i = 0; i < indexNames.size(); ++i) {
				String tableName = tableNames.get(i);
				String schemaName = schemaNames.get(i);
				String indexName = indexNames.get(i);

				try {
					String dropSql = String.format("DROP INDEX %s ON %s.%s", indexName, schemaName, tableName);
					int rc = stmt.executeUpdate(dropSql);
					if (rc != 0) {
						log.error("Could not drop the index. SQL: " + dropSql);
					}
				} catch (SQLException e) {
					log.error("Caught an exception while reseting indexes/indexed views: " + e.getMessage());
				}
			}

			// drop indexed views.
			sql = "SELECT t.name as view_name, s.name as schema_name, ind.name as index_name " +
					"FROM sys.indexes ind " +
					"INNER JOIN sys.views t ON ind.object_id = t.object_id " +
					"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
					"WHERE t.is_ms_shipped = 0 AND ind.type = 1 and ind.is_primary_key = 0 and ind.is_unique_constraint = 0";
			rs = stmt.executeQuery(sql);

			tableNames.clear();
			schemaNames.clear();
			indexNames.clear();

			while (rs.next()) {
				tableNames.add(rs.getString("view_name"));
				schemaNames.add(rs.getString("schema_name"));
				indexNames.add(rs.getString("index_name"));
			}
			rs.close();

			for (int i = 0; i < indexNames.size(); ++i) {
				String tableName = tableNames.get(i);
				String schemaName = schemaNames.get(i);
				String indexName = indexNames.get(i);

				String dropSql = String.format("DROP INDEX %s ON %s.%s", indexName, schemaName, tableName);
				try {
					int rc = stmt.executeUpdate(dropSql);
					if (rc != 0) {
						log.error("Could not drop the index. SQL: " + dropSql);
					}
				} catch (SQLException e) {
					log.error("Caught an exception while reseting indexes/indexed views: " + e.getMessage());
				}
				// DY: dropping clustered index on view automatically drops view as well.
//				dropSql = String.format("DROP VIEW %s.%s", schemaName, tableName);
//				try {
//					int rc = stmt.executeUpdate(dropSql);
//					if (rc != 0) {
//						log.error("Could not drop the view. SQL: " + dropSql);
//					}
//				} catch (SQLException e) {
//					log.error("Caught an exception while reseting indexes/indexed views: " + e.getMessage());
//				}
			}
		} catch (SQLException e) {
			log.error("Caught an exception while reseting indexes/indexed views: " + e.getMessage());
		} 

		try {
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return true;
	}

	// drop all indexes and indexed views.
	private boolean resetTest(DatabaseLoginConfiguration login) {

		Connection conn = null;
		Statement stmt = null;

		try
		{
			conn = DriverManager.getConnection(
					String.format("jdbc:sqlserver://%s:%s;databasename=%s", 
							login.getDBhost(), login.getDBport(), login.getDBname()), 
							login.getDBuser(), login.getDBpasswd());

			if (conn != null) {
				log.status(LogLevel.DEBUG, "JDBC connection successful");
			}

			// drop indexes first.
			String sql = "SELECT t.name as table_name, s.name as schema_name, ind.name as index_name " +
					"FROM sys.indexes ind " +
					"INNER JOIN sys.tables t ON ind.object_id = t.object_id " +
					"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
					"WHERE t.is_ms_shipped = 0 AND ind.type > 0 and ind.is_primary_key = 0 and ind.is_unique_constraint = 0";

			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);

			List<String> tableNames = new ArrayList<String>();
			List<String> schemaNames = new ArrayList<String>();
			List<String> indexNames = new ArrayList<String>();

			while (rs.next()) {
				tableNames.add(rs.getString("table_name"));
				schemaNames.add(rs.getString("schema_name"));
				indexNames.add(rs.getString("index_name"));
			}
			rs.close();

			for (int i = 0; i < indexNames.size(); ++i) {
				String tableName = tableNames.get(i);
				String schemaName = schemaNames.get(i);
				String indexName = indexNames.get(i);

				try {
					String dropSql = String.format("DROP INDEX %s ON %s.%s", indexName, schemaName, tableName);
					int rc = stmt.executeUpdate(dropSql);
					if (rc != 0) {
						log.error("Could not drop the index. SQL: " + dropSql);
					}
				} catch (SQLException e) {
					log.error("Caught an exception while reseting indexes/indexed views: " + e.getMessage());
				}
			}

			// drop indexed views.
			sql = "SELECT t.name as view_name, s.name as schema_name, ind.name as index_name " +
					"FROM sys.indexes ind " +
					"INNER JOIN sys.views t ON ind.object_id = t.object_id " +
					"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
					"WHERE t.is_ms_shipped = 0 AND ind.type = 1 and ind.is_primary_key = 0 and ind.is_unique_constraint = 0";
			rs = stmt.executeQuery(sql);

			tableNames.clear();
			schemaNames.clear();
			indexNames.clear();

			while (rs.next()) {
				tableNames.add(rs.getString("view_name"));
				schemaNames.add(rs.getString("schema_name"));
				indexNames.add(rs.getString("index_name"));
			}
			rs.close();

			for (int i = 0; i < indexNames.size(); ++i) {
				String tableName = tableNames.get(i);
				String schemaName = schemaNames.get(i);
				String indexName = indexNames.get(i);

				String dropSql = String.format("DROP INDEX %s ON %s.%s", indexName, schemaName, tableName);
				try {
					int rc = stmt.executeUpdate(dropSql);
					if (rc != 0) {
						log.error("Could not drop the index. SQL: " + dropSql);
					}
				} catch (SQLException e) {
					log.error("Caught an exception while reseting indexes/indexed views: " + e.getMessage());
				}
				// DY: dropping clustered index on view automatically drops view as well.
//				dropSql = String.format("DROP VIEW %s.%s", schemaName, tableName);
//				try {
//					int rc = stmt.executeUpdate(dropSql);
//					if (rc != 0) {
//						log.error("Could not drop the view. SQL: " + dropSql);
//					}
//				} catch (SQLException e) {
//					log.error("Caught an exception while reseting indexes/indexed views: " + e.getMessage());
//				}
			}
		} catch (SQLException e) {
			log.error("Caught an exception while reseting indexes/indexed views: " + e.getMessage());
		}

		try {
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return true;
	}

	private MicrosoftPhysicalDesign retrievePhysicalDesignFromDatabase(
			DatabaseLoginConfiguration databaseLogin, String physicalDesignFilename) throws Exception {

		List<PhysicalStructure> physicalStructures = new ArrayList<PhysicalStructure>();

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        Connection conn = DriverManager.getConnection(
        		String.format("jdbc:sqlserver://%s:%s;databasename=%s", 
        				databaseLogin.getDBhost(), databaseLogin.getDBport(), databaseLogin.getDBname()), 
        				databaseLogin.getDBuser(), databaseLogin.getDBpasswd());

        if (conn != null) {
        	log.status(LogLevel.DEBUG, "JDBC connection successful");
        }
        Statement stmt = conn.createStatement();

        // only retrieve nonclustered indexes.
        String sql = "SELECT t.name as table_name, s.name as schema_name, ind.* " +
        		"FROM sys.indexes ind INNER JOIN sys.tables t ON ind.object_id = t.object_id " +
        		"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
        		"WHERE t.is_ms_shipped = 0 AND ind.type = 2 and ind.is_primary_key = 0 and ind.is_unique_constraint = 0 " +
        		"and ind.is_hypothetical = 0;";
        ResultSet rs = stmt.executeQuery(sql);

        while (rs.next()) {

        	String indexName = rs.getString("name");
        	String schemaName = rs.getString("schema_name");
        	String tableName = rs.getString("table_name");

        	int objectId = rs.getInt("object_id");
        	int indexId = rs.getInt("index_id");
        	int indexType = rs.getInt("type");

        	int isPrimaryKey = rs.getInt("is_primary_key");
        	int isDisabled = rs.getInt("is_disabled");
        	int isUnique = rs.getInt("is_unique");
        	int isUniqueConstraint = rs.getInt("is_unique_constraint");

        	List<MicrosoftColumn> keyColumnList = new ArrayList<MicrosoftColumn>();
        	List<MicrosoftColumn> includeColumnList = new ArrayList<MicrosoftColumn>();

        	String sqlGetKeyColumns = "SELECT c.name as column_name, ic.* FROM sys.index_columns ic "
        			+ "INNER JOIN sys.columns c ON ic.object_id = c.object_id and ic.column_id = c.column_id "
        			+ "WHERE ic.object_id = ? and ic.index_id = ? and ic.is_included_column = 0 " +
        			"ORDER BY key_ordinal";
        	PreparedStatement keyColumnStatement = conn.prepareStatement(sqlGetKeyColumns);
        	keyColumnStatement.setInt(1, objectId);
        	keyColumnStatement.setInt(2, indexId);
        	ResultSet keyColumns = keyColumnStatement.executeQuery();

        	while (keyColumns.next()) {
        		MicrosoftColumn newColumn = new MicrosoftColumn(keyColumns.getString("column_name"), 
        				keyColumns.getInt("column_id"), keyColumns.getInt("is_descending_key"), 
        				keyColumns.getInt("key_ordinal"), keyColumns.getInt("partition_ordinal"));
        		keyColumnList.add(newColumn);
        	}
        	keyColumns.close();

        	String sqlGetIncludeColumns = "SELECT c.name as column_name, ic.* FROM sys.index_columns ic "
        			+ "INNER JOIN sys.columns c ON ic.object_id = c.object_id and ic.column_id = c.column_id "
        			+ "WHERE ic.object_id = ? and ic.index_id = ? and ic.is_included_column = 1";
        	PreparedStatement includeColumnStatement = conn.prepareStatement(sqlGetIncludeColumns);
        	includeColumnStatement.setInt(1, objectId);
        	includeColumnStatement.setInt(2, indexId);
        	ResultSet includeColumns = includeColumnStatement.executeQuery();
        	while (includeColumns.next()) {
        		MicrosoftColumn newColumn = new MicrosoftColumn(includeColumns.getString("column_name"), 
        				includeColumns.getInt("column_id"), includeColumns.getInt("is_descending_key"), 
        				includeColumns.getInt("key_ordinal"), includeColumns.getInt("partition_ordinal"));
        		includeColumnList.add(newColumn);
        	}
        	includeColumns.close();

        	MicrosoftIndex newIndex = new MicrosoftIndex(objectId, indexName, schemaName, tableName, indexType, indexId, isUnique, 
        			isPrimaryKey, isUniqueConstraint, isDisabled, keyColumnList, includeColumnList);

        	physicalStructures.add(newIndex);
        }
        rs.close();

        stmt = conn.createStatement();
        String sqlGetIndexedViews = "SELECT o.object_id, o.name as view_name, i.*, s.name as schema_name " +
        		"FROM sys.objects o " +
        		"INNER JOIN sys.indexes i ON o.object_id = i.object_id " +
        		"INNER JOIN sys.views v ON v.object_id = i.object_id " +
        		"INNER JOIN sys.schemas s ON s.schema_id = v.schema_id " +
        		"WHERE o.type = 'V' and i.type = 1";
        rs = stmt.executeQuery(sqlGetIndexedViews);

        while (rs.next()) {

        	String viewName = rs.getString("view_name");
        	String indexName = rs.getString("name");
        	String schemaName = rs.getString("schema_name");

        	int objectId = rs.getInt("object_id");
        	int indexId = rs.getInt("index_id");
        	int indexType = rs.getInt("type");

        	int isPrimaryKey = rs.getInt("is_primary_key");
        	int isDisabled = rs.getInt("is_disabled");
        	int isUnique = rs.getInt("is_unique");
        	int isUniqueConstraint = rs.getInt("is_unique_constraint");

        	List<String> columns = new ArrayList<String>();
        	List<String> aliases = new ArrayList<String>();
        	List<Integer> indexedColumns = new ArrayList<Integer>();
        	List<Integer> isIndexedColumnsDescending = new ArrayList<Integer>();

        	String from = "";
        	String where = "";
        	String groupBy = "";

        	String sqlGetViewDefinition = "SELECT definition from sys.sql_modules WHERE object_id = ?";
        	PreparedStatement viewDefinitionStmt = conn.prepareStatement(sqlGetViewDefinition);
        	viewDefinitionStmt.setInt(1, objectId);

        	ResultSet viewDefinition = viewDefinitionStmt.executeQuery();
        	if (viewDefinition.next()) {

        		String definition = viewDefinition.getString("definition");
        		TGSqlParser parser = new TGSqlParser(EDbVendor.dbvmssql);
        		parser.setSqltext(definition);
        		int ret = parser.parse();

        		if (ret == 0) {
        			if (parser.sqlstatements.size() != 1) {
        				continue;
        			}
        			else {
        				TCustomSqlStatement customSqlStatement = parser.sqlstatements.get(0);

        				if (customSqlStatement instanceof TCreateViewSqlStatement) {

        					TCreateViewSqlStatement viewStatement = (TCreateViewSqlStatement)customSqlStatement;
        					TSelectSqlStatement selectSqlStatement = viewStatement.getSubquery();

        					// get select columns
        					for (int i = 0; i < selectSqlStatement.getResultColumnList().size(); ++i) {
        						TResultColumn column = selectSqlStatement.getResultColumnList().getResultColumn(i);

        						String columnExpr = column.getExpr().toString();
        						String alias = "";
        						if (column.getAliasClause() != null) {
        							alias = column.getAliasClause().toString();
        						}

        						columns.add(columnExpr);
        						aliases.add(alias);
        					}

        					// get from clause
        					from = selectSqlStatement.joins.toString();

        					// get where clause
        					TWhereClause whereClause = selectSqlStatement.getWhereClause();
        					if (whereClause != null) {
        						where = whereClause.toString();
        					}

        					// get group-by & having clause
        					TGroupBy groupByClause = selectSqlStatement.getGroupByClause();
        					if (groupByClause != null) {
        						groupBy = groupByClause.toString();
        					}

        					// get indexed columns
        					String sqlGetIndexedColumns = "SELECT c.name, i.is_descending_key FROM sys.index_columns i " +
        							"INNER JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = i.column_id " +
        							"WHERE i.object_id = ? AND i.index_id = ? ORDER BY key_ordinal";

        					PreparedStatement getIndexedColumnStatement = conn.prepareStatement(sqlGetIndexedColumns);
        					getIndexedColumnStatement.setInt(1, objectId);
        					getIndexedColumnStatement.setInt(2, indexId);

        					ResultSet indexedColumnSet = getIndexedColumnStatement.executeQuery();

        					// add indexed columns
        					while (indexedColumnSet.next())
        					{
        						String indexColumnName = indexedColumnSet.getString("name");
        						int isDescending = indexedColumnSet.getInt("is_descending_key");
        						for (int i = 0; i < columns.size(); ++i) {
        							String col = columns.get(i);
        							String alias = aliases.get(i);

        							// check alias if exists.
        							if (alias != "") {
        								if (alias.trim().compareTo(indexColumnName.trim()) == 0) {
        									indexedColumns.add(i);
        									break;
        								}
        							}
        							else {
        								if (col.contains(indexColumnName)) {
        									indexedColumns.add(i);
        									break;
        								}
        							}
        						}
        						isIndexedColumnsDescending.add(isDescending);
        						
        						if (indexedColumns.size() != isIndexedColumnsDescending.size()) {
        							throw new Exception("Indexed column in an indexed view not found: " + indexColumnName);
        						}
        					}

        					indexedColumnSet.close();
        				}
        				else {
        					continue;
        				}
        			}
        		}
        	}
        	viewDefinition.close();

        	MicrosoftIndexedView newView = new MicrosoftIndexedView(columns, aliases, indexedColumns, isIndexedColumnsDescending,
        			from, where, groupBy, viewName, indexName, schemaName, objectId, indexId, indexType, isDisabled, isUnique, 
        			isPrimaryKey, isUniqueConstraint);

        	physicalStructures.add(newView);
        }

        rs.close();
        stmt.close();
        conn.close();

        MicrosoftPhysicalDesign newDesign = new MicrosoftPhysicalDesign(physicalStructures, physicalDesignFilename);

		return newDesign;
	}

	private boolean copyStatistics(String pathToStat) {
		
		try {
			List<String> arguments = new ArrayList<String>();

			File statFile = new File(pathToStat);
			String statFilename = statFile.getName();
			String statFilepath = statFile.getParentFile().getCanonicalPath();

			arguments.add("robocopy"); // robocopy command for copying
			arguments.add(statFilepath); // local source.
			arguments.add(String.format("\\\\%s", testLogin.getDBhost() + File.separator + remoteInputPath)); // remote destination.
			arguments.add(statFilename);

			runExternalProcess(arguments);

			statisticsFilename = statFilename;

		} catch (Exception e) {
			log.error("copyStatistics failed.");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private void importStatistics() {

		try {
			List<String> arguments = new ArrayList<String>();

			arguments.add(psExecPath);
			arguments.add("\\\\" + testLogin.getDBhost());
			arguments.add("-accepteula");
//			arguments.add("-i");

			arguments.add("sqlcmd");

			// SQL Server name
			arguments.add("-S");
			arguments.add("localhost");

			// DB name
			arguments.add("-d");
			arguments.add(testLogin.getDBname());

			// username
			arguments.add("-U");
			arguments.add(testLogin.getDBuser());

			// password
			arguments.add("-P");
			arguments.add(testLogin.getDBpasswd());

			// input physical design script
			arguments.add("-i");
			arguments.add(inputPath + File.separator + statisticsFilename);

			runExternalProcess(arguments);
		} catch (Exception e) {
			log.error("importStatistics failed.");
			e.printStackTrace();
		}
	}

}
