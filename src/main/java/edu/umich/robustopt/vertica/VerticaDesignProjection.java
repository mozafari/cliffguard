package edu.umich.robustopt.vertica;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.umich.robustopt.util.NamedIdentifier;

/**
 * A projection *proposed* by the vertica DBD. Isn't necessarily
 * implemented yet
 * 
 * @author stephentu
 */
public class VerticaDesignProjection {
	
	private Integer deployment_id = null;
	private Integer deployment_projection_id = null;
	private String design_name = null;
	private String deployment_projection_name = null;
	private String anchor_table_schema = null;
	private String anchor_table_name = null;
	private String deployment_operation = null;
	private String deployment_projection_type = null;
	private Integer deploy_weight = null;
	private String statement = null;                 
	private VerticaDeployedProjection repr = null;
	private String projection_schema = null;
	
	public VerticaDesignProjection(Connection secondaryConnection, ResultSet res) throws SQLException {
		/* Parse this:
		 * 			 * dbadmin=>  select * from v_dbd_example.vs_deployment_projections;
			   deployment_id   | deployment_projection_id | design_name |    deployment_projection_name    | anchor_table_schema | anchor_table_name | deployment_operation | deployment_projection_type | deploy_weight 
			-------------------+--------------------------+-------------+----------------------------------+---------------------+-------------------+----------------------+----------------------------+---------------
			 45035996273712269 |                        1 | designname  | foo_DBD_1_rep_example_designname | s                   | foo               | add                  | DBD                        |           300
			 45035996273712269 |                        2 | designname  | bar_DBD_2_rep_example_designname | s                   | bar               | add                  | DBD                        |             0
			 45035996273712269 |                        3 | N/A         | foo_super                        | s                   | foo               | drop                 | CATALOG                    |             0
		 * 
		 */
		this.deployment_id = res.getInt("deployment_id");
		this.deployment_projection_id = res.getInt("deployment_projection_id");
		this.design_name = res.getString("design_name");
		this.deployment_projection_name = res.getString("deployment_projection_name");
		this.anchor_table_schema = res.getString("anchor_table_schema");
		
		this.anchor_table_name = res.getString("anchor_table_name");
		String proj_type = res.getString("deployment_projection_type");
		this.deployment_operation = res.getString("deployment_operation");
		this.deployment_projection_type = res.getString("deployment_projection_type");
		this.deploy_weight = res.getInt("deploy_weight");
		this.statement = res.getString("statement");

		if (proj_type.toLowerCase().equals("dbd")) {
			this.projection_schema = this.anchor_table_schema; // Note that since this projection is not actually deployed in the system (i.e., it's only for the DBD purpose), we cannot find its projection_schema in the v_catalog.projections table, since it doesn't exist yet!
			this.repr = getProjection(secondaryConnection);
		} else {
			if (proj_type.toLowerCase().equals("existing"))
				System.err.println("You have existing projection on the design DB. Delete them!");
			String sqlForFindingSchema = "select projection_schema from v_catalog.projections where projection_name='"+ deployment_projection_name + 
					"' and anchor_table_name = '"+ anchor_table_name + "';";
			Statement stmt = secondaryConnection.createStatement();
			ResultSet secRs = stmt.executeQuery(sqlForFindingSchema);
			List<String> schemaNames = new ArrayList<String>();
			while (secRs.next()) {
				schemaNames.add(secRs.getString(1));
			}
			if (schemaNames.size() != 1) {
				System.err.println("The following query returned " + schemaNames.size() + " results instead of exactly 1: " + sqlForFindingSchema);
				for (String s : schemaNames)
					System.err.println("output: " + s);
				throw new SQLException("unexpected results for: "+ sqlForFindingSchema);
			} else
				this.projection_schema  = schemaNames.get(0);
			secRs.close();
			stmt.close();
			this.repr = VerticaDeployedProjection.BuildFrom(secondaryConnection, getProjection_schema(), deployment_projection_name, false);
		}
	}
	
	public NamedIdentifier getNamedIdent() {
		return new NamedIdentifier(getProjection_schema(), getDeployment_projection_name());
	}
	
	public Integer getDeployment_id() {
		return deployment_id;
	}

	public Integer getDeployment_projection_id() {
		return deployment_projection_id;
	}

	public String getDesign_name() {
		return design_name;
	}

	public String getDeployment_projection_name() {
		return deployment_projection_name;
	}

	public String getAnchor_table_schema() {
		return anchor_table_schema;
	}

	public String getAnchor_table_name() {
		return anchor_table_name;
	}

	public String getDeployment_operation() {
		return deployment_operation;
	}

	public String getDeployment_projection_type() {
		return deployment_projection_type;
	}

	public Integer getDeploy_weight() {
		return deploy_weight;
	}

	public String getStatement() {
		return statement;
	}
	
	
	
	public String getProjection_schema() {
		return projection_schema;
	}

	private VerticaDeployedProjection getProjection(Connection conn) throws SQLException {
		Statement stmt = null;
		VerticaDeployedProjection p = null;
		String sqlStr = null;
		try {
			
			try {
				p = VerticaDeployedProjection.BuildFrom(conn, getProjection_schema(), deployment_projection_name, true);
			} catch (SQLException e) {
				// projection does not exist, so create it yourself and then delete it!
				stmt = conn.createStatement();
				//sqlStr = "DROP PROJECTION IF EXISTS " + getNamedIdent().toString();
				//stmt.execute(sqlStr);
				
				sqlStr = getStatement();
				stmt.execute(sqlStr);
				
				p = VerticaDeployedProjection.BuildFrom(conn, getProjection_schema(), deployment_projection_name, true);
				
				try {
					sqlStr = "DROP PROJECTION " + getNamedIdent().toString();
					stmt.execute(sqlStr);
				} catch (SQLException s) {
					//screw them! this stupid system doesn't even let me delete a projection that they didn't have before!
				}
			}
		} catch (SQLException e) {
			System.err.println("sqlStr: " + sqlStr);
			throw e;
		}	finally {
			if (stmt != null)
				stmt.close();
		}
		
		return p;
	}
	
	public VerticaDeployedProjection getRepresentation() {
		return repr;
	}
	
	@Override
	public String toString() {
		return getNamedIdent().toString();
	}
	
}
