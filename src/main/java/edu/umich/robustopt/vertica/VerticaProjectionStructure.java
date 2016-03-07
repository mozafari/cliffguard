package edu.umich.robustopt.vertica;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


import edu.umich.robustopt.physicalstructures.PhysicalStructure;
import edu.umich.robustopt.physicalstructures.Projection;
import edu.umich.robustopt.util.ListUtils;
import edu.umich.robustopt.util.NamedIdentifier;
import edu.umich.robustopt.util.Pair;
import edu.umich.robustopt.util.StringUtils;

/**
 * Contains the underlying structure of a vertica projection.
 * 
 * @author stephentu
 *
 */
public class VerticaProjectionStructure extends PhysicalStructure  {
	
	private static final long serialVersionUID = -2413599799048990258L;

	private final NamedIdentifier projection_anchor_table;
	private final List<String> projection_columns;
	private final List<String> projection_column_datatypes;
	private final List<String> projection_column_encodings;
	private final List<String> projection_sort_order;
	
	public VerticaProjectionStructure(
			NamedIdentifier projection_anchor_table,
			List<String> projection_columns,
			List<String> projection_column_datatypes,
			List<String> projection_column_encodings,
			List<String> projection_sort_order) {
		if (projection_columns.size() != projection_column_datatypes.size())
			throw new IllegalArgumentException("sizes do not match");
		if (projection_columns.size() != projection_column_encodings.size())
			throw new IllegalArgumentException("sizes do not match");
		this.projection_anchor_table = projection_anchor_table;
		this.projection_columns = projection_columns;
		this.projection_column_datatypes = projection_column_datatypes;
		this.projection_column_encodings = projection_column_encodings;
		this.projection_sort_order = projection_sort_order;
	}
	
	public NamedIdentifier getProjection_anchor_table() {
		return projection_anchor_table;
	}

	public List<String> getProjection_columns() {
		return projection_columns;
	}

	public List<String> getProjection_column_datatypes() {
		return projection_column_datatypes;
	}

	public List<String> getProjection_column_encodings() {
		return projection_column_encodings;
	}
	
	public List<String> getProjection_sort_order() {
		return projection_sort_order;
	}

	
	/**
	 * Creates a CREATE PROJECTION DDL statement used to implement this projection 
	 * at schema.projName
	 * 
	 * @param structureName ("schema.projName")
	 * @return
	 */
	public ArrayList<String> createPhysicalStructureSQL(String structureName) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE PROJECTION ");
		sb.append(structureName);
		sb.append("(");
		
		List<String> elems = new ArrayList<String>();
		for (Pair<String, String> e : ListUtils.Zip(projection_columns, projection_column_encodings))
			elems.add(e.first + " ENCODING " + e.second);
		sb.append(StringUtils.Join(elems, ", "));
		
		sb.append(")");
		sb.append(" AS SELECT ");
		sb.append(StringUtils.Join(projection_columns, ", "));
		sb.append(" FROM " + projection_anchor_table.getQualifiedName());
		
		if (!projection_sort_order.isEmpty()) {
			sb.append(" ORDER BY ");
			sb.append(StringUtils.Join(projection_sort_order, ", "));
		}
		
		sb.append(" UNSEGMENTED ALL NODES");
		
		String createProjectionSql = sb.toString();
		ArrayList<String> res = new ArrayList<String>();
		res.add(createProjectionSql);
		return res;
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((projection_anchor_table == null) ? 0
						: projection_anchor_table.hashCode());
		result = prime
				* result
				+ ((projection_column_datatypes == null) ? 0
						: projection_column_datatypes.hashCode());
		result = prime
				* result
				+ ((projection_column_encodings == null) ? 0
						: projection_column_encodings.hashCode());
		result = prime
				* result
				+ ((projection_columns == null) ? 0 : projection_columns
						.hashCode());
		result = prime
				* result
				+ ((projection_sort_order == null) ? 0 : projection_sort_order
						.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VerticaProjectionStructure other = (VerticaProjectionStructure) obj;
		if (projection_anchor_table == null) {
			if (other.projection_anchor_table != null)
				return false;
		} else if (!projection_anchor_table
				.equals(other.projection_anchor_table))
			return false;
		if (projection_column_datatypes == null) {
			if (other.projection_column_datatypes != null)
				return false;
		} else if (!projection_column_datatypes
				.equals(other.projection_column_datatypes))
			return false;
		if (projection_column_encodings == null) {
			if (other.projection_column_encodings != null)
				return false;
		} else if (!projection_column_encodings
				.equals(other.projection_column_encodings))
			return false;
		if (projection_columns == null) {
			if (other.projection_columns != null)
				return false;
		} else if (!projection_columns.equals(other.projection_columns))
			return false;
		if (projection_sort_order == null) {
			if (other.projection_sort_order != null)
				return false;
		} else if (!projection_sort_order.equals(other.projection_sort_order))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "VerticaProjectionStructure [projection_anchor_table="
				+ projection_anchor_table + "\n projection_columns="
				+ projection_columns + "\n projection_column_datatypes="
				+ projection_column_datatypes
				+ "\n projection_column_encodings="
				+ projection_column_encodings + "\n projection_sort_order="
				+ projection_sort_order + "]\n";
	}
	
	public String getHumanReadableSummary() {
		return "("+(getDiskSizeInGigabytes()!=null ? Math.round(getDiskSizeInGigabytes()*100)/100.0 +"GB" : "unknown)") +") PROJECTTION on table " + projection_anchor_table 
				+ "\n\tcolumns= " + projection_columns
				+ "\n\t sorted on= " + projection_sort_order + "\n";
	}
	
	public static List<VerticaProjectionStructure> generateTPCHProjections() {
		List<VerticaProjectionStructure> projections = new ArrayList<VerticaProjectionStructure>();
		NamedIdentifier anchor_table = new NamedIdentifier("public","lineitem");
		List<String> columns = new ArrayList<String>();
		List<String> column_datatypes = new ArrayList<String>();
		List<String> column_encodings = new ArrayList<String>();
		List<String> sort_order = new ArrayList<String>();
		columns.add("L_PARTKEY");
		column_datatypes.add("int");
		column_encodings.add("NONE");
		
		columns.add("L_SUPPKEY");
		column_datatypes.add("int");
		column_encodings.add("NONE");
		
		columns.add("L_LINENUMBER");
		column_datatypes.add("int");
		column_encodings.add("NONE");
		
		sort_order.add("L_SUPPKEY");
		sort_order.add("L_LINENUMBER");
		sort_order.add("L_PARTKEY");
		VerticaProjectionStructure projection1 = new VerticaProjectionStructure(anchor_table, columns, column_datatypes, column_encodings, sort_order);
		projections.add(projection1);
		
		columns = new ArrayList<String>();
		column_datatypes = new ArrayList<String>();
		column_encodings = new ArrayList<String>();
		sort_order = new ArrayList<String>();
		columns.add("L_COMMITDATE");
		column_datatypes.add("date");
		column_encodings.add("NONE");
		
		columns.add("L_RECEIPTDATE");
		column_datatypes.add("date");
		column_encodings.add("NONE");
		
		columns.add("L_SHIPDATE");
		column_datatypes.add("date");
		column_encodings.add("NONE");
		
		sort_order.add("L_RECEIPTDATE");
		sort_order.add("L_SHIPDATE");
		sort_order.add("L_COMMITDATE");
		VerticaProjectionStructure projection2 = new VerticaProjectionStructure(anchor_table, columns, column_datatypes, column_encodings, sort_order);
		projections.add(projection2);
		
		return projections;
	}
	
}
