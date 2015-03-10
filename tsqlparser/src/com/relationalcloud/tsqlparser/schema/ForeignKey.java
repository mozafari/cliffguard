/* ================================================================
 * JSQLParser : java based sql parser 
 * ================================================================
 *
 * Project Info:  http://jsqlparser.sourceforge.net
 * Project Lead:  Leonardo Francalanci (leoonardoo@yahoo.it);
 *
 * (C) Copyright 2004, by Leonardo Francalanci
 *
 * This library is free software; you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License as published by the Free Software Foundation;
 * either version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307, USA.
 */

package com.relationalcloud.tsqlparser.schema;

import java.util.List;

/**
 * A foreign Key
 * <br> Includes the referenced table and the column involved in the foreign key
 * @author fangar
 *
 */
public class ForeignKey extends Index {

	private List<String> columnsReferenced;
	private ReferenceDefinition referenceDefinition;
	
	
	/**
	 * Get the list of {@link Column} referenced by the foreign key
	 * @return
	 */
	public List<String> getColumnsReferenced() {
		return columnsReferenced;
	}
	/**
	 * Set the list of {@link Column} referenced by the foreign key
	 * @param columnsReferenced
	 */
	public void setColumnsReferenced(List<String> columnsReferenced) {
		this.columnsReferenced = columnsReferenced;
	}
	
	public ReferenceDefinition getReferenceDefinition() {
		return referenceDefinition;
	}

	public void setReferenceDefinition(ReferenceDefinition referenceDefinition) {
		this.referenceDefinition = referenceDefinition;
	}
	
	
}
