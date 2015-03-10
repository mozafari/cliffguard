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

package com.relationalcloud.tsqlparser.statement.alter.table;

import java.util.List;

/**
 * An ORDER BY statement
 * Syntax:
 * ORDER BY col_name [, col_name] ...
 * @author fangar
 *
 */
public class AlterTableOrderByStatement extends AlterTableStatement {

	private List orderByElements;

	public List getOrderByElements() {
		return orderByElements;
	}

	public void setOrderByElements(List orderByElements) {
		this.orderByElements = orderByElements;
	}
	
	
	
}
