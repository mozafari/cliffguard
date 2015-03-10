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

package com.relationalcloud.tsqlparser.schema.datatypes;

import java.util.List;

/**
 * A string data type
 * <br>Syntax:
 * <ul>
 * <li>[NATIONAL] CHAR[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]</li>
 * <li>[NATIONAL] VARCHAR(M) [CHARACTER SET charset_name] [COLLATE collation_name]</li>
 * <li>BINARY(M)</li>
 * <li>VARBINARY(M)</li>
 * <li>TINYBLOB</li>
 * <li>TINYTEXT [CHARACTER SET charset_name] [COLLATE collation_name]</li>
 * <li>BLOB[(M)]</li>
 * <li>TEXT[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]</li>
 * <li>MEDIUMBLOB</li>
 * <li>MEDIUMTEXT [CHARACTER SET charset_name] [COLLATE collation_name]</li>
 * <li>LONGBLOB</li>
 * <li>LONGTEXT [CHARACTER SET charset_name] [COLLATE collation_name]</li>
 * <li>ENUM('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]</li>
 * <li>SET('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]</li>
 * </ul>
 * @author fangar
 *
 */
public class StringDataType extends DataType {

	
	private String charsetName;
	private String collation;
	private List<String> possibleValues;
	private int length;
	
	
	public String getCharsetName() {
		return charsetName;
	}
	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}
	public String getCollation() {
		return collation;
	}
	public void setCollation(String collation) {
		this.collation = collation;
	}
	public List<String> getPossibleValues() {
		return possibleValues;
	}
	public void setPossibleValues(List<String> possibleValues) {
		this.possibleValues = possibleValues;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	
}
