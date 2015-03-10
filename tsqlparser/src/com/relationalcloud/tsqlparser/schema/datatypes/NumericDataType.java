package com.relationalcloud.tsqlparser.schema.datatypes;


/**
 * A numeric data type
 * <br>Syntax:
 * <ul>
 * <li>BIT[(M)]</li>
 * <li>TINYINT[(M)] [UNSIGNED] [ZEROFILL]</li>
 * <li>BOOL, BOOLEAN</li>
 * <li>SMALLINT[(M)] [UNSIGNED] [ZEROFILL]</li>
 * <li>MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]</li>
 * <li>INT[(M)] [UNSIGNED] [ZEROFILL]</li>
 * <li>INTEGER[(M)] [UNSIGNED] [ZEROFILL]</li>
 * <li>BIGINT[(M)] [UNSIGNED] [ZEROFILL]</li>
 * <li>FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]</li>
 * <li>DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]</li>
 * <li>DOUBLE PRECISION[(M,D)] [UNSIGNED] [ZEROFILL]</li>
 * <li>REAL[(M,D)] [UNSIGNED] [ZEROFILL]</li>
 * <li>FLOAT(p) [UNSIGNED] [ZEROFILL]</li>
 * <li>DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]</li>
 * <li>DEC[(M[,D])] [UNSIGNED] [ZEROFILL]</li>
 * <li>NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL]</li>
 * <li>FIXED[(M[,D])] [UNSIGNED] [ZEROFILL]</li>
 * </ul>
 * @author fangar
 *
 */
public class NumericDataType extends DataType {

	private boolean unsigned;
	private boolean zerofill;
	private int precision;
	private int scale;
	
	/*
	 * scale e precision set to 0 (zero) if nothing is specified
	 */
	
	public boolean isUnsigned() {
		return unsigned;
	}
	public void setUnsigned(boolean unsigned) {
		this.unsigned = unsigned;
	}
	public boolean isZerofill() {
		return zerofill;
	}
	public void setZerofill(boolean zerofill) {
		this.zerofill = zerofill;
	}
	public int getPrecision() {
		return precision;
	}
	public void setPrecision(int precision) {
		this.precision = precision;
	}
	public int getScale() {
		return scale;
	}
	public void setScale(int scale) {
		this.scale = scale;
	}
	
}
