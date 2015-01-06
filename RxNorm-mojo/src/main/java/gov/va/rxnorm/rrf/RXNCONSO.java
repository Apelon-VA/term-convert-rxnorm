package gov.va.rxnorm.rrf;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RXNCONSO
{
	public String rxcui, lat, rxaui, saui, scui, sab, tty, code, str, suppress, cvf;

	public RXNCONSO(ResultSet rs) throws SQLException
	{
		rxcui = rs.getString("RXCUI");
		lat = rs.getString("LAT");
		rxaui = rs.getString("RXAUI");
		saui = rs.getString("SAUI");
		scui = rs.getString("SCUI");
		sab = rs.getString("SAB");
		tty = rs.getString("TTY");
		code = rs.getString("CODE");
		str = rs.getString("STR");
		suppress = rs.getString("SUPPRESS");
		cvf = rs.getString("CVF");
	}
}
