package nl.topicus.jdbc.xa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import javax.transaction.xa.Xid;

import org.junit.Test;

public class RecoveredXidTest
{

	@Test
	public void testHashCodeAndEquals()
	{
		Xid xid1 = RecoveredXid.stringToXid("9999_Z3RyaWQ=_YnF1YWw=");
		Xid xid2 = RecoveredXid.stringToXid("9999_Z3RyaWQ=_YnF1YWw=");
		Xid xid3 = RecoveredXid.stringToXid("1234_Z3RyaWQ=_YnF1YWw=");
		assertEquals(xid1.hashCode(), xid2.hashCode());
		assertEquals(xid1, xid2);
		assertNotEquals(xid1.hashCode(), xid3.hashCode());
		assertNotEquals(xid1, xid3);

		xid1 = RecoveredXid.stringToXid("9999_AbCdEfG=_YnF1YWw=");
		xid2 = RecoveredXid.stringToXid("9999_AbCdEfG=_YnF1YWw=");
		xid3 = RecoveredXid.stringToXid("1234_abcdefg=_YnF1YWw=");
		assertEquals(xid1.hashCode(), xid2.hashCode());
		assertEquals(xid1, xid2);
		assertNotEquals(xid1.hashCode(), xid3.hashCode());
		assertNotEquals(xid1, xid3);
	}

}
