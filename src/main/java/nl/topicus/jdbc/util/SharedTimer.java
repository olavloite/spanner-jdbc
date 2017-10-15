package nl.topicus.jdbc.util;

import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

import nl.topicus.jdbc.Logger;

/**
 * Implementation based on PostgreSQL
 * 
 * PostgreSQL is released under the PostgreSQL License, a liberal Open Source
 * license, similar to the BSD or MIT licenses.
 *
 * PostgreSQL Database Management System (formerly known as Postgres, then as
 * Postgres95)
 *
 * Portions Copyright (c) 1996-2017, The PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
 * "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
 * MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * @author loite
 *
 */
public class SharedTimer
{
	// Incremented for each Timer created, this allows each to have a unique
	// Timer name
	private static AtomicInteger timerCount = new AtomicInteger(0);

	private Logger log;
	private volatile Timer timer = null;
	private AtomicInteger refCount = new AtomicInteger(0);

	public SharedTimer(Logger log)
	{
		this.log = log;
	}

	public int getRefCount()
	{
		return refCount.get();
	}

	public synchronized Timer getTimer()
	{
		if (timer == null)
		{
			int index = timerCount.incrementAndGet();
			timer = new Timer("PostgreSQL-JDBC-SharedTimer-" + index, true);
		}
		refCount.incrementAndGet();
		return timer;
	}

	public synchronized void releaseTimer()
	{
		int count = refCount.decrementAndGet();
		if (count > 0)
		{
			// There are outstanding references to the timer so do nothing
			log.debug("Outstanding references still exist so not closing shared Timer");
		}
		else if (count == 0)
		{
			// This is the last usage of the Timer so cancel it so it's
			// resources can be release.
			log.debug("No outstanding references to shared Timer, will cancel and close it");
			if (timer != null)
			{
				timer.cancel();
				timer = null;
			}
		}
		else
		{
			// Should not get here under normal circumstance, probably a bug in
			// app code.
			log.debug("releaseTimer() called too many times; there is probably a bug in the calling code");
			refCount.set(0);
		}
	}
}
