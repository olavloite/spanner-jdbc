package nl.topicus.sql2.operation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import nl.topicus.java.sql2.Submission;

public class CloudSpannerSubmission<T> implements Submission<T>
{
	private CompletableFuture<T> res;

	private Future<Boolean> cancelFuture;

	CloudSpannerSubmission(CompletableFuture<T> res)
	{
		this.res = res;
	}

	@Override
	public Future<Boolean> cancel()
	{
		return cancelFuture;
	}

	@Override
	public CompletableFuture<T> toCompletableFuture()
	{
		return res;
	}

}
