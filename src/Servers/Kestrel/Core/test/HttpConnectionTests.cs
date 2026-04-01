// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using Microsoft.AspNetCore.InternalTesting;
using Moq;
using Xunit;

namespace Microsoft.AspNetCore.Server.Kestrel.Core.Tests;

public class HttpConnectionTests
{
    [Fact]
    public async Task WriteDataRateTimeoutAbortsConnection()
    {
        var mockConnectionContext = new Mock<ConnectionContext>();

        var httpConnectionContext = TestContextFactory.CreateHttpConnectionContext(
            serviceContext: new TestServiceContext(),
            connectionContext: mockConnectionContext.Object,
            connectionFeatures: new FeatureCollection(),
            transport: new DuplexPipe(Mock.Of<PipeReader>(), Mock.Of<PipeWriter>()));

        var httpConnection = new HttpConnection(httpConnectionContext);

        var aborted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var http1Connection = new Http1Connection(httpConnectionContext);

        httpConnection.Initialize(http1Connection);
        http1Connection.Reset();
        http1Connection.RequestAborted.Register(() =>
        {
            aborted.SetResult();
        });

        httpConnection.OnTimeout(TimeoutReason.WriteDataRate);

        mockConnectionContext
            .Verify(c => c.Abort(It.Is<ConnectionAbortedException>(ex => ex.Message == CoreStrings.ConnectionTimedBecauseResponseMininumDataRateNotSatisfied)),
                Times.Once);

        await aborted.Task.DefaultTimeout();
    }

    [Fact]
    public async Task ProcessRequestsAsync_ConnectionClosedDuringH2cNegotiation_CompletesWithoutSelectingProtocol()
    {
        var (httpConnection, connectionContext, _, readStartedReader, _) = CreateHttpConnectionForH2cNegotiation();

        var processingTask = httpConnection.ProcessRequestsAsync(new DummyApplication());

        await readStartedReader.ReadStarted.Task.DefaultTimeout();

        connectionContext.Abort(new ConnectionAbortedException("Test abort"));

        await processingTask.DefaultTimeout();

        Assert.Null(httpConnection._requestProcessor);
    }

    [Fact]
    public async Task ProcessRequestsAsync_GracefulShutdownDuringH2cNegotiation_CompletesWithoutSelectingProtocol()
    {
        var (httpConnection, _, lifetimeNotificationFeature, readStartedReader, _) = CreateHttpConnectionForH2cNegotiation();

        var processingTask = httpConnection.ProcessRequestsAsync(new DummyApplication());

        await readStartedReader.ReadStarted.Task.DefaultTimeout();

        lifetimeNotificationFeature.RequestClose();

        await processingTask.DefaultTimeout();

        Assert.Null(httpConnection._requestProcessor);
    }

    [Fact]
    public async Task ProcessRequestsAsync_KeepAliveTimeoutDuringH2cNegotiation_CompletesWithoutSelectingProtocol()
    {
        var (httpConnection, _, _, readStartedReader, metricsContext) = CreateHttpConnectionForH2cNegotiation();

        var processingTask = httpConnection.ProcessRequestsAsync(new DummyApplication());

        await readStartedReader.ReadStarted.Task.DefaultTimeout();

        httpConnection.OnTimeout(TimeoutReason.KeepAlive);

        await processingTask.DefaultTimeout();

        Assert.Null(httpConnection._requestProcessor);
        Assert.Equal(ConnectionEndReason.KeepAliveTimeout, metricsContext.ConnectionEndReason);
    }

    private static (HttpConnection HttpConnection, DefaultConnectionContext ConnectionContext, TestConnectionLifetimeFeature LifetimeNotificationFeature, ReadStartedPipeReader ReadStartedReader, ConnectionMetricsContext MetricsContext) CreateHttpConnectionForH2cNegotiation()
    {
        var serviceContext = new TestServiceContext();
        var transportPair = DuplexPipe.CreateConnectionPair(new PipeOptions(), new PipeOptions());
        var connectionContext = new DefaultConnectionContext();
        var lifetimeNotificationFeature = new TestConnectionLifetimeFeature();
        var readStartedReader = new ReadStartedPipeReader(transportPair.Transport.Input);
        var transport = new DuplexPipe(readStartedReader, transportPair.Transport.Output);
        var metricsContext = TestContextFactory.CreateMetricsContext(connectionContext);

        connectionContext.Transport = transport;
        connectionContext.Features.Set<IConnectionHeartbeatFeature>(lifetimeNotificationFeature);
        connectionContext.Features.Set<IConnectionLifetimeNotificationFeature>(lifetimeNotificationFeature);

        var httpConnectionContext = TestContextFactory.CreateHttpConnectionContext(
            serviceContext: serviceContext,
            connectionContext: connectionContext,
            connectionFeatures: connectionContext.Features,
            transport: transport,
            protocols: HttpProtocols.Http1AndHttp2,
            metricsContext: metricsContext);

        return (new HttpConnection(httpConnectionContext), connectionContext, lifetimeNotificationFeature, readStartedReader, metricsContext);
    }

    private sealed class TestConnectionLifetimeFeature : IConnectionHeartbeatFeature, IConnectionLifetimeNotificationFeature
    {
        private readonly CancellationTokenSource _connectionClosedRequestedCts = new();

        public CancellationToken ConnectionClosedRequested { get; set; }

        public TestConnectionLifetimeFeature()
        {
            ConnectionClosedRequested = _connectionClosedRequestedCts.Token;
        }

        public void OnHeartbeat(Action<object> action, object state)
        {
        }

        public void RequestClose()
        {
            _connectionClosedRequestedCts.Cancel();
        }
    }

    private sealed class ReadStartedPipeReader : PipeReader
    {
        private readonly PipeReader _inner;

        public ReadStartedPipeReader(PipeReader inner)
        {
            _inner = inner;
        }

        public TaskCompletionSource ReadStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override void AdvanceTo(SequencePosition consumed)
            => _inner.AdvanceTo(consumed);

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
            => _inner.AdvanceTo(consumed, examined);

        public override void CancelPendingRead()
            => _inner.CancelPendingRead();

        public override void Complete(Exception exception = null)
            => _inner.Complete(exception);

        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            ReadStarted.TrySetResult();
            return _inner.ReadAsync(cancellationToken);
        }

        public override bool TryRead(out ReadResult result)
            => _inner.TryRead(out result);
    }
}
