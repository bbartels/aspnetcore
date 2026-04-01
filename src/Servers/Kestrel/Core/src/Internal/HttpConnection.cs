// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http2;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http3;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Microsoft.AspNetCore.Server.Kestrel.Core.Internal;

/// <remarks>
/// Instantiated by <see cref="HttpConnectionMiddleware{TContext}"/> when a connection is received.
/// <para/>
/// Not related, type-wise, to <see cref="Http1Connection{TContext}"/>, <see cref="Http2Connection"/>,
/// or <see cref="Http3Connection"/>. It does, however, instantiate one of those types as its
/// <see cref="_requestProcessor"/> based on the protocol.
/// </remarks>
internal sealed class HttpConnection : ITimeoutHandler
{
    private static ReadOnlySpan<byte> Http2Id => "h2"u8;

    private readonly BaseHttpConnectionContext _context;
    private readonly TimeProvider _timeProvider;
    private readonly TimeoutControl _timeoutControl;

    private readonly Lock _protocolSelectionLock = new();
    private ProtocolSelectionState _protocolSelectionState = ProtocolSelectionState.Initializing;
    private Http1Connection? _http1Connection;

    // Internal for testing
    internal IRequestProcessor? _requestProcessor;

    // Non-null only during H2C prior-knowledge protocol negotiation (see NegotiateH2cProtocolAsync).
    // Used to cancel a pending read when an external event (timeout, graceful shutdown, connection
    // close) occurs before protocol selection has completed.
    private volatile PipeReader? _negotiationInput;

    public HttpConnection(BaseHttpConnectionContext context)
    {
        _context = context;
        _timeProvider = _context.ServiceContext.TimeProvider;

        _timeoutControl = new TimeoutControl(this, _timeProvider);

        // Tests override the timeout control sometimes
        _context.TimeoutControl ??= _timeoutControl;
    }

    private KestrelTrace Log => _context.ServiceContext.Log;

    public async Task ProcessRequestsAsync<TContext>(IHttpApplication<TContext> httpApplication) where TContext : notnull
    {
        IConnectionMetricsTagsFeature? connectionMetricsTagsFeature = null;

        try
        {
            connectionMetricsTagsFeature = _context.ConnectionFeatures.Get<IConnectionMetricsTagsFeature>();

            // Ensure TimeoutControl._lastTimestamp is initialized before anything that could set timeouts runs.
            _timeoutControl.Initialize();

            var connectionHeartbeatFeature = _context.ConnectionFeatures.Get<IConnectionHeartbeatFeature>();
            var connectionLifetimeNotificationFeature = _context.ConnectionFeatures.Get<IConnectionLifetimeNotificationFeature>();

            // These features should never be null in Kestrel itself, if this middleware is ever refactored to run outside of kestrel,
            // we'll need to handle these missing.
            Debug.Assert(connectionHeartbeatFeature != null, nameof(IConnectionHeartbeatFeature) + " is missing!");
            Debug.Assert(connectionLifetimeNotificationFeature != null, nameof(IConnectionLifetimeNotificationFeature) + " is missing!");

            // Register callbacks before protocol selection so that timeouts and graceful-shutdown
            // signals are honoured even during H2C prior-knowledge preface detection.

            // The heart beat for various timeouts
            connectionHeartbeatFeature?.OnHeartbeat(state => ((HttpConnection)state).Tick(), this);

            // Register for graceful shutdown of the server
            using var shutdownRegistration = connectionLifetimeNotificationFeature?.ConnectionClosedRequested.Register(state => ((HttpConnection)state!).StopProcessingNextRequest(ConnectionEndReason.GracefulAppShutdown), this);

            // Register for connection close
            using var closedRegistration = _context.ConnectionContext.ConnectionClosed.Register(state => ((HttpConnection)state!).OnConnectionClosed(), this);

            IRequestProcessor? requestProcessor = null;
            string? httpVersion = null;

            switch (await SelectProtocolAsync())
            {
                case HttpProtocols.Http1:
                    // _http1Connection must be initialized before adding the connection to the connection manager
                    requestProcessor = new Http1Connection<TContext>((HttpConnectionContext)_context);
                    httpVersion = KestrelMetrics.Http11;
                    break;
                case HttpProtocols.Http2:
                    // _http2Connection must be initialized before yielding control to the transport thread,
                    // to prevent a race condition where _http2Connection.Abort() is called just as
                    // _http2Connection is about to be initialized.
                    requestProcessor = new Http2Connection((HttpConnectionContext)_context);
                    httpVersion = KestrelMetrics.Http2;
                    break;
                case HttpProtocols.Http3:
                    requestProcessor = new Http3Connection((HttpMultiplexedConnectionContext)_context);
                    httpVersion = KestrelMetrics.Http3;
                    break;
                case HttpProtocols.None:
                    // An error was already logged in SelectProtocol(), but we should close the connection.
                    break;

                default:
                    // SelectProtocol() only returns Http1, Http2, Http3 or None.
                    throw new NotSupportedException($"{nameof(SelectProtocolAsync)} returned something other than Http1, Http2 or None.");
            }

            if (requestProcessor != null && TryActivateRequestProcessor(requestProcessor))
            {
                AddMetricsHttpProtocolTag(httpVersion!);
                await requestProcessor.ProcessRequestsAsync(httpApplication);
            }
        }
        catch (Exception ex)
        {
            Log.LogCritical(0, ex, $"Unexpected exception in {nameof(HttpConnection)}.{nameof(ProcessRequestsAsync)}.");
        }
        finally
        {
            // Before exiting HTTP layer, set the end reason on the context as a connection metrics tag.
            if (_context.MetricsContext.ConnectionEndReason is { } connectionEndReason)
            {
                KestrelMetrics.AddConnectionEndReason(connectionMetricsTagsFeature, connectionEndReason);
            }
        }
    }

    private void AddMetricsHttpProtocolTag(string httpVersion)
    {
        if (_context.ConnectionContext.Features.Get<IConnectionMetricsTagsFeature>() is { } metricsTags)
        {
            metricsTags.Tags.Add(new KeyValuePair<string, object?>("network.protocol.name", "http"));
            metricsTags.Tags.Add(new KeyValuePair<string, object?>("network.protocol.version", httpVersion));
        }
    }

    // For testing only
    internal void Initialize(IRequestProcessor requestProcessor)
    {
        _requestProcessor = requestProcessor;
        _http1Connection = requestProcessor as Http1Connection;
        _protocolSelectionState = ProtocolSelectionState.Selected;
    }

    private bool TryActivateRequestProcessor(IRequestProcessor requestProcessor)
    {
        lock (_protocolSelectionLock)
        {
            if (_protocolSelectionState == ProtocolSelectionState.Aborted)
            {
                return false;
            }

            Debug.Assert(_protocolSelectionState == ProtocolSelectionState.Initializing, $"Unexpected {nameof(ProtocolSelectionState)} {_protocolSelectionState}.");

            _requestProcessor = requestProcessor;
            _http1Connection = requestProcessor as Http1Connection;
            _protocolSelectionState = ProtocolSelectionState.Selected;

            return true;
        }
    }

    private void StopProcessingNextRequest(ConnectionEndReason reason)
    {
        ProtocolSelectionState previousState;
        lock (_protocolSelectionLock)
        {
            previousState = _protocolSelectionState;
            if (previousState == ProtocolSelectionState.Initializing)
            {
                _protocolSelectionState = ProtocolSelectionState.Aborted;
            }
        }

        switch (previousState)
        {
            case ProtocolSelectionState.Initializing:
                // Protocol selection (H2C preface detection) is in progress; cancel the pending read
                // so the selection task can unblock and observe that it has been aborted.
                _negotiationInput?.CancelPendingRead();
                break;
            case ProtocolSelectionState.Selected:
                _requestProcessor!.StopProcessingNextRequest(reason);
                break;
            case ProtocolSelectionState.Aborted:
                break;
        }
    }

    private void OnConnectionClosed()
    {
        ProtocolSelectionState previousState;
        lock (_protocolSelectionLock)
        {
            previousState = _protocolSelectionState;
            if (previousState == ProtocolSelectionState.Initializing)
            {
                _protocolSelectionState = ProtocolSelectionState.Aborted;
            }
        }

        switch (previousState)
        {
            case ProtocolSelectionState.Initializing:
                _negotiationInput?.CancelPendingRead();
                break;
            case ProtocolSelectionState.Selected:
                _requestProcessor!.OnInputOrOutputCompleted();
                break;
            case ProtocolSelectionState.Aborted:
                break;
        }
    }

    private void Abort(ConnectionAbortedException ex, ConnectionEndReason reason)
    {
        ProtocolSelectionState previousState;

        lock (_protocolSelectionLock)
        {
            previousState = _protocolSelectionState;
            if (previousState != ProtocolSelectionState.Aborted)
            {
                _protocolSelectionState = ProtocolSelectionState.Aborted;
            }
        }

        switch (previousState)
        {
            case ProtocolSelectionState.Initializing:
                _negotiationInput?.CancelPendingRead();
                break;
            case ProtocolSelectionState.Selected:
                _requestProcessor!.Abort(ex, reason);
                break;
            case ProtocolSelectionState.Aborted:
                break;
        }
    }

    private HttpProtocols SelectProtocol()
    {
        var hasTls = _context.ConnectionFeatures.Get<ITlsConnectionFeature>() != null;
        var applicationProtocol = _context.ConnectionFeatures.Get<ITlsApplicationProtocolFeature>()?.ApplicationProtocol
            ?? new ReadOnlyMemory<byte>();
        var isMultiplexTransport = _context is HttpMultiplexedConnectionContext;
        var http1Enabled = _context.Protocols.HasFlag(HttpProtocols.Http1);
        var http2Enabled = _context.Protocols.HasFlag(HttpProtocols.Http2);
        var http3Enabled = _context.Protocols.HasFlag(HttpProtocols.Http3);

        string? error = null;

        if (_context.Protocols == HttpProtocols.None)
        {
            error = CoreStrings.EndPointRequiresAtLeastOneProtocol;
        }

        if (isMultiplexTransport)
        {
            if (http3Enabled)
            {
                return HttpProtocols.Http3;
            }

            error = $"Protocols {_context.Protocols} not supported on multiplexed transport.";
        }

        if (!http1Enabled && http2Enabled && hasTls && !Http2Id.SequenceEqual(applicationProtocol.Span))
        {
            error = CoreStrings.EndPointHttp2NotNegotiated;
        }

        if (error != null)
        {
            Log.LogError(0, error);
            return HttpProtocols.None;
        }

        if (!hasTls && http1Enabled)
        {
            // Even if Http2 was enabled, default to Http1 because it's ambiguous without ALPN.
            // SelectProtocolAsync may upgrade this to Http2 via H2C prior-knowledge detection.
            return HttpProtocols.Http1;
        }

        return http2Enabled && (!hasTls || Http2Id.SequenceEqual(applicationProtocol.Span)) ? HttpProtocols.Http2 : HttpProtocols.Http1;
    }

    // Wraps SelectProtocol() and, for cleartext endpoints supporting both HTTP/1 and HTTP/2,
    // peeks at the initial bytes to detect an HTTP/2 prior-knowledge connection preface (RFC 7540
    // Section 3.4) and selects HTTP/2 accordingly.
    private async ValueTask<HttpProtocols> SelectProtocolAsync()
    {
        var protocol = SelectProtocol();

        // Only negotiate H2C when: the synchronous selection chose Http1, the endpoint also
        // advertises Http2, the connection is cleartext (no TLS), and we have direct access to
        // the transport pipe.  All other cases (TLS/ALPN, Http1-only, Http3) are handled above.
        if (protocol == HttpProtocols.Http1
            && _context.Protocols.HasFlag(HttpProtocols.Http2)
            && _context.ConnectionFeatures.Get<ITlsConnectionFeature>() == null
            && _context is HttpConnectionContext httpConnectionContext)
        {
            // While waiting for the preface, apply the keep-alive timeout so that a connection
            // that is opened but never sends data is eventually closed (the same timeout that
            // Http1Connection sets before its first BeginRead).
            _context.TimeoutControl.SetTimeout(
                _context.ServiceContext.ServerOptions.Limits.KeepAliveTimeout,
                TimeoutReason.KeepAlive);

            _negotiationInput = httpConnectionContext.Transport.Input;

            try
            {
                protocol = await NegotiateH2cProtocolAsync(_negotiationInput);
            }
            finally
            {
                _negotiationInput = null;
                // Cancel the pre-selection keep-alive timeout now that we have determined the protocol
                // (or aborted). The selected protocol handler will set its own timeout in its startup path.
                _context.TimeoutControl.CancelTimeout();
            }

            // If the connection was aborted during negotiation (e.g. by a timeout handler or a
            // graceful-shutdown signal), do not proceed to create a protocol handler.
            lock (_protocolSelectionLock)
            {
                if (_protocolSelectionState == ProtocolSelectionState.Aborted)
                {
                    return HttpProtocols.None;
                }
            }
        }

        return protocol;
    }

    // Performs as many reads as needed to distinguish an HTTP/2 client preface from a non-HTTP/2
    // connection and returns:
    //   • HttpProtocols.Http2  – if the first 24 bytes are exactly the HTTP/2 connection preface
    //   • HttpProtocols.Http1  – if the received bytes diverge from the preface, the read is
    //                            cancelled, or the connection completes before the preface arrives
    //
    // Bytes are never consumed so that the chosen protocol handler can process them normally.
    private static async ValueTask<HttpProtocols> NegotiateH2cProtocolAsync(PipeReader input)
    {
        var prefaceLength = Http2Connection.ClientPreface.Length;

        while (true)
        {
            var result = await input.ReadAsync();
            var buffer = result.Buffer;
            var consumed = buffer.Start;
            var examined = buffer.Start;

            try
            {
                if (result.IsCanceled)
                {
                    return HttpProtocols.Http1;
                }

                if (!HasHttp2PrefacePrefix(buffer))
                {
                    return HttpProtocols.Http1;
                }

                if (buffer.Length >= prefaceLength)
                {
                    return HttpProtocols.Http2;
                }

                if (result.IsCompleted)
                {
                    return HttpProtocols.Http1;
                }

                // Keep all bytes available for the eventual protocol handler, but mark the current
                // data as examined so the reader will wait for more input before completing again.
                examined = buffer.End;
            }
            finally
            {
                input.AdvanceTo(consumed, examined);
            }
        }
    }

    private static bool IsHttp2Preface(ReadOnlySequence<byte> preface)
        => SequenceEquals(preface, Http2Connection.ClientPreface);

    private static bool HasHttp2PrefacePrefix(ReadOnlySequence<byte> buffer)
    {
        var prefixLength = (int)Math.Min(buffer.Length, (long)Http2Connection.ClientPreface.Length);

        return SequenceEquals(buffer.Slice(0, prefixLength), Http2Connection.ClientPreface.Slice(0, prefixLength));
    }

    private static bool SequenceEquals(ReadOnlySequence<byte> sequence, ReadOnlySpan<byte> expected)
    {
        if (sequence.IsSingleSegment)
        {
            return sequence.FirstSpan.SequenceEqual(expected);
        }

        Span<byte> span = stackalloc byte[expected.Length];
        sequence.CopyTo(span);
        return span.SequenceEqual(expected);
    }

    private void Tick()
    {
        if (_protocolSelectionState == ProtocolSelectionState.Aborted)
        {
            // It's safe to check for timeouts on a dead connection,
            // but try not to in order to avoid extraneous logs.
            return;
        }

        var timestamp = _timeProvider.GetTimestamp();
        _timeoutControl.Tick(timestamp);

        // _requestProcessor is null while H2C prior-knowledge negotiation is in progress.
        _requestProcessor?.Tick(timestamp);
    }

    public void OnTimeout(TimeoutReason reason)
    {
        // In the cases that don't log directly here, we expect the setter of the timeout to also be the input
        // reader, so when the read is canceled or aborted, the reader should write the appropriate log.
        switch (reason)
        {
            case TimeoutReason.KeepAlive:
                if (_requestProcessor is null)
                {
                    // Timeout fired during H2C preface detection (no data received). Abort the
                    // connection; the negotiation task will observe the Aborted state and return None.
                    KestrelMetrics.AddConnectionEndReason(_context.MetricsContext, ConnectionEndReason.KeepAliveTimeout);
                    Abort(new ConnectionAbortedException(CoreStrings.ConnectionTimedOutByServer), ConnectionEndReason.KeepAliveTimeout);
                }
                else
                {
                    _requestProcessor.StopProcessingNextRequest(ConnectionEndReason.KeepAliveTimeout);
                }
                break;
            case TimeoutReason.RequestHeaders:
                _requestProcessor!.HandleRequestHeadersTimeout();
                break;
            case TimeoutReason.ReadDataRate:
                _requestProcessor!.HandleReadDataRateTimeout();
                break;
            case TimeoutReason.WriteDataRate:
                Log.ResponseMinimumDataRateNotSatisfied(_context.ConnectionId, _http1Connection?.TraceIdentifier);
                Abort(new ConnectionAbortedException(CoreStrings.ConnectionTimedBecauseResponseMininumDataRateNotSatisfied), ConnectionEndReason.MinResponseDataRate);
                break;
            case TimeoutReason.RequestBodyDrain:
            case TimeoutReason.TimeoutFeature:
                Abort(new ConnectionAbortedException(CoreStrings.ConnectionTimedOutByServer), ConnectionEndReason.ServerTimeout);
                break;
            default:
                Debug.Assert(false, "Invalid TimeoutReason");
                break;
        }
    }

    private enum ProtocolSelectionState
    {
        Initializing,
        Selected,
        Aborted
    }
}
