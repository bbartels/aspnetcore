// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http2;
using Microsoft.AspNetCore.Server.Kestrel.InMemory.FunctionalTests.TestTransport;
using Microsoft.AspNetCore.InternalTesting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Microsoft.AspNetCore.Server.Kestrel.InMemory.FunctionalTests;

public class HttpProtocolSelectionTests : TestApplicationErrorLoggerLoggedTest
{
    private static readonly string Http2ClientPreface = Encoding.ASCII.GetString(Http2Connection.ClientPreface);
    private static readonly string Http2ServerPreface = Encoding.ASCII.GetString(new byte[]
    {
            0x00, 0x00, 0x18,
            0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00, 0x00, 0x64,
            0x00, 0x04, 0x00, 0x0C, 0x00, 0x00,
            0x00, 0x06, 0x00, 0x00, 0x80, 0x00,
            0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x04,
            0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x0F, 0x00, 0x01,
    });

    [Fact]
    public Task Server_NoProtocols_Error()
    {
        return TestError<InvalidOperationException>(HttpProtocols.None, CoreStrings.EndPointRequiresAtLeastOneProtocol);
    }

    [Fact]
    public Task Server_Http1AndHttp2_Cleartext_Http1Default()
    {
        return TestSuccess(HttpProtocols.Http1AndHttp2, "GET / HTTP/1.1\r\nHost:\r\n\r\n", "HTTP/1.1 200 OK");
    }

    [Fact]
    public Task Server_Http1Only_Cleartext_Success()
    {
        return TestSuccess(HttpProtocols.Http1, "GET / HTTP/1.1\r\nHost:\r\n\r\n", "HTTP/1.1 200 OK");
    }

    [Fact]
    public Task Server_Http2Only_Cleartext_Success()
        => TestHttp2PrefaceSuccess(HttpProtocols.Http2);

    /// <summary>
    /// When a cleartext endpoint advertises both HTTP/1 and HTTP/2 and the client sends the HTTP/2
    /// prior-knowledge connection preface, the server must select HTTP/2 (H2C, RFC 7540 §3.4).
    /// </summary>
    [Fact]
    public Task Server_Http1AndHttp2_Cleartext_SelectsHttp2_WhenH2cPrefaceSent()
        => TestHttp2PrefaceSuccess(HttpProtocols.Http1AndHttp2);

    /// <summary>
    /// When a cleartext endpoint advertises HTTP/1, HTTP/2, and HTTP/3, and the client sends the
    /// HTTP/2 prior-knowledge connection preface, HTTP/2 must be selected on the cleartext
    /// transport (HTTP/3 requires TLS/QUIC and is not available on a cleartext TCP connection).
    /// </summary>
    [Fact]
    public Task Server_Http1AndHttp2AndHttp3_Cleartext_SelectsHttp2_WhenH2cPrefaceSent()
        => TestHttp2PrefaceSuccess(HttpProtocols.Http1AndHttp2AndHttp3);

    [Fact]
    public async Task Server_Http1AndHttp2_Cleartext_SelectsHttp2_WhenH2cPrefaceIsFragmented()
    {
        var testContext = new TestServiceContext(LoggerFactory);
        var listenOptions = CreateListenOptions(HttpProtocols.Http1AndHttp2);

        await using var server = new TestServer(context => Task.CompletedTask, testContext, listenOptions);
        using var connection = server.CreateConnection();

        var preface = Http2Connection.ClientPreface.ToArray();

        await connection.Stream.WriteAsync(preface.AsMemory(0, 8));
        await connection.Stream.FlushAsync();
        await Task.Yield();
        await connection.Stream.WriteAsync(preface.AsMemory(8));
        await connection.Stream.FlushAsync();

        await connection.Receive(Http2ServerPreface);
    }

    [Fact]
    public async Task Server_Http1AndHttp2_Cleartext_FallsBackToHttp1_WhenPrefacePrefixDiverges()
    {
        var testContext = new TestServiceContext(LoggerFactory);
        var listenOptions = CreateListenOptions(HttpProtocols.Http1AndHttp2);

        await using var server = new TestServer(context => Task.CompletedTask, testContext, listenOptions);
        using var connection = server.CreateConnection();

        await connection.Stream.WriteAsync(Encoding.ASCII.GetBytes("PRI "));
        await connection.Stream.FlushAsync();
        await Task.Yield();
        await connection.Stream.WriteAsync(Encoding.ASCII.GetBytes("/ HTTP/1.1\r\nHost:\r\n\r\n"));
        await connection.Stream.FlushAsync();

        await connection.Receive("HTTP/1.1 200 OK");
    }

    private Task TestHttp2PrefaceSuccess(HttpProtocols serverProtocols)
        => TestSuccess(serverProtocols, Http2ClientPreface, Http2ServerPreface);

    private async Task TestSuccess(HttpProtocols serverProtocols, string request, string expectedResponse)
    {
        var testContext = new TestServiceContext(LoggerFactory);
        var listenOptions = CreateListenOptions(serverProtocols);

        await using (var server = new TestServer(context => Task.CompletedTask, testContext, listenOptions))
        {
            using (var connection = server.CreateConnection())
            {
                await connection.Send(request);
                await connection.Receive(expectedResponse);
            }
        }
    }

    private async Task TestError<TException>(HttpProtocols serverProtocols, string expectedErrorMessage)
        where TException : Exception
    {
        var testContext = new TestServiceContext(LoggerFactory);
        var listenOptions = CreateListenOptions(serverProtocols);

        await using (var server = new TestServer(context => Task.CompletedTask, testContext, listenOptions))
        {
            using (var connection = server.CreateConnection())
            {
                await connection.WaitForConnectionClose();
            }
        }

        Assert.Single(LogMessages, message => message.LogLevel == LogLevel.Error
            && message.EventId.Id == 0
            && message.Message == expectedErrorMessage);
    }

    private static ListenOptions CreateListenOptions(HttpProtocols protocols)
        => new(new IPEndPoint(IPAddress.Loopback, 0))
        {
            Protocols = protocols
        };
}
