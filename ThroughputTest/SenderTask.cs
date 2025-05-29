//---------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.  
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE. 
//---------------------------------------------------------------------------------

namespace ThroughputTest
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Net.Sockets;
    using Azure.Messaging.ServiceBus;
    using Azure.Identity;

    sealed class SenderTask : PerformanceTask
    {
        readonly List<Task> senders;
        //private readonly ServiceBusClient serviceBusClient;
        private readonly ServiceBusSender sender;
        private readonly byte[] payload;
        private readonly SessionProvider sessionProvider;

        public SenderTask(Settings settings, Metrics metrics, SessionProvider sessionProvider, CancellationToken cancellationToken)
            : base(settings, metrics, cancellationToken)
        {
            this.senders = new List<Task>();
            
            ServiceBusClient serviceBusClient;
            if (!string.IsNullOrWhiteSpace(settings.ServiceBusNamespace))
            {
                // Use Entra ID authentication with DefaultAzureCredential
                serviceBusClient = new ServiceBusClient(settings.ServiceBusNamespace, new DefaultAzureCredential());
            }
            else
            {
                // Use connection string authentication
                serviceBusClient = new ServiceBusClient(settings.ConnectionString);
            }
            
            sender = serviceBusClient.CreateSender(settings.SendPath);
            payload = new byte[settings.MessageSizeInBytes];
            this.sessionProvider = sessionProvider;
        }

        protected override Task OnOpenAsync()
        {
            return Task.CompletedTask;
        }

        protected override Task OnStartAsync()
        {
            for (int i = 0; i < this.Settings.SenderCount; i++)
            {
                this.senders.Add(Task.Run(SendTask));
            }
            return Task.WhenAll(senders);
        }

        async Task SendTask()
        {
            var semaphore = new DynamicSemaphoreSlim(this.Settings.MaxInflightSends.Value);
            var done = new SemaphoreSlim(1);
            done.Wait();
            long totalSends = 0;

            this.Settings.MaxInflightSends.Changing += (a, e) => AdjustSemaphore(e, semaphore);
            var sw = Stopwatch.StartNew();

            // first send will fail out if the cxn string is bad
            await sender.SendMessageAsync(CreateMessage());

            for (int j = 0; (Settings.MessageCount == -1 || j < Settings.MessageCount) && !this.CancellationToken.IsCancellationRequested; j++)
            {
                var sendMetrics = new SendMetrics() { Tick = sw.ElapsedTicks };

                var nsec = sw.ElapsedTicks;
                semaphore.Wait();
                sendMetrics.InflightSends = this.Settings.MaxInflightSends.Value - semaphore.CurrentCount;
                sendMetrics.GateLockDuration100ns = sw.ElapsedTicks - nsec;

                if (Settings.SendDelay > 0)
                {
                    await Task.Delay(Settings.SendDelay);
                }
                if (Settings.SendBatchCount <= 1)
                {
                    try
                    {
                        await sender.SendMessageAsync(CreateMessage());
                        sendMetrics.SendDuration100ns = sw.ElapsedTicks - nsec;
                        sendMetrics.Sends = 1;
                        sendMetrics.Messages = 1;
                        semaphore.Release();
                        Metrics.PushSendMetrics(sendMetrics);
                        if (Settings.MessageCount != -1 && Interlocked.Increment(ref totalSends) >= Settings.MessageCount)
                        {
                            done.Release();
                        }
                    }

                    catch (Exception ex)
                    {
                        await HandleExceptions(semaphore, sendMetrics, new AggregateException(ex));
                    }
                }
                else
                {
                    var batch = new List<ServiceBusMessage>();
                    for (int i = 0; i < Settings.SendBatchCount && (Settings.MessageCount == -1 || j < Settings.MessageCount) && !this.CancellationToken.IsCancellationRequested; i++, j++)
                    {
                        batch.Add(CreateMessage());
                    }
                    try
                    {
                        await sender.SendMessagesAsync(batch);
                        sendMetrics.SendDuration100ns = sw.ElapsedTicks - nsec;
                        sendMetrics.Sends = 1;
                        sendMetrics.Messages = Settings.SendBatchCount;
                        semaphore.Release();
                        Metrics.PushSendMetrics(sendMetrics);
                        if (Settings.MessageCount != -1 && Interlocked.Increment(ref totalSends) >= Settings.MessageCount)
                        {
                            done.Release();
                        }
                    }
                    catch (Exception ex)
                    {
                        await HandleExceptions(semaphore, sendMetrics, new AggregateException(ex));
                    }

                }
            }
            await done.WaitAsync();
        }

        private ServiceBusMessage CreateMessage()
        {
            if (Settings.MessageTimeToLiveMinutes != 0)
            {
                return new ServiceBusMessage(payload) { TimeToLive = TimeSpan.FromMinutes(Settings.MessageTimeToLiveMinutes), SessionId = this.sessionProvider.IsEnabled ? this.sessionProvider.GetSession() : null };
            }
            else
            {
                return new ServiceBusMessage(payload) { SessionId = this.sessionProvider.IsEnabled ? this.sessionProvider.GetSession() : null };
            }
        }

        static void AdjustSemaphore(Observable<int>.ChangingEventArgs e, DynamicSemaphoreSlim semaphore)
        {
            if (e.NewValue > e.OldValue)
            {
                for (int i = e.OldValue; i < e.NewValue; i++)
                {
                    semaphore.Grant();
                }
            }
            else
            {
                for (int i = e.NewValue; i < e.OldValue; i++)
                {
                    semaphore.Revoke();
                }
            }
        }

        private async Task HandleExceptions(DynamicSemaphoreSlim semaphore, SendMetrics sendMetrics, AggregateException ex)
        {
            bool wait = false;
            ex.Handle((x) =>
            {
                if (x is ServiceBusException sbException)
                {
                    if (sbException.Reason == ServiceBusFailureReason.ServiceCommunicationProblem)
                    {
                        if (sbException.InnerException is SocketException socketException &&
                        socketException.SocketErrorCode == SocketError.HostNotFound)
                        {
                            return false;
                        }
                    }
                    if (sbException.Reason == ServiceBusFailureReason.ServiceBusy)
                    {
                        sendMetrics.BusyErrors = 1;
                        if (!this.CancellationToken.IsCancellationRequested)
                        {
                            wait = true;
                        }
                    }
                    else
                    {
                        sendMetrics.Errors = 1;
                    }
                }
                return true;
            });

            if (wait)
            {
                await Task.Delay(3000, this.CancellationToken).ConfigureAwait(false);
            }
            semaphore.Release();
            Metrics.PushSendMetrics(sendMetrics);

        }
    }
}
