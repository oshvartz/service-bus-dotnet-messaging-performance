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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.ServiceBus;
    using Azure.Identity;

    sealed class ReceiverTask : PerformanceTask
    {
        readonly List<Task> receivers;

        public ReceiverTask(Settings settings, Metrics metrics, CancellationToken cancellationToken)
            : base(settings, metrics, cancellationToken)
        {
            this.receivers = new List<Task>();
        }

        protected override Task OnOpenAsync()
        {
            return Task.CompletedTask;
        }

        protected override Task OnStartAsync()
        {
            var receiverPaths = this.Settings.ReceivePaths;
            foreach (var receiverPath in receiverPaths)
            {
                if (this.Settings.ReceiverCount == Settings.RECVICER_PROCESSOR_MODE) 
                {
                    this.receivers.Add(Task.Run(() => ReceiveTask(receiverPath)));

                }
                else
                {
                    for (int i = 0; i < this.Settings.ReceiverCount; i++)
                    {
                        this.receivers.Add(Task.Run(() => ReceiveTask(receiverPath)));
                    }
                }
            }
            return Task.WhenAll(this.receivers);
        }

        async Task ReceiveTask(string path)
        {
            ServiceBusClient client;
            if (!string.IsNullOrWhiteSpace(this.Settings.ServiceBusNamespace))
            {
                // Use Entra ID authentication with DefaultAzureCredential
                client = new ServiceBusClient(this.Settings.ServiceBusNamespace, new DefaultAzureCredential());
            }
            else
            {
                // Use connection string authentication
                client = new ServiceBusClient(this.Settings.ConnectionString);
            }
            
            var options = new ServiceBusReceiverOptions();
            options.ReceiveMode = this.Settings.ReceiveMode;
            options.PrefetchCount = Settings.PrefetchCount;

            var semaphore = new DynamicSemaphoreSlim(this.Settings.MaxInflightReceives.Value + 1);
            var done = new SemaphoreSlim(1); done.Wait();
            var sw = Stopwatch.StartNew();
            long totalReceives = 0;
            await Task.Delay(TimeSpan.FromMilliseconds(Settings.WorkDuration));
            this.Settings.MaxInflightReceives.Changing += (a, e) => AdjustSemaphore(e, semaphore);

            if (Settings.ReceiverCount == -1)
            {
                var serviceBusProcessorOptions = new ServiceBusProcessorOptions
                {
                    MaxConcurrentCalls = Settings.CfgMaxInflightReceives,
                    ReceiveMode = Settings.ReceiveMode,
                    MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(1),
                };

                var processor = client.CreateProcessor(path, serviceBusProcessorOptions);
                processor.ProcessMessageAsync += Processor_ProcessMessageAsync;
                processor.ProcessErrorAsync += Processor_ProcessErrorAsync;

                await processor.StartProcessingAsync().ConfigureAwait(false);

            }
            else
            {
                ServiceBusReceiver receiver = client.CreateReceiver(path, options);
                for (int j = 0; (Settings.MessageCount == -1 || j < Settings.MessageCount) && !this.CancellationToken.IsCancellationRequested; j++)
                {
                    var receiveMetrics = new ReceiveMetrics() { Tick = sw.ElapsedTicks };

                    var nsec = sw.ElapsedTicks;
                    await semaphore.WaitAsync().ConfigureAwait(false);
                    receiveMetrics.GateLockDuration100ns = sw.ElapsedTicks - nsec;

                    if (Settings.ReceiveBatchCount <= 1)
                    {

                        nsec = sw.ElapsedTicks;

                        try
                        {
                            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                            receiveMetrics.ReceiveDuration100ns = sw.ElapsedTicks - nsec;
                            receiveMetrics.Receives = receiveMetrics.Messages = 1;
                            nsec = sw.ElapsedTicks;
                            // simulate work by introducing a delay, if needed
                            if (msg != null)
                            {
                                if (Settings.WorkDuration > 0)
                                {
                                    await Task.Delay(TimeSpan.FromMilliseconds(Settings.WorkDuration)).ConfigureAwait(false);
                                    await receiver.RenewMessageLockAsync(msg).ConfigureAwait(false);
                                }

                                await receiver.CompleteMessageAsync(msg).ConfigureAwait(false);
                            }
                            Metrics.PushReceiveMetrics(receiveMetrics);
                            semaphore.Release();
                            if (Settings.MessageCount != -1 && Interlocked.Increment(ref totalReceives) >= Settings.MessageCount)
                            {
                                done.Release();
                            }
                        }
                        catch (Exception ex)
                        {
                            receiveMetrics.ReceiveDuration100ns = sw.ElapsedTicks - nsec;
                            if (ex is ServiceBusException sbException && sbException.Reason == ServiceBusFailureReason.ServiceBusy)
                            {
                                receiveMetrics.BusyErrors = 1;
                                if (!this.CancellationToken.IsCancellationRequested)
                                {
                                    await Task.Delay(3000, this.CancellationToken).ConfigureAwait(false);
                                }
                            }
                            else
                            {
                                receiveMetrics.Errors = 1;
                            }
                            Metrics.PushReceiveMetrics(receiveMetrics);
                            semaphore.Release();
                            if (Settings.MessageCount != -1 && Interlocked.Increment(ref totalReceives) >= Settings.MessageCount)
                            {
                                done.Release();
                            }

                        }
                    }
                    //TODO fix batch receive
                    else
                    {
                        nsec = sw.ElapsedTicks;
                        // we're going to unblock the receives after 10 seconds if there's no pending message
                        receiver.ReceiveMessagesAsync(Settings.ReceiveBatchCount, TimeSpan.FromSeconds(10)).ContinueWith(async (t) =>
                        {
                            receiveMetrics.ReceiveDuration100ns = sw.ElapsedTicks - nsec;
                            if (t.IsFaulted || t.IsCanceled || t.Result == null)
                            {
                                if ((Exception)t.Exception is ServiceBusException sbException && sbException.Reason == ServiceBusFailureReason.ServiceBusy)
                                {
                                    receiveMetrics.BusyErrors = 1;
                                    if (!this.CancellationToken.IsCancellationRequested)
                                    {
                                        await Task.Delay(3000, this.CancellationToken).ConfigureAwait(false);
                                    }
                                }
                                else
                                {
                                    receiveMetrics.Errors = 1;
                                }
                                Metrics.PushReceiveMetrics(receiveMetrics);
                                semaphore.Release();
                                if (Interlocked.Increment(ref totalReceives) >= Settings.MessageCount)
                                {
                                    done.Release();
                                }
                            }
                            else
                            {
                                receiveMetrics.Messages = t.Result.Count;
                                receiveMetrics.Receives = 1;
                                nsec = sw.ElapsedTicks;
                                if (Settings.ReceiveMode == ServiceBusReceiveMode.PeekLock)
                                {
                                    if (Settings.WorkDuration > 0)
                                    {
                                        // handle completes singly
                                        for (int i = 0; i < t.Result.Count; i++)
                                        {
                                            await Task.Delay(TimeSpan.FromMilliseconds(Settings.WorkDuration));
                                            await receiver.CompleteMessageAsync(t.Result[i]).ContinueWith(async (t1) =>
                                            {
                                                receiveMetrics.CompleteDuration100ns = sw.ElapsedTicks - nsec;
                                                if (t1.IsFaulted)
                                                {
                                                    if ((Exception)t1.Exception is ServiceBusException sbException && sbException.Reason == ServiceBusFailureReason.ServiceBusy)
                                                    {
                                                        receiveMetrics.BusyErrors = 1;
                                                        if (!this.CancellationToken.IsCancellationRequested)
                                                        {
                                                            await Task.Delay(3000, this.CancellationToken).ConfigureAwait(false);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        receiveMetrics.Errors = 1;
                                                    }
                                                }
                                                else
                                                {
                                                    receiveMetrics.CompleteCalls = 1;
                                                    receiveMetrics.Completions = t.Result.Count;
                                                }
                                                Metrics.PushReceiveMetrics(receiveMetrics);
                                                semaphore.Release();
                                                if (Interlocked.Increment(ref totalReceives) >= Settings.MessageCount)
                                                {
                                                    done.Release();
                                                }
                                            });
                                        }
                                    }
                                    else
                                    {
                                        // batch complete
                                        await receiver.CompleteMessageAsync((ServiceBusReceivedMessage)t.Result.Select((m) => { return m; })).ContinueWith(async (t1) =>
                                        {
                                            receiveMetrics.CompleteDuration100ns = sw.ElapsedTicks - nsec;
                                            if (t1.IsFaulted)
                                            {
                                                if ((Exception)t1.Exception is ServiceBusException sbException && sbException.Reason == ServiceBusFailureReason.ServiceBusy)
                                                {
                                                    receiveMetrics.BusyErrors = 1;
                                                    if (!this.CancellationToken.IsCancellationRequested)
                                                    {
                                                        await Task.Delay(3000, this.CancellationToken).ConfigureAwait(false);
                                                    }
                                                }
                                                else
                                                {
                                                    receiveMetrics.Errors = 1;
                                                }
                                            }
                                            else
                                            {
                                                receiveMetrics.CompleteCalls = 1;
                                                receiveMetrics.Completions = t.Result.Count;
                                            }
                                            Metrics.PushReceiveMetrics(receiveMetrics);
                                            semaphore.Release();

                                            // count all the messages
                                            for (int k = 0; k < t.Result.Count; k++)
                                            {
                                                if (Interlocked.Increment(ref totalReceives) >= Settings.MessageCount)
                                                {
                                                    done.Release();
                                                }
                                            }
                                        });
                                    }
                                }
                                else
                                {
                                    if (Settings.WorkDuration > 0)
                                    {
                                        await Task.Delay(TimeSpan.FromMilliseconds(Settings.WorkDuration));
                                    }
                                    Metrics.PushReceiveMetrics(receiveMetrics);
                                    semaphore.Release();
                                    if (Interlocked.Increment(ref totalReceives) >= Settings.MessageCount)
                                    {
                                        done.Release();
                                    }
                                }
                            }
                        }).Fork();

                    }
                }
            }
            await done.WaitAsync();
        }

        private Task Processor_ProcessErrorAsync(ProcessErrorEventArgs args)
        {
            var receiveMetrics = new ReceiveMetrics
            {
                Errors = 1
            };
            Metrics.PushReceiveMetrics(receiveMetrics);
            return Task.CompletedTask;
        }

        private async Task Processor_ProcessMessageAsync(ProcessMessageEventArgs arg)
        {
            var receiveMetrics = new ReceiveMetrics
            {
                CompleteCalls = 1
            };
            if(Settings.WorkDuration > 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(Settings.WorkDuration));
            }   
            await arg.CompleteMessageAsync(arg.Message).ConfigureAwait(false);

            Metrics.PushReceiveMetrics(receiveMetrics);
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
    }
}
