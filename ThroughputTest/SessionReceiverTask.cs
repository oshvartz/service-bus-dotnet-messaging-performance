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

    sealed class SessionReceiverTask : PerformanceTask
    {
        readonly List<Task> receivers;
        readonly ServiceBusClient client;

        public SessionReceiverTask(Settings settings, Metrics metrics, CancellationToken cancellationToken)
            : base(settings, metrics, cancellationToken)
        {
            this.receivers = new List<Task>();
            this.client = new ServiceBusClient(this.Settings.ConnectionString);
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
                //processor mode
                if (Settings.ReceiverCount == -1)
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
            var done = new SemaphoreSlim(1); done.Wait();
            var options = new ServiceBusSessionReceiverOptions
            {
                ReceiveMode = Settings.ReceiveMode,
                PrefetchCount = Settings.PrefetchCount,
            };

            if (Settings.ReceiverCount == -1)
            {
                var serviceBusProcessorOptions = new ServiceBusSessionProcessorOptions
                {
                    //MaxConcurrentCallsPerSession = 1,
                    MaxConcurrentSessions = Settings.MaxInflightReceives.Value,
                    ReceiveMode = Settings.ReceiveMode,
                    PrefetchCount = Settings.PrefetchCount,
                    MaxAutoLockRenewalDuration = TimeSpan.FromMilliseconds(Settings.WorkDuration) + TimeSpan.FromSeconds(5),
                };

                var processor = client.CreateSessionProcessor(path, serviceBusProcessorOptions);
                processor.ProcessMessageAsync += Processor_ProcessMessageAsync;
                processor.ProcessErrorAsync += Processor_ProcessErrorAsync;

                await processor.StartProcessingAsync().ConfigureAwait(false);

            }
            else
            {

                var semaphore = new DynamicSemaphoreSlim(this.Settings.MaxInflightReceives.Value + 1);
                var sw = Stopwatch.StartNew();
                this.Settings.MaxInflightReceives.Changing += (a, e) => AdjustSemaphore(e, semaphore);

                for (int j = 0; (Settings.MessageCount == -1 || j < Settings.MessageCount) && !this.CancellationToken.IsCancellationRequested; j++)
                {
                    var receiveMetrics = new ReceiveMetrics() { Tick = sw.ElapsedTicks };
                    var nsec = sw.ElapsedTicks;

                    receiveMetrics.GateLockDuration100ns = sw.ElapsedTicks - nsec;
                    ServiceBusSessionReceiver serviceBusSessionReceiver = null;
                    try
                    {
                        serviceBusSessionReceiver = await client.AcceptNextSessionAsync(path, options);

                        //todo: add timeout
                        var messages = await serviceBusSessionReceiver.ReceiveMessagesAsync(Settings.ReceiveBatchCount, TimeSpan.FromSeconds(30));

                        receiveMetrics.ReceiveDuration100ns = sw.ElapsedTicks - nsec;
                        receiveMetrics.Receives = receiveMetrics.Messages = 1;
                        nsec = sw.ElapsedTicks;

                        var processTasks = new List<Task>();
                        foreach (var message in messages)
                        {
                            processTasks.Add(ProcessMessage(serviceBusSessionReceiver, message, semaphore));
                        }
                        await Task.WhenAll(processTasks);

                        Metrics.PushReceiveMetrics(receiveMetrics);
                    }
                    catch (Exception ex)
                    {

                        // receiveMetrics.ReceiveDuration100ns = sw.ElapsedTicks - nsec;
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
                    }
                    finally
                    {
                        if (serviceBusSessionReceiver != null)
                        {
                            await serviceBusSessionReceiver.DisposeAsync();
                        }
                    }
                }
            }

            await done.WaitAsync();
        }



        private async Task ProcessMessage(ServiceBusSessionReceiver serviceBusSessionReceiver, ServiceBusReceivedMessage message, DynamicSemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                if (Settings.WorkDuration > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(Settings.WorkDuration)).ConfigureAwait(false);
                }

                await serviceBusSessionReceiver.CompleteMessageAsync(message);
            }
            finally
            {
                semaphore.Release();
            }
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

        private async Task Processor_ProcessMessageAsync(ProcessSessionMessageEventArgs arg)
        {
            var receiveMetrics = new ReceiveMetrics
            {
                CompleteCalls = 1
            };

            var processorTasks = new List<Task>() { ProcessProcessorMessage(arg, arg.Message) };
            var msgs = await arg.GetReceiveActions().ReceiveMessagesAsync(Settings.ReceiveBatchCount - 1, TimeSpan.FromSeconds(30));

            foreach (var msg in msgs)
            {
                receiveMetrics.CompleteCalls++;
                processorTasks.Add(ProcessProcessorMessage(arg,msg));
            }
            await Task.WhenAll(processorTasks);
            Metrics.PushReceiveMetrics(receiveMetrics);
        }


        private async Task ProcessProcessorMessage(ProcessSessionMessageEventArgs serviceBusSessionReceiver, ServiceBusReceivedMessage message)
        {
            if (Settings.WorkDuration > 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(Settings.WorkDuration)).ConfigureAwait(false);
            }
            await serviceBusSessionReceiver.CompleteMessageAsync(message);
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
