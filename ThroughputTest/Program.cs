//---------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.  
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE. 
//---------------------------------------------------------------------------------

namespace ThroughputTest
{
    using CommandLine;
    using System;
    using System.Linq;
    using Azure.Messaging.ServiceBus;
    using System.Threading;

    class Program
    {
        static int result = 0;
        static int Main(params string[] args)
        {
            CommandLine.Parser.Default.ParseArguments<Settings>(args)
                 .WithParsed<Settings>(opts => RunOptionsAndReturnExitCode(opts));
            return result;
        }
        
        static void RunOptionsAndReturnExitCode(Settings settings)
        {
            // Validate authentication parameters
            if (string.IsNullOrWhiteSpace(settings.ServiceBusNamespace) && string.IsNullOrWhiteSpace(settings.ConnectionString))
            {
                Console.WriteLine("Error: Either --sb-namespace or --connection-string must be provided.");
                result = 1;
                return;
            }

            if (!string.IsNullOrWhiteSpace(settings.ServiceBusNamespace) && !string.IsNullOrWhiteSpace(settings.ConnectionString))
            {
                Console.WriteLine("Error: --sb-namespace and --connection-string cannot be used together. Please provide one or the other.");
                result = 1;
                return;
            }

            // Handle ServiceBus namespace authentication (Entra ID)
            if (!string.IsNullOrWhiteSpace(settings.ServiceBusNamespace))
            {
                // When using namespace auth, SendPath must be explicitly set
                if (string.IsNullOrWhiteSpace(settings.SendPath))
                {
                    Console.WriteLine("--send-path option must be specified when using --sb-namespace.");
                    result = 1;
                    return;
                }
            }
            else // Handle connection string authentication
            {
                ServiceBusConnectionStringProperties cb = ServiceBusConnectionStringProperties.Parse(settings.ConnectionString);
                if (string.IsNullOrWhiteSpace(cb.EntityPath))
                {
                    if (string.IsNullOrWhiteSpace(settings.SendPath))
                    {
                        Console.WriteLine("--send-path option must be specified if there's no EntityPath in the connection string.");
                        result = 1;
                        return;
                    }
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(settings.SendPath))
                    {
                        settings.SendPath = cb.EntityPath;
                    }
                    
                    settings.ConnectionString = cb.ToString();
                }
            }
            if (settings.ReceivePaths == null || settings.ReceivePaths.Count() == 0)
            {
                settings.ReceivePaths = new string[] { settings.SendPath };
            }
            Console.WriteLine("\n\nPress <ENTER> to STOP at anytime\n");
            Metrics metrics = new Metrics(settings);
            ServiceBusPerformanceApp app = new ServiceBusPerformanceApp(settings, metrics);
            var experiments = new Experiment[]
            {
                // new IncreaseInflightSendsExperiment(50, metrics, settings),
                // new IncreaseInflightReceivesExperiment(10, metrics, settings)
            };
            app.Run(experiments).Wait();
            Console.WriteLine("Complete");
            
        }
    }
}
