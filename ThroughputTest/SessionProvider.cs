using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ThroughputTest
{
    public class SessionProvider
    {
        private List<string> _sessions = new List<string>();

        public SessionProvider(int sessionsNum)
        {
            IsEnabled = sessionsNum > 0;
            _sessions = Enumerable.Range(0, sessionsNum).Select(i => Guid.NewGuid().ToString()).ToList();
        }

        public string GetSession()
        {
            return _sessions[new Random().Next(0, _sessions.Count)];
        }

        public bool IsEnabled { get; init; }
    }
}
