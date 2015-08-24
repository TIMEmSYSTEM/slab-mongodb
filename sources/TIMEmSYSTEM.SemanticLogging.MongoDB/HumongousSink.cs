using System;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;

namespace TIMEmSYSTEM.SemanticLogging.MongoDB
{
    public class HumongousSink : IObserver<EventEntry>, IDisposable
    {
        public void OnNext(EventEntry value)
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
