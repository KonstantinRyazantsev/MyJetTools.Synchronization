using System.Collections.Generic;

namespace MyJetTools.Synchronization.PublishSubscribe
{
    public interface IPublisher<in T>
    {
        void Put(T item);
        void PutRange(IEnumerable<T> item);
    }
}