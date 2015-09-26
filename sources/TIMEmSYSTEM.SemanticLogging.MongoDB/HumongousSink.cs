// --------------------------------------------------------------------------------------------------------------------
// <copyright file="HumongousSink.cs" company="">
//   
// </copyright>
// <summary>
//   The humongous sink.
// </summary>
// --------------------------------------------------------------------------------------------------------------------
namespace TIMEmSYSTEM.SemanticLogging.MongoDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;

    using global::MongoDB.Driver;

    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;

    /// <summary>
    /// The humongous sink.
    /// </summary>
    public class HumongousSink : IObserver<EventEntry>, IDisposable
    {
        /// <summary>
        /// The _buffered publisher.
        /// </summary>
        private readonly BufferedEventPublisher<EventEntry> bufferedPublisher;

        /// <summary>
        /// The _cancellation token source.
        /// </summary>
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        /// <summary>
        /// The _on completed timeout.
        /// </summary>
        private readonly TimeSpan onCompletedTimeout;

        /// <summary>
        /// The collection.
        /// </summary>
        private readonly IMongoCollection<EventEntry> collection;

        /// <summary>
        /// Initializes a new instance of the <see cref="HumongousSink"/> class.
        /// </summary>
        /// <param name="connectionString">
        /// The connection string.
        /// </param>
        /// <param name="instanceName">
        /// The instance name.
        /// </param>
        /// <param name="collectionName">
        /// The collection name.
        /// </param>
        /// <param name="bufferingInterval">
        /// The buffering interval.
        /// </param>
        /// <param name="bufferingCount">
        /// The buffering count.
        /// </param>
        /// <param name="maxBufferSize">
        /// The max buffer size.
        /// </param>
        /// <param name="onCompletedTimeout">
        /// The on completed timeout.
        /// </param>
        public HumongousSink(
            string connectionString, 
            string instanceName, 
            string collectionName, 
            TimeSpan bufferingInterval, 
            int bufferingCount, 
            int maxBufferSize, 
            TimeSpan onCompletedTimeout)
            : this(
                new MongoClient(connectionString), 
                instanceName, 
                collectionName, 
                bufferingInterval, 
                bufferingCount, 
                maxBufferSize, 
                onCompletedTimeout)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HumongousSink"/> class.
        /// </summary>
        /// <param name="client">
        /// The client.
        /// </param>
        /// <param name="instanceName">
        /// The instance name.
        /// </param>
        /// <param name="collectionName">
        /// The collection name.
        /// </param>
        /// <param name="bufferingInterval">
        /// The buffering interval.
        /// </param>
        /// <param name="bufferingCount">
        /// The buffering count.
        /// </param>
        /// <param name="maxBufferSize">
        /// The max buffer size.
        /// </param>
        /// <param name="onCompletedTimeout">
        /// The on completed timeout.
        /// </param>
        public HumongousSink(
            IMongoClient client, 
            string instanceName, 
            string collectionName, 
            TimeSpan bufferingInterval, 
            int bufferingCount, 
            int maxBufferSize, 
            TimeSpan onCompletedTimeout)
        {
            this.onCompletedTimeout = onCompletedTimeout;
            this.bufferedPublisher = BufferedEventPublisher<EventEntry>.CreateAndStart(
                "mongodb", 
                this.PublishEventsAsync, 
                bufferingInterval, 
                bufferingCount, 
                maxBufferSize, 
                this.cancellationTokenSource.Token);
            this.collection = client.GetDatabase(instanceName).GetCollection<EventEntry>(collectionName);
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="HumongousSink"/> class. 
        /// </summary>
        ~HumongousSink()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// The dispose.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// The on completed.
        /// </summary>
        public void OnCompleted()
        {
            this.FlushSafe();
            this.Dispose();
        }

        /// <summary>
        /// The on error.
        /// </summary>
        /// <param name="error">
        /// The error.
        /// </param>
        public void OnError(Exception error)
        {
            this.FlushSafe();
            this.Dispose();
        }

        /// <summary>
        /// The on next.
        /// </summary>
        /// <param name="value">
        /// The value.
        /// </param>
        public void OnNext(EventEntry value)
        {
            this.bufferedPublisher.TryPost(value);
        }

        /// <summary>
        /// The dispose.
        /// </summary>
        /// <param name="disposing">
        /// The disposing.
        /// </param>
        [SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed", 
            MessageId = "cancellationTokenSource", Justification = "Token is cancelled")]
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.cancellationTokenSource.Cancel();
                this.bufferedPublisher.Dispose();
            }
        }

        /// <summary>
        /// The flush async.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        private Task FlushAsync()
        {
            return this.bufferedPublisher.FlushAsync();
        }

        /// <summary>
        /// The flush safe.
        /// </summary>
        private void FlushSafe()
        {
            try
            {
                this.FlushAsync().Wait(this.onCompletedTimeout);
            }
            catch (AggregateException ex)
            {
                ex.Handle(e => e is FlushFailedException);
            }
        }

        /// <summary>
        /// The publish events async.
        /// </summary>
        /// <param name="eventEntries">
        /// The event entries.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        private async Task<int> PublishEventsAsync(IList<EventEntry> eventEntries)
        {
            if (eventEntries.Count == 1)
            {
                await this.collection.InsertOneAsync(eventEntries[0]).ConfigureAwait(false);
            }
            else
            {
                await this.collection.InsertManyAsync(eventEntries).ConfigureAwait(false);
            }

            return eventEntries.Count;
        }
    }
}