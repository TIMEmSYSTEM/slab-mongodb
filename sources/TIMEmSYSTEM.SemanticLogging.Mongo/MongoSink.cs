// --------------------------------------------------------------------------------------------------------------------
// <copyright file="MongoSink.cs" company="TIMEmSYSTEM ApS">
//   © TIMEmSYSTEM 2015
// </copyright>
// <summary>
//   The mongo sink.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
using MongoDB.Bson;
using MongoDB.Driver;

namespace TIMEmSYSTEM.SemanticLogging.Mongo
{
    /// <summary>
    ///     The mongo sink.
    /// </summary>
    public class MongoSink : IObserver<EventEntry>, IDisposable
    {
        /// <summary>
        ///     The _buffered publisher.
        /// </summary>
        private readonly BufferedEventPublisher<EventEntry> _bufferedPublisher;

        /// <summary>
        ///     The _cancellation token source.
        /// </summary>
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        /// <summary>
        ///     The collection.
        /// </summary>
        private readonly IMongoCollection<BsonDocument> _collection;

        /// <summary>
        ///     The _on completed timeout.
        /// </summary>
        private readonly TimeSpan _onCompletedTimeout;

        /// <summary>
        ///     Initializes a new instance of the <see cref="MongoSink" /> class.
        /// </summary>
        /// <param name="connectionString">
        ///     The connection string.
        /// </param>
        /// <param name="instanceName">
        ///     The instance name.
        /// </param>
        /// <param name="collectionName">
        ///     The collection name.
        /// </param>
        /// <param name="bufferingInterval">
        ///     The buffering interval.
        /// </param>
        /// <param name="bufferingCount">
        ///     The buffering count.
        /// </param>
        /// <param name="maxBufferSize">
        ///     The max buffer size.
        /// </param>
        /// <param name="onCompletedTimeout">
        ///     The on completed timeout.
        /// </param>
        public MongoSink(
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
        ///     Initializes a new instance of the <see cref="MongoSink" /> class.
        /// </summary>
        /// <param name="client">
        ///     The client.
        /// </param>
        /// <param name="instanceName">
        ///     The instance name.
        /// </param>
        /// <param name="collectionName">
        ///     The collection name.
        /// </param>
        /// <param name="bufferingInterval">
        ///     The buffering interval.
        /// </param>
        /// <param name="bufferingCount">
        ///     The buffering count.
        /// </param>
        /// <param name="maxBufferSize">
        ///     The max buffer size.
        /// </param>
        /// <param name="onCompletedTimeout">
        ///     The on completed timeout.
        /// </param>
        public MongoSink(
            IMongoClient client,
            string instanceName,
            string collectionName,
            TimeSpan bufferingInterval,
            int bufferingCount,
            int maxBufferSize,
            TimeSpan onCompletedTimeout)
        {
            _onCompletedTimeout = onCompletedTimeout;
            _bufferedPublisher = BufferedEventPublisher<EventEntry>.CreateAndStart(
                "mongodb",
                PublishEventsAsync,
                bufferingInterval,
                bufferingCount,
                maxBufferSize,
                _cancellationTokenSource.Token);
            _collection = client.GetDatabase(instanceName).GetCollection<BsonDocument>(collectionName);
        }

        /// <summary>
        ///     The dispose.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///     The on completed.
        /// </summary>
        public void OnCompleted()
        {
            FlushSafe();
            Dispose();
        }

        /// <summary>
        ///     The on error.
        /// </summary>
        /// <param name="error">
        ///     The error.
        /// </param>
        public void OnError(Exception error)
        {
            FlushSafe();
            Dispose();
        }

        /// <summary>
        ///     The on next.
        /// </summary>
        /// <param name="value">
        ///     The value.
        /// </param>
        public void OnNext(EventEntry value)
        {
            _bufferedPublisher.TryPost(value);
        }

        /// <summary>
        ///     Finalizes an instance of the <see cref="MongoSink" /> class.
        /// </summary>
        ~MongoSink()
        {
            Dispose(false);
        }

        /// <summary>
        ///     The dispose.
        /// </summary>
        /// <param name="disposing">
        ///     The disposing.
        /// </param>
        [SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed",
            MessageId = "cancellationTokenSource", Justification = "Token is cancelled")]
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _cancellationTokenSource.Cancel();
                _bufferedPublisher.Dispose();
            }
        }

        /// <summary>
        ///     The flush async.
        /// </summary>
        /// <returns>
        ///     The <see cref="Task" />.
        /// </returns>
        private Task FlushAsync()
        {
            return _bufferedPublisher.FlushAsync();
        }

        /// <summary>
        ///     The flush safe.
        /// </summary>
        private void FlushSafe()
        {
            try
            {
                FlushAsync().Wait(_onCompletedTimeout);
            }
            catch (AggregateException ex)
            {
                ex.Handle(e => e is FlushFailedException);
            }
        }

        /// <summary>
        ///     The publish events async.
        /// </summary>
        /// <param name="eventEntries">
        ///     The event entries.
        /// </param>
        /// <returns>
        ///     The <see cref="Task" />.
        /// </returns>
        private async Task<int> PublishEventsAsync(IList<EventEntry> eventEntries)
        {
            if (eventEntries.Count == 1)
            {
                await _collection.InsertOneAsync(eventEntries[0].AsBsonDocument()).ConfigureAwait(false);
            }
            else if (eventEntries.Count > 1)
            {
                await _collection.InsertManyAsync(eventEntries.AsBsonDocuments()).ConfigureAwait(false);
            }

            return eventEntries.Count;
        }
    }
}