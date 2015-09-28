// --------------------------------------------------------------------------------------------------------------------
// <copyright file="MongoSinkBufferingIntervalTests.cs" company="TIMEmSYSTEM ApS">
//   © TIMEmSYSTEM 2015
// </copyright>
// <summary>
//   The mongo sink tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using MongoDB.Driver;
using Moq;
using NUnit.Framework;

namespace TIMEmSYSTEM.SemanticLogging.Mongo.Tests
{
    /// <summary>
    ///     The mongo sink buffering interval tests.
    /// </summary>
    [TestFixture]
    public class MongoSinkBufferingIntervalTests
    {
        /// <summary>
        ///     The set up.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _clientMock.Setup(x => x.GetDatabase(InstanceName, null)).Returns(() => _databaseMock.Object);
            _databaseMock.Setup(x => x.GetCollection<EventEntry>(CollectionName, null))
                .Returns(() => _collectionMock.Object);

            _sink = new MongoSink(
                _clientMock.Object,
                InstanceName,
                CollectionName,
                _bufferingInterval,
                int.MaxValue,
                int.MaxValue,
                TimeSpan.Zero);

            _eventListener = new ObservableEventListener();
            _eventListener.EnableEvents(TestEventSource.EventSource, EventLevel.LogAlways);
            _subscription = _eventListener.Subscribe(_sink);
        }

        /// <summary>
        ///     The tear down.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            _eventListener.DisableEvents(TestEventSource.EventSource);
            _eventListener.Dispose();
            _subscription.Dispose();
            _collectionMock.Reset();
        }

        /// <summary>
        ///     The mongo client.
        /// </summary>
        private readonly Mock<IMongoClient> _clientMock = new Mock<IMongoClient>();

        /// <summary>
        ///     The mongo database.
        /// </summary>
        private readonly Mock<IMongoDatabase> _databaseMock = new Mock<IMongoDatabase>();

        /// <summary>
        ///     The mongo collection.
        /// </summary>
        private readonly Mock<IMongoCollection<EventEntry>> _collectionMock = new Mock<IMongoCollection<EventEntry>>();

        /// <summary>
        ///     The buffering interval.
        /// </summary>
        private readonly TimeSpan _bufferingInterval = TimeSpan.FromMilliseconds(1);

        /// <summary>
        ///     The sink.
        /// </summary>
        private IObserver<EventEntry> _sink;

        /// <summary>
        ///     The event listener.
        /// </summary>
        private ObservableEventListener _eventListener;

        /// <summary>
        ///     The instance name
        /// </summary>
        public const string InstanceName = "slab";

        /// <summary>
        ///     The collection name
        /// </summary>
        public const string CollectionName = "events";

        /// <summary>
        ///     The subscription.
        /// </summary>
        private IDisposable _subscription;

        [Test]
        public void WhenNoEventsShouldNeverCallInsertMany()
        {
            Thread.Sleep(TimeSpan.FromSeconds(1));
            _collectionMock.Verify(
                x =>
                    x.InsertManyAsync(
                        It.Is<IEnumerable<EventEntry>>(events => events.All(@event => @event.EventId == 1)), null,
                        CancellationToken.None), Times.Never);
        }

        /// <summary>
        ///     Test should not insert events into collection when event list is empty.
        /// </summary>
        [Test]
        public void WhenNoEventsShouldNeverCallInsertOne()
        {
            Thread.Sleep(TimeSpan.FromSeconds(1));
            _collectionMock.Verify(
                x => x.InsertOneAsync(It.Is<EventEntry>(@event => @event.EventId == 1), CancellationToken.None),
                Times.Never);
        }

        /// <summary>
        ///     Test should insert one event into collection.
        /// </summary>
        [Test]
        public void WhenOneEventShouldCallInsertOneOnce()
        {
            TestEventSource.EventSource.TestEvent();
            Thread.Sleep(TimeSpan.FromSeconds(1));
            _collectionMock.Verify(
                x => x.InsertOneAsync(It.Is<EventEntry>(@event => @event.EventId == 1), CancellationToken.None),
                Times.Once);
        }

        [Test]
        public void WhenOneEventShouldNeverCallInsertMany()
        {
            TestEventSource.EventSource.TestEvent();
            Thread.Sleep(TimeSpan.FromSeconds(1));
            _collectionMock.Verify(
                x =>
                    x.InsertManyAsync(
                        It.Is<IEnumerable<EventEntry>>(events => events.All(@event => @event.EventId == 1)), null,
                        CancellationToken.None), Times.Never);
        }

        [Test]
        public void WhenSeveralEventsShouldCallInsertMany()
        {
            for (byte i = 0; i < byte.MaxValue; i++)
            {
                TestEventSource.EventSource.TestEvent();
            }
            Thread.Sleep(TimeSpan.FromSeconds(1));
            _collectionMock.Verify(
                x =>
                    x.InsertManyAsync(
                        It.Is<IEnumerable<EventEntry>>(events => events.All(@event => @event.EventId == 1)), null,
                        CancellationToken.None), Times.Once);
        }

        /// <summary>
        ///     Test should insert several events into collection.
        /// </summary>
        [Test]
        public void WhenSeveralEventsShouldNotCallInsertOne()
        {
            for (byte i = 0; i < byte.MaxValue; i++)
            {
                TestEventSource.EventSource.TestEvent();
            }
            Thread.Sleep(TimeSpan.FromSeconds(1));
            _collectionMock.Verify(
                x => x.InsertOneAsync(It.Is<EventEntry>(@event => @event.EventId == 1), CancellationToken.None),
                Times.Never);
        }
    }
}