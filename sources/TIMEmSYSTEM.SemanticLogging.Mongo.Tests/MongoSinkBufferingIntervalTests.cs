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
using TIMEmSYSTEM.SemanticLogging.MongoDB.Tests;

namespace TIMEmSYSTEM.SemanticLogging.Mongo.Tests
{
    /// <summary>
    ///     The mongo sink buffering interval tests.
    /// </summary>
    [TestFixture]
    public class MongoSinkBufferingIntervalTests
    {
        /// <summary>
        ///     The mongo client.
        /// </summary>
        private readonly Mock<IMongoClient> _client = new Mock<IMongoClient>();

        /// <summary>
        ///     The mongo database.
        /// </summary>
        private readonly Mock<IMongoDatabase> _database = new Mock<IMongoDatabase>();

        /// <summary>
        ///     The mongo collection.
        /// </summary>
        private readonly Mock<IMongoCollection<EventEntry>> _collection = new Mock<IMongoCollection<EventEntry>>();

        /// <summary>
        ///     The buffering interval.
        /// </summary>
        private readonly TimeSpan _bufferingInterval = TimeSpan.FromSeconds(1);

        /// <summary>
        ///     The sink.
        /// </summary>
        private IObserver<EventEntry> _sink;

        /// <summary>
        /// The event listener.
        /// </summary>
        private ObservableEventListener _eventListener;

        /// <summary>
        /// The instance name
        /// </summary>
        public const string InstanceName = "slab";

        /// <summary>
        /// The collection name
        /// </summary>
        public const string CollectionName = "events";

        /// <summary>
        ///     The set up.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            _client.Setup(x => x.GetDatabase(InstanceName, null)).Returns(() => _database.Object);
            _database.Setup(x => x.GetCollection<EventEntry>(CollectionName, null)).Returns(() => _collection.Object);
            _sink = new MongoSink(
                _client.Object,
                InstanceName,
                CollectionName, 
                _bufferingInterval, 
                byte.MaxValue, 
                int.MaxValue, 
                TimeSpan.Zero);
            _eventListener = new ObservableEventListener();
            _eventListener.EnableEvents(TestEventSource.EventSource, EventLevel.LogAlways);
            _eventListener.Subscribe(_sink);
        }

        /// <summary>
        /// Test should insert one event into collection.
        /// </summary>
        [Test]
        public void ShouldInsertOneEventIntoCollection()
        {
            TestEventSource.EventSource.TestEvent();
            Thread.Sleep(_bufferingInterval.Add(TimeSpan.FromMilliseconds(1)));
            _collection.Verify(x => x.InsertOneAsync(It.Is<EventEntry>(@event => @event.EventId == 1), CancellationToken.None), Times.Once);
            _collection.Verify(x => x.InsertManyAsync(It.Is<IEnumerable<EventEntry>>(events => events.All(@event => @event.EventId == 1)), null, CancellationToken.None), Times.Never);
        }
        
        /// <summary>
        /// Test should insert several events into collection.
        /// </summary>
        [Test]
        public void ShouldInsertSeveralEventsIntoCollection()
        {
            for (byte i = 0; i < byte.MaxValue; i++)
            {
                TestEventSource.EventSource.TestEvent();
            }

            Thread.Sleep(_bufferingInterval.Add(TimeSpan.FromMilliseconds(1)));
            _collection.Verify(x => x.InsertOneAsync(It.Is<EventEntry>(@event => @event.EventId == 1), CancellationToken.None), Times.AtMostOnce);
            _collection.Verify(x => x.InsertManyAsync(It.Is<IEnumerable<EventEntry>>(events => events.All(@event => @event.EventId == 1)), null, CancellationToken.None), Times.AtLeastOnce);
        }

        /// <summary>
        /// The tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void TearDown()
        {
            _eventListener.DisableEvents(TestEventSource.EventSource);
            _eventListener.Dispose();
        }
    }
}