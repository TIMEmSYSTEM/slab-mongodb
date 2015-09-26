// --------------------------------------------------------------------------------------------------------------------
// <copyright file="HumongousSinkBufferingIntervalTests.cs" company="TIMEmSYSTEM ApS">
//   © TIMEmSYSTEM 2015
// </copyright>
// <summary>
//   The humongous sink tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------
namespace TIMEmSYSTEM.SemanticLogging.MongoDB.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Tracing;
    using System.Linq;
    using System.Threading;

    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;

    using global::MongoDB.Driver;

    using Moq;

    using NUnit.Framework;

    /// <summary>
    ///     The humongous sink buffering interval tests.
    /// </summary>
    [TestFixture]
    public class HumongousSinkBufferingIntervalTests
    {
        /// <summary>
        ///     The mongo client.
        /// </summary>
        private readonly Mock<IMongoClient> client = new Mock<IMongoClient>();

        /// <summary>
        ///     The mongo database.
        /// </summary>
        private readonly Mock<IMongoDatabase> database = new Mock<IMongoDatabase>();

        /// <summary>
        ///     The mongo collection.
        /// </summary>
        private readonly Mock<IMongoCollection<EventEntry>> collection = new Mock<IMongoCollection<EventEntry>>();

        /// <summary>
        ///     The buffering interval.
        /// </summary>
        private readonly TimeSpan bufferingInterval = TimeSpan.FromSeconds(1);

        /// <summary>
        ///     The sink.
        /// </summary>
        private IObserver<EventEntry> sink;

        /// <summary>
        /// The event listener.
        /// </summary>
        private ObservableEventListener eventListener;

        /// <summary>
        ///     The set up.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            HumongousSinkTestsSetUp.SetUpMocks(this.client, this.database, this.collection);
            this.sink = new HumongousSink(
                this.client.Object,
                HumongousSinkTestsSetUp.InstanceName,
                HumongousSinkTestsSetUp.CollectionName, 
                this.bufferingInterval, 
                byte.MaxValue, 
                int.MaxValue, 
                TimeSpan.Zero);
            this.eventListener = new ObservableEventListener();
            this.eventListener.EnableEvents(TestEventSource.EventSource, EventLevel.LogAlways);
            this.eventListener.Subscribe(this.sink);
        }

        /// <summary>
        /// Test should insert one event into collection.
        /// </summary>
        [Test]
        public void ShouldInsertOneEventIntoCollection()
        {
            TestEventSource.EventSource.TestEvent();
            Thread.Sleep(this.bufferingInterval.Add(TimeSpan.FromMilliseconds(1)));
            this.collection.Verify(x => x.InsertOneAsync(It.Is<EventEntry>(@event => @event.EventId == 1), CancellationToken.None), Times.Once);
            this.collection.Verify(x => x.InsertManyAsync(It.Is<IEnumerable<EventEntry>>(events => events.All(@event => @event.EventId == 1)), null, CancellationToken.None), Times.Never);
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

            Thread.Sleep(this.bufferingInterval.Add(TimeSpan.FromMilliseconds(1)));
            this.collection.Verify(x => x.InsertOneAsync(It.Is<EventEntry>(@event => @event.EventId == 1), CancellationToken.None), Times.AtMostOnce);
            this.collection.Verify(x => x.InsertManyAsync(It.Is<IEnumerable<EventEntry>>(events => events.All(@event => @event.EventId == 1)), null, CancellationToken.None), Times.AtLeastOnce);
        }

        /// <summary>
        /// The tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void TearDown()
        {
            this.eventListener.DisableEvents(TestEventSource.EventSource);
            this.eventListener.Dispose();
        }
    }
}