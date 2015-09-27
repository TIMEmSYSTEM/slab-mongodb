// --------------------------------------------------------------------------------------------------------------------
// <copyright file="TestEventSource.cs" company="TIMEmSYSTEM ApS">
//   © TIMEmSYSTEM 2015
// </copyright>
// <summary>
//   Test event source.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System;
using System.Diagnostics.Tracing;

namespace TIMEmSYSTEM.SemanticLogging.Mongo.Tests
{
    /// <summary>
    ///     Test event source.
    /// </summary>
    [EventSource(Name = "Test")]
    public class TestEventSource : EventSource
    {
        /// <summary>
        ///     Log instance.
        /// </summary>
        private static readonly Lazy<TestEventSource> Log = new Lazy<TestEventSource>(() => new TestEventSource());

        /// <summary>
        ///     Prevents a default instance of the <see cref="TestEventSource" /> class from being created.
        /// </summary>
        private TestEventSource()
        {
        }

        /// <summary>
        ///     Gets the event source.
        /// </summary>
        public static TestEventSource EventSource
        {
            get
            {
                return Log.Value;
            }
        }

        /// <summary>
        /// Test event.
        /// </summary>
        [Event(1)]
        public void TestEvent()
        {
            if (IsEnabled())
            {
                WriteEvent(1);
            }
        }
    }
}