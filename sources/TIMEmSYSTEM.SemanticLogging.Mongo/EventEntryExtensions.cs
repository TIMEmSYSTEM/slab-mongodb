using System.Collections.Generic;
using System.Linq;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using MongoDB.Bson;

namespace TIMEmSYSTEM.SemanticLogging.Mongo
{
    internal static class EventEntryExtensions
    {
        internal static BsonDocument AsBsonDocument(this EventEntry eventEntry)
        {
            var payload = eventEntry.Schema.Payload.Zip(eventEntry.Payload,
                (key, value) => new KeyValuePair<string, object>(key, value)).ToDictionary(x => x.Key, y => y.Value);
            var dictionary = new Dictionary<string, object>
            {
                {"event", eventEntry.EventId},
                {"message", eventEntry.FormattedMessage},
                {"timestamp", eventEntry.Timestamp.LocalDateTime},
                {"provider", eventEntry.ProviderId},
                {"activity", eventEntry.ActivityId},
                {"process", eventEntry.ProcessId},
                {"thread", eventEntry.ThreadId},
                {"level", (int) eventEntry.Schema.Level},
                {"keywords", (long) eventEntry.Schema.Keywords},
                {"payload", new BsonDocument(payload)}
            };
            return new BsonDocument(dictionary);
        }

        internal static BsonDocument[] AsBsonDocuments(this IEnumerable<EventEntry> eventEntries)
        {
            return eventEntries.Select(AsBsonDocument).ToArray();
        }
    }
}