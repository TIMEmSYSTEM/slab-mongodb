namespace TIMEmSYSTEM.SemanticLogging.MongoDB.Tests
{
    using global::MongoDB.Driver;

    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;

    using Moq;

    public static class HumongousSinkTestsSetUp
    {
        public const string InstanceName = "slab";

        public const string CollectionName = "events";

        public static void SetUpMocks(Mock<IMongoClient> client, Mock<IMongoDatabase> database, Mock<IMongoCollection<EventEntry>> collection)
        {
            client.Setup(x => x.GetDatabase(InstanceName, null)).Returns(() => database.Object);
            database.Setup(x => x.GetCollection<EventEntry>(CollectionName, null))
                .Returns(() => collection.Object);
        }
    }
}