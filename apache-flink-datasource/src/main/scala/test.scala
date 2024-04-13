//DataStream<SomeObject> dataStream = (...);
//
//SerializationSchema<SomeObject> serializationSchema = (...);
//SinkFunction<SomeObject> pubsubSink = PubSubSink.newBuilder()
//  .withSerializationSchema(serializationSchema)
//  .withProjectName("project")
//  .withSubscriptionName("subscription")
//  .build()
//
//dataStream.addSink(pubsubSink);