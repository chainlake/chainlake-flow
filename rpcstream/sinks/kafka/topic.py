kafka-configs --alter \
  --entity-type topics \
  --entity-name your_topic \
  --add-config message.timestamp.type=LogAppendTime