# main.py
from protocol import Protocol

p = Protocol()

# Test arguments:
topic = "test-topic"
num_partitions = 3
replication_factor = 1

# Call your encode method
result = p.encode_create_topic_request(topic, num_partitions, replication_factor)

print("Raw bytearray:", result)
print("Hex representation:", result.hex())
