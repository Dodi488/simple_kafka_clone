from __future__ import annotations
from dataclasses import dataclass
from typing import ClassVar
import socket
from broker_info import BrokerInfo

@dataclass(frozen=True)
class Protocol:
    # Client request types
    PRODUCE: ClassVar[bytes] = b'\x01'
    FETCH: ClassVar[bytes] = b'\x02'
    METADATA: ClassVar[bytes] = b'\x03'
    CREATE_TOPIC: ClassVar[bytes] = b'\x04'

    # Broker response types
    PRODUCE_RESPONSE: ClassVar[bytes] = b'\x11'
    FETCH_RESPONSE: ClassVar[bytes] = b'\x12'
    METADATA_RESPONSE: ClassVar[bytes] = b'\x13'
    CREATE_TOPIC_RESPONSE: ClassVar[bytes] = b'\x14'
    ERROR_RESPONSE: ClassVar[bytes] = b'\x1F'

    # Internal broker communication
    REPLICATE: ClassVar[bytes] = b'\x21'
    REPLICATE_ACK: ClassVar[bytes] = b'\x22'
    TOPIC_NOTIFICATION: ClassVar[bytes] = b'\x23'

    # Send an error message to the client.
    def send_error_response(self, channel: socket.socket, error_message: str) -> None:
        buffer = bytearray()
        buffer.extend(self.ERROR_RESPONSE)
        buffer.extend(len(error_message).to_bytes(2, "big"))
        buffer.extend(error_message.encode("utf-8"))
        channel.sendall(buffer)

    # Encodes a producer's request.
    def encode_produce_request(self, topic: str, partition: int, message: bytearray) -> bytearray:
        buffer = bytearray()
        buffer.extend(self.PRODUCE)
        buffer.extend(len(topic).to_bytes(2, "big"))
        buffer.extend(topic.encode("utf-8"))
        buffer.extend(partition.to_bytes(4, "big"))
        buffer.extend(len(message).to_bytes(4, "big"))
        buffer.extend(message)
        return buffer

    # Encodes a request to fetch a message from a topic.
    def encode_fetch_request(self, topic: str, partition: int, offset: int, max_bytes: int) -> bytearray:
        buffer = bytearray()
        buffer.extend(self.FETCH)
        buffer.extend(len(topic).to_bytes(2, "big"))
        buffer.extend(topic.encode("utf-8"))
        buffer.extend(partition.to_bytes(4, "big"))
        buffer.extend(offset.to_bytes(8, "big"))
        buffer.extend(max_bytes.to_bytes(4, "big"))
        return buffer

    # Encodes a metadata request.
    def encode_metadata_request(self) -> bytearray:
        buffer = bytearray()
        buffer.extend(self.METADATA)
        return buffer

    # Encodes a request to crate a new topic.
    def encode_create_topic_request(self, topic: string, num_partitions: int, replication_factors: int) -> bytearray:
        buffer = bytearray()
        buffer.extend(self.CREATE_TOPIC)
        buffer.extend(len(topic).to_bytes(2, "big"))
        buffer.extend(topic.encode("utf-8"))
        buffer.extend(num_partitions.to_bytes(4, "big"))
        buffer.extend(replication_factors.to_bytes(2, "big"))
        return buffer

    # Encode a replication requests.
    def encode_replicate_request(self, topic: str, partition: int, offset: int, message: bytearray) -> bytearray:
        buffer = bytearray()
        buffer.extend(self.REPLICATE)
        buffer.extend(len(topic).to_bytes(2, "big"))
        buffer.extend(topic.encode("utf-8"))
        buffer.extend(partition.to_bytes(4, "big"))
        buffer.extend(offset.to_bytes(8, "big"))
        buffer.extend(len(message).to_bytes(4, "big"))
        buffer.extend(message)
        return buffer

    # Encode a topic notification.
    def encode_topic_notification(self, topic: str) -> bytearray:
        buffer = bytearray()
        buffer.extend(self.TOPIC_NOTIFICATION)
        buffer.extend(len(topic).to_bytes(2, "big"))
        buffer.extend(topic.encode("utf-8"))
        return buffer

    # Decodes a producer's response.
    def decode_produce_response(self, buffer: bytearray) -> ProducerResponse:
        response_type = buffer[0:1]
        if response_type != self.PRODUCE_RESPONSE:
            if response_type == self.ERROR_RESPONSE:
                error_length = int.from_bytes(buffer[1:3], "big")
                error_bytes = buffer[3:3 + error_length]
                error = error_bytes.decode("utf-8")
                return self.ProduceResult(-1, error)
            return self.ProduceResult(-1, "Invalid response type")

        offset = int.from_bytes(buffer[1:9], "big")
        status = int.from_bytes(buffer[9:11], "big")

        return self.ProduceResult(offset, None if status == 0 else "Produce failed")

    # Decodes a response from a fetch request.
    def decode_fetch_response(self, buffer: bytearray) -> FetchResult:
        response_type = buffer[0:1]
        if response_type != self.FETCH_RESPONSE:
            if response_type == self.ERROR_RESPONSE:
                error_length = int.from_bytes(buffer[1:3], "big")
                error_bytes = buffer[3:3 + error_length]
                error = error_bytes.decode("utf-8")
                return self.FetchResult([], error)
            return self.FetchResult([], "Invalid response type")

        message_count = int.from_buffer(buffer[1:5], "big")

        pos = 5
        messages = []
        for _ in range(message_count):
            pos += 8  # skip offset
            message_size = int.from_bytes(buffer[pos:pos+4], 'big')
            pos += 4
            messages.append(buffer[pos:pos+message_size])
            pos += message_size

        return self.FetchResult(messages, None)

    # Decodes a response containing cluster metadata.
    def decode_metadata_response(self, buffer: bytearray) -> MetadataResult:
        response_type = buffer[0:1]
        if response_type != self.METADATA_RESPONSE:
            if response_type == self.ERROR_RESPONSE:
                error_length = int.from_bytes(buffer[1:3], "big")
                error_bytes = buffer[3:3 + error_length]
                error = error_bytes.decode("utf-8")
                return self.MetadataResult([], [], error)
            return self.MetadataResult([], [], "Invalid response type")

        broker_count = int.from_bytes(buffer[1:5], "big")
        brokers = []

        pos = 5
        for i in range(broker_count):
            broker_id = int.from_bytes(buffer[pos:pos+4], "big")
            pos += 4
            host_length = int.from_bytes(buffer[pos:pos+2], "big")
            pos += 2
            # host_bytes = int.from_bytes(buffer[pos:pos+host_length])
            host_bytes = buffer[pos:pos+host_length]
            pos += host_length
            host = host_bytes.decode("utf-8")
            port = int.from_bytes(buffer[pos:pos+4], "big")
            pos += 4

            brokers.append(BrokerInfo(broker_id, host, port))

        topic_count = int.from_bytes(buffer[pos:pos+4], "big")
        topics = []
        pos += 4
        for i in range(topic_count):
            topic_length = int.from_bytes(buffer[pos:pos+2], "big")
            pos += 2
            topic_bytes = buffer[pos:pos+topic_length]
            pos += topic_length
            topic_name = topic_bytes.decode("utf-8")
        
            partition_count = int.from_bytes(buffer[pos:pos+4], "big")
            pos += 4 
            partitions = []

            for j in range(partition_count):
                partition_id = int.from_bytes(buffer[pos:pos+4], "big")
                pos += 4 
                leader_id = int.from_bytes(buffer[pos:pos+4], "big")
                pos += 4
        
                replicas = int.from_bytes(buffer[pos:pos+4], "big")
                pos += 4
        
                replica_ids = []
                for k in range(replicas):
                    replica_ids.append(int.from_bytes(buffer[pos:pos+4], "big"))
                    pos += 4
        
                partitions.append(self.PartitionMetadata(partition_id, leader_id, replica_ids))
        
            topics.append(self.TopicMetadata(topic_name, partitions))
        
        return self.MetadataResult(brokers, topics, None)
        
    @dataclass(frozen=True)
    class ProduceResult:
        offset: int
        error: str | None = None

        def get_offset(self) -> int:
            return self.offset

        def get_error(self) -> str | None:
            return self.error

        def is_success(self) -> bool:
            return self.error == None

    @dataclass(frozen=True)
    class FetchResult:
        message: bytearray
        error: str

        def get_error(self):
            return self.message

        def get_message_count(self):
            return len(self.message)

        def get_error(self):
            return self.error

        def is_success(self):
            return self.error == None

    @dataclass(frozen=True)
    class MetadataResult:
        brokers: List[BrokerInfo]
        topics: List[TopicMetadata]
        error: str

        def get_brokers(self) -> List[BrokerInfo]:
            return self.brokers

        def get_topics(self) -> List[TopicMetadata]:
            return self.topics

        def get_error(self) -> str:
            return self.error

        def is_success(self) -> bool:
            return self.error == None

    @dataclass(frozen=True)
    class TopicMetadata:
        name: str
        partitions: List[PartitionMetadata]

        def get_name(self) -> str:
            return self.name

        def get_partitions(self) -> List[PartitionMetadata]:
            return self.partitions

    @dataclass(frozen=True)
    class PartitionMetadata:
        id_: int
        leader: int
        replicas: List[Int]

        def get_id(self):
            return self.id_

        def get_leader(self):
            return self.leader

        def get_replicas(self):
            return self.replicas
