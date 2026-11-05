from dataclasses import dataclass
from typing import ClassVar

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

    # Send an error message to the client.
    def send_error_response(self, channel: socket_channel, error_message: str) -> None:
        pass

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
    def encode_replication_request(self, partition: int, offset: int, message: bytearray) -> bytearray:
        pass

    # Encode a topic notification.
    def encode_topic_notification(self, topic: str) -> bytearray:
        pass

    # Decodes a producer's response.
    def decode_produce_response(self, buffer: bytearray) -> ProducerResponse:
        response_type = buffer[0]
        if response_type != PRODUCE_RESPONSE:
            if response_type == ERROR_RESPONSE:
                error_length = int.from_bytes(buffer[1:3], "big")
                error_bytes = buffer[3:3 + error_length]
                # buffer.get(errorBytes)
                error = str(error_bytes) # byte.decode?
                return ProduceResult(-1, error)
            return ProduceResult(-1, "Invalid response type")

        offset = buffer[1:9]
        status = buffer[10:12]

        return ProduceResult(offset, None if status == None else "Produce failed")

    # Decodes a response from a fetch request.
    def decode_fetch_request(self, buffer: bytearray) -> FetchResult:
        response_type = buffer[0]
        if response_type != FETCH_RESPONSE:
            if response_type == ERROR_RESPONSE:
                error_length = int.from_bytes(buffer[1:3], "big")
                error_bytes = buffer[3:3 + error_length]
                error = str(error_bytes) # byte.decode?
                # return new FetchResult(new byte[0][], "Invalid response type")

    # Decodes a response containing cluster metadata.
    def decode_metadata_response(self, buffer: bytearray) -> MetadataResult:
        pass 
