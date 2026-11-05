from dataclasses import dataclass
from typing import ClassVar

@dataclass(frozen=True)
class Protocol:
    # Client request types
    PRODUCE: ClassVar[bytes] = b'\x01'
    FETCH: ClassVar[bytes] = b'\X02'
    METADATAL: ClassVar[bytes] = b'\x03'
    CREATE_TOPIC: ClassVar[bytes] = b'\x04'

    # Broker response types
    PRODUCE_RESPONSE = ClassVar[bytes] = b'\x11'
    FETCH_RESPONSE = ClassVar[bytes] = b'\x12'

    # Send an error message to the client.
    def send_error_response(self, channel: socket_channel, error_message: str) -> None:
        pass

    # Encodes a producer's request.
    def encode_produce_request(self, topic: str, partition: int, message: bytearray) -> bytearray:
        pass

    # Encodes a request to fetch a message from a topic.
    def encode_fetch_request(self, topic: str, partition: int, offset: int, max_bytes: int) -> bytearray:
        pass

    # Encodes a metadata request.
    def encode_metadata_request(self) -> bytearray:
        pass

    # Encodes a request to crate a new topic.
    def encode_create_topic_request(self, topic: string, num_partitions: int, replication_factors: int) -> bytearray:
        pass

    # Encode a replication requests.
    def encode_replication_request(self, partition: int, offset: int, message: bytearray) -> bytearray:
        pass

    # Encode a topic notification.
    def encode_topic_notification(self, topic: str) -> bytearray:
        pass

    # Decodes a producer's response.
    def decode_produce_response(self, buffer: bytearray) -> ProducerResponse:
        pass

    # Decodes a response from a fetch request.
    def decode_fetch_request(self, buffer: bytearray) -> FetchResult:
        pass

    # Decodes a response containing cluster metadata.
    def decode_metadata_response(self, buffer: bytearray) -> MetadataResult:
        pass

    # 
