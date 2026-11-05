from dataclasses import dataclass
from typing import override

@dataclass(frozen=True)
class BrokerInfo:
    id_: int
    host: str
    port: int

    def get_id(self) -> int: # This is not necesarry in python, we can just do print(broker.id_)
        return self.id_

    def get_host(self) -> str:
        return self.host

    def get_port(self) -> int:
        return self.port

    @override
    def __str__(self) -> str:
        return f"BrokerInfo(id_={self.id_}, host='{self.host}', port={self.port})"

    @override
    def __eq__(self, obj: object) -> bool:
        if self is obj:
            return True

        if type(self) is not type(obj):
            return False

        return self.id_ == obj.id_

    @override
    def __hash__(self) -> int:
        return hash((self.id_))
