import typing
import uuid
from typing import Any, Dict, Optional

import utils


@utils.add_unique_id
class Traffic:
    def __init__(
        self,
        execution_time: int,
        expired_time: Optional[int] = None,
        client_location: Optional[utils.GeographicalRegion] = None,
    ) -> None:
        self.execution_time = execution_time
        self.remaining_processing_time = execution_time
        self.start_time: Optional[int] = None
        self.finish_time: Optional[int] = None
        self.expired_time: Optional[int] = expired_time
        self.client_location = client_location
        self.expired: bool = False

    def set_start_time(self, start_time: int) -> "Traffic":
        self.start_time = start_time
        return self

    def set_finish_time(self, finished_time: int) -> "Traffic":
        self.finish_time = finished_time
        return self

    def set_expired_time(self, expired_time: Optional[int]) -> "Traffic":
        self.expired_time = expired_time
        return self

    def set_client_location(
        self, client_location: Optional[utils.GeographicalRegion]
    ) -> "Traffic":
        self.client_location = client_location
        return self

    def latency(self) -> Optional[int]:
        if self.finish_time is None or self.start_time is None:
            return None
        return self.finish_time - self.start_time

    def clear_compute(self) -> "Traffic":
        self.remaining_processing_time = self.execution_time
        return self

    def finished(self, current_tick: int) -> bool:
        if self.remaining_processing_time <= 0:
            return True
        if self.start_time is not None and self.expired_time is not None:
            if current_tick >= self.start_time + self.expired_time:
                self.expired = True
                return True
        return False

    def info(self) -> Dict[str, Any]:
        return {
            "id": self.id,  # type: ignore # pylint: disable=no-member
            "remaining_processing_time": self.remaining_processing_time,
            "finish_time": self.finish_time,
            "expired": self.expired,
        }

    def meta_info(self) -> Dict[str, Any]:
        return {
            "id": self.id,  # type: ignore # pylint: disable=no-member
            "execution_time": self.execution_time,
            "start_time": self.start_time,
            "expired_time": self.expired_time,
            "client_location": (
                self.client_location.value if self.client_location is not None else None
            ),
        }
