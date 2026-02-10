from abc import ABC, abstractmethod
from typing import List, Any, Optional, Union, Dict


class DataStream(ABC):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"id": self.stream_id}


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.history: List[float] = []
        self.stream_type: str = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            raise ValueError("data_batch should be a list!")

        current_batch_count = 0
        batch_sum = 0.0

        for item in data_batch:
            if isinstance(item, str) and ":" in item:
                _, value_str = item.split(":", 1)
                try:
                    value = float(value_str)
                    self.history.append(value)
                    batch_sum += value
                    current_batch_count += 1
                except ValueError:
                    continue

        avg = (
            batch_sum / current_batch_count if current_batch_count > 0 else 0.0
        )
        return (
            f"Sensor analysis: {current_batch_count} readings processed, "
            f"avg: {avg}"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if not criteria:
            return data_batch
        return [
            d
            for d in data_batch
            if isinstance(d, str) and d.startswith(criteria)
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        total_readings = len(self.history)
        avg_total = (
            sum(self.history) / total_readings if total_readings > 0 else 0.0
        )
        return {
            "id": self.stream_id,
            "type": self.stream_type,
            "total_processed": total_readings,
            "average_value": round(avg_total, 2),
        }


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.stream_type: str = "Financial Data"
        self.operations_count: int = 0
        self.total_net_flow: float = 0.0

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            raise ValueError("data_batch should be a list!")

        current_batch_count = 0

        for item in data_batch:
            if isinstance(item, str) and ":" in item:
                action, value_str = item.split(":", 1)
                try:
                    value = float(value_str)
                    if action.strip().lower() == "buy":
                        self.total_net_flow += value
                    elif action.strip().lower() == "sell":
                        self.total_net_flow -= value
                    current_batch_count += 1
                    self.operations_count += 1
                except ValueError:
                    continue

        net_sign = "+" if self.total_net_flow >= 0 else ""
        return (
            f"Transaction analysis: {current_batch_count} operations, "
            f"net flow: {net_sign}{self.total_net_flow:.0f} units"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if not criteria:
            return data_batch
        return [
            d
            for d in data_batch
            if isinstance(d, str) and d.startswith(criteria)
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "id": self.stream_id,
            "type": self.stream_type,
            "total_operations": self.operations_count,
            "net_flow": round(self.total_net_flow, 2),
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.stream_type: str = "System Events"
        self.events_count: int = 0
        self.error_count: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        if not isinstance(data_batch, list):
            raise ValueError("data_batch should be a list!")

        current_batch_count = 0
        current_error_count = 0

        for item in data_batch:
            if isinstance(item, str):
                current_batch_count += 1
                self.events_count += 1
                if item.strip().lower() == "error":
                    current_error_count += 1
                    self.error_count += 1

        return (
            f"Event analysis: {current_batch_count} events, "
            f"{current_error_count} error detected"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if not criteria:
            return data_batch
        return [
            d
            for d in data_batch
            if isinstance(d, str) and d.strip().lower() == criteria.lower()
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "id": self.stream_id,
            "type": self.stream_type,
            "total_events": self.events_count,
            "error_count": self.error_count,
        }


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_all(
        self, batches: Dict[str, List[Any]]
    ) -> List[str]:
        results: List[str] = []
        for stream in self.streams:
            if stream.stream_id in batches:
                try:
                    result = stream.process_batch(
                        batches[stream.stream_id]
                    )
                    results.append(result)
                except ValueError as e:
                    results.append(
                        f"Error processing {stream.stream_id}: {e}"
                    )
        return results

    def filter_all(
        self,
        batches: Dict[str, List[Any]],
        criteria: Optional[str] = None,
    ) -> Dict[str, List[Any]]:
        filtered: Dict[str, List[Any]] = {}
        for stream in self.streams:
            if stream.stream_id in batches:
                filtered[stream.stream_id] = stream.filter_data(
                    batches[stream.stream_id], criteria
                )
        return filtered

    def get_all_stats(
        self,
    ) -> List[Dict[str, Union[str, int, float]]]:
        return [stream.get_stats() for stream in self.streams]


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    # --- Sensor Stream ---
    print("\nInitializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    sensor_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {sensor_data}")
    try:
        print(sensor.process_batch(sensor_data))
    except ValueError as e:
        print(f"Error: {e}")

    # --- Transaction Stream ---
    print("\nInitializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print(
        f"Stream ID: {transaction.stream_id}, "
        f"Type: {transaction.stream_type}"
    )
    transaction_data = ["buy:100", "sell:150", "buy:75"]
    print(f"Processing transaction batch: {transaction_data}")
    try:
        print(transaction.process_batch(transaction_data))
    except ValueError as e:
        print(f"Error: {e}")

    # --- Event Stream ---
    print("\nInitializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")
    event_data = ["login", "error", "logout"]
    print(f"Processing event batch: {event_data}")
    try:
        print(event.process_batch(event_data))
    except ValueError as e:
        print(f"Error: {e}")

    # --- Polymorphic Stream Processing ---
    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    processor = StreamProcessor()
    sensor2 = SensorStream("SENSOR_002")
    transaction2 = TransactionStream("TRANS_002")
    event2 = EventStream("EVENT_002")

    processor.add_stream(sensor2)
    processor.add_stream(transaction2)
    processor.add_stream(event2)

    batches: Dict[str, List[Any]] = {
        "SENSOR_002": ["temp:30.1", "humidity:80"],
        "TRANS_002": ["buy:200", "sell:50", "buy:120", "sell:30"],
        "EVENT_002": ["login", "error", "logout"],
    }

    results = processor.process_all(batches)
    print("\nBatch 1 Results:")
    for result in results:
        print(f"- {result}")

    # --- Filtering ---
    print("\nStream filtering active: High-priority data only")
    sensor_filtered = sensor2.filter_data(
        ["temp:95.0", "humidity:40", "temp:102.3", "pressure:1000"],
        criteria="temp",
    )
    transaction_filtered = transaction2.filter_data(
        ["buy:5000", "sell:20", "buy:10"],
        criteria="buy",
    )
    print(
        f"Filtered results: {len(sensor_filtered)} critical sensor alerts, "
        f"{len(transaction_filtered)} large transaction"
    )

    print("\nAll streams processed successfully. "
          "Nexus throughput optimal.")
