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


class EventStream(DataStream):
    pass


class StreamProcessor:
    pass


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    print("\nInitializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    sensor_data = ["temp: 22.5", "humidity: 65", "pressure: 1013"]
    print(f"Processing sensor batch: {sensor_data}")
    try:
        print(sensor.process_batch(sensor_data))
    except ValueError as e:
        print(f"Error: {e}")
    # # 2. Тест TransactionStream (нужно будет дописать класс)
    # print("\nInitializing Transaction Stream...")
    # # Ожидаемый вывод: Stream ID: TRANS_001, Type: Financial Data [cite: 232]
    # # Ожидаемый вывод: Transaction analysis: 3 operations, net flow: +25 units [cite: 235]

    # # 3. Тест EventStream (нужно будет дописать класс)
    # print("\nInitializing Event Stream...")
    # # Ожидаемый вывод: Stream ID: EVENT_001, Type: System Events [cite: 237]
    # # Ожидаемый вывод: Event analysis: 3 events, 1 error detected [cite: 239]

    # # === Polymorphic Stream Processing ===
    # print("\n=== Polymorphic Stream Processing ===")
    # print("Processing mixed stream types through unified interface...")
    # # Список потоков для демонстрации подтипного полиморфизма [cite: 215, 219]
    # streams: List[DataStream] = [
    #     sensor,
    #     # TransactionStream("TRANS_001"),
    #     # EventStream("EVENT_001")
    # ]

    # # Здесь логика StreamProcessor, который обрабатывает их в цикле [cite: 209, 215]
    # print("Batch 1 Results:")
    # # Пример вывода для сенсора в полиморфном режиме[cite: 243]:
    # # print(f"- Sensor data: {len(sensor.history)} readings processed")
