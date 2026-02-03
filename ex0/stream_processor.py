from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Data Processor formatting: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        print(f"Processing data: {data}")

        if not self.validate(data):
            raise ValueError("Numeric validation error")

        print("Validation: Numeric data verified")

        summary = sum(data)
        avg = summary / len(data)
        return f"{len(data)}:{summary}:{avg}"

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False

        return all(isinstance(n, (int, float)) for n in data)

    def format_output(self, result: str) -> str:
        count_val, total_sum, avg = result.split(":")
        return (
            f"Output: Processed {count_val} numeric values, "
            f"sum={total_sum}, avg={avg}"
        )


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        print(f"Processing data: {data}")

        if not self.validate(data):
            raise ValueError("Text validation error")

        print("Validation: Text data verified")
        return f"{len(data)}:{len(data.split())}"

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def format_output(self, result: str) -> str:
        char_amount, words_amount = result.split(":")
        return (
            f"Output: Processed text: {char_amount} characters, "
            f"{words_amount} words"
        )


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        print(f"Processing data: {data}")

        if not self.validate(data):
            raise ValueError("Log validation error")

        print("Validation: Log entry verified")

        level, message = data.split(":", 1)
        return f"{level}:{message}"

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ":" in data

    def format_output(self, result: str) -> str:
        level, message = result.split(":")
        tag = "[ALERT]" if level == "ERROR" else "[INFO]"
        return f"Output: {tag} {level} level detected: {message}"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    numeric_processor = NumericProcessor()
    try:
        result = numeric_processor.process([1, 2, 3, 4, 5])
        print(numeric_processor.format_output(result))
    except ValueError as e:
        print(f"{e}")

    print("\nInitializing Text Processor...")
    text_proc = TextProcessor()
    try:
        result = text_proc.process("Hello Nexus World")
        print(text_proc.format_output(result))
    except ValueError as e:
        print(f"{e}")

    print("\nInitializing Log Processor...")
    log_proc = LogProcessor()
    try:
        result = log_proc.process("ERROR: Connection timeout")
        print(log_proc.format_output(result))
    except ValueError as e:
        print(f"{e}")

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor(),
    ]

    test_data = [[1, 2, 3], "Hello Nexus", "INFO: System ready"]

    for i, processor in enumerate(processors, 1):
        try:
            res = processor.process(test_data[i])
            print(f"Result {i}: {processor.format_output(res)}\n")
        except ValueError as e:
            print(f"{e}")
