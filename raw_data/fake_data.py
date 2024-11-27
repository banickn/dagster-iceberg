import json
import uuid
from datetime import datetime
import random
import multiprocessing
import itertools
import time
from typing import Any
# Pre-generate lists to reduce random call overhead
PROCESS_STEPS: list[str] = ["Etching", "Doping", "Deposition", "Photolithography", "Annealing"]
TOOLS: list[str] = ["Applied Material", "ASML", "LAM", "KLA Corporation", "Tokyo Electron Limited"]
CHEMICALS: list[str] = ["Ar", "N2", "O2", "SiH4", "HCl"]


def generate_record() -> dict[str, Any]:
    """
    Generate a single record more efficiently.
    Uses pre-generated lists and reduces function call overhead.
    """
    return {
        "timestamp": datetime.now().isoformat(),
        "wafer_id": str(uuid.uuid4()),
        "process_step": random.choice(PROCESS_STEPS),
        "tool": random.choice(TOOLS),
        "temperature_c": round(random.uniform(20.0, 300.0), 2),
        "pressure_pa": round(random.uniform(1e5, 1e6), 2),
        "chemical_used": random.choice(CHEMICALS),
        "defects_detected": random.randint(0, 5),
        "operator_id": random.randint(1000, 9999),
        "batch_id": str(uuid.uuid4())
    }


def generate_chunk(chunk_size: int) -> list[dict[str, Any]]:
    """
    Generate a chunk of records to be used in multiprocessing.
    """
    return [generate_record() for _ in range(chunk_size)]


def generate_fake_data(num_records: int, use_multiprocessing: bool = True) -> list[dict[str, Any]]:
    """
    Generate fake semiconductor fabrication data efficiently.

    Args:
        num_records (int): Total number of records to generate
        use_multiprocessing (bool): Whether to use multiprocessing for generation

    Returns:
        list: Generated records
    """
    if not use_multiprocessing:
        return [generate_record() for _ in range(num_records)]

    # Determine optimal chunk size and number of processes
    num_cpus: int = multiprocessing.cpu_count()
    chunk_size: int = max(1000, num_records // num_cpus)

    # Create a pool of workers
    with multiprocessing.Pool(processes=num_cpus) as pool:
        # Distribute work across multiple processes
        chunks: list[list[dict[str, Any]]] = pool.map(generate_chunk,
                                                      [chunk_size] * (num_records // chunk_size + 1))

    # Flatten the chunks and trim to exact number of records
    return list(itertools.islice(itertools.chain.from_iterable(chunks), num_records))


def write_data(file_name: str, num_records: int, output_format: str = 'json', use_multiprocessing: bool = True) -> None:
    """
    Write generated data to file.

    Args:
        file_name (str): Output file name
        num_records (int): Number of records to generate
        output_format (str): 'json' for standard JSON or 'ldj' for line-delimited JSON
        use_multiprocessing (bool): Whether to use multiprocessing for generation
    """
    # Generate data
    data: list[dict[str, Any]] = generate_fake_data(num_records, use_multiprocessing)

    # Write data based on format
    with open(file_name, "w") as f:
        if output_format == 'json':
            json.dump(data, f, indent=4)
        elif output_format == 'ldj':
            # True line-delimited JSON without spaces between records
            for record in data:
                f.write(json.dumps(record) + '\n')
        else:
            raise ValueError("Output format must be 'json' or 'ldj'")


def main() -> None:
    # Generate timestamp for unique filename
    start: float = time.time()
    timestamp: str = datetime.now().isoformat().replace(':', '-')
    file_name: str = f"semi_data-{timestamp}"
    num_records: int = 500_000

    print("Generating JSON file...")
    write_data(f"{file_name}.json", num_records=num_records, output_format='ldj')

    end: float = time.time()
    print(f"Generated {num_records} records in {end-start} seconds and saved to {file_name}.")


if __name__ == "__main__":
    main()
