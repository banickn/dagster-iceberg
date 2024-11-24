import json
from faker import Faker
from datetime import datetime
import random


fake = Faker()


def generate_fake_data(num_records):
    data = []
    for _ in range(num_records):
        record = {
            "timestamp": datetime.now().isoformat(),
            "wafer_id": fake.uuid4(),
            "process_step": random.choice(["Etching", "Doping", "Deposition", "Photolithography", "Annealing"]),
            "tool": random.choice(["Applied Material", "ASML", "LAM", "KLA Corporation", "Tokyo Electron Limited"]),
            "temperature_c": round(random.uniform(20.0, 300.0), 2),
            "pressure_pa": round(random.uniform(1e5, 1e6), 2),
            "chemical_used": random.choice(["Ar", "N2", "O2", "SiH4", "HCl"]),
            "defects_detected": random.randint(0, 5),
            "operator_id": fake.random_int(min=1000, max=9999),
            "batch_id": fake.uuid4()
        }
        data.append(record)
    return data


def write_data_to_json(file_name, num_records):
    data = generate_fake_data(num_records)
    with open(file_name, "w") as f:
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    file_name = f"semi_data-{datetime.now().isoformat()}.json"
    num_records = 100  # Specify the number of records to generate
    write_data_to_json(file_name, num_records)
    print(f"Generated {num_records} records and saved to {file_name}.")
