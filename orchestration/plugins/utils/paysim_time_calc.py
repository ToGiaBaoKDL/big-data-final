import sys
import pendulum
import argparse
import os

SIMULATION_START = "2025-12-28T00:00:00+00:00"

def calculate_step(logical_date_str):
    logical_date = pendulum.parse(logical_date_str)
    sim_start = pendulum.parse(SIMULATION_START)
    
    diff = logical_date - sim_start
    step = int(diff.total_seconds() / 3600) + 1
    if step < 1:
        raise ValueError(f"Invalid step: {step} (Date too early)")
    return step, sim_start

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("logical_date", help="Airflow Logical Date (ISO format)")
    parser.add_argument("output_type", choices=["step", "part_dt", "part_hour", "all"], default="step")
    args = parser.parse_args()

    try:
        step, sim_start = calculate_step(args.logical_date)
        sim_timestamp = sim_start.add(hours=step - 1)

        if args.output_type == 'step':
            print(step)
        elif args.output_type == 'part_dt':
            print(sim_timestamp.format('YYYYMMDD'))
        elif args.output_type == 'part_hour':
            print(sim_timestamp.format('HH'))
        elif args.output_type == 'all':
            print(f"{step} {sim_timestamp.format('YYYYMMDD')} {sim_timestamp.format('HH')}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
