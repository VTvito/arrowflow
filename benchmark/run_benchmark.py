"""
ETL Benchmark Runner — Microservices vs Monolith

Runs the same ETL pipeline (HR Analytics) using:
  1. Monolithic approach (in-process Pandas)
  2. Microservices approach (via Preparator SDK → HTTP → services)

Measures: execution time, memory usage, throughput (rows/sec).
Generates comparison charts (matplotlib + plotly).

Prerequisites:
  - Generate datasets first: python benchmark/generate_hr_dataset.py --all-scales
  - For microservices benchmark: all services must be running (docker compose up)

Usage:
    python benchmark/run_benchmark.py --mode monolith
    python benchmark/run_benchmark.py --mode microservices
    python benchmark/run_benchmark.py --mode both --plot
"""

import argparse
import json
import os
import sys
import time
import tracemalloc

import pandas as pd

# Add project root for imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from benchmark.monolithic_pipeline import run_monolithic_pipeline  # noqa: E402

SCALES = [1_000, 10_000, 50_000, 100_000, 500_000]
DATA_DIR = os.path.join(PROJECT_ROOT, "benchmark", "data")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "benchmark", "results")
SERVICES_CONFIG_PATH = os.path.join(PROJECT_ROOT, "preparator", "services_config.json")


def run_monolith_benchmark(scales: list[int] | None = None, repeats: int = 3) -> list[dict]:
    """Run monolithic pipeline at various dataset scales."""
    scales = scales or SCALES
    results = []

    for n in scales:
        input_path = os.path.join(DATA_DIR, f"hr_{n // 1000}k.csv")
        if not os.path.exists(input_path):
            print(f"  SKIP {n:>8,} rows — file not found: {input_path}")
            continue

        durations = []
        peak_mems = []

        for r in range(repeats):
            output_path = os.path.join(RESULTS_DIR, f"mono_{n // 1000}k_run{r}.csv")

            tracemalloc.start()
            t0 = time.time()
            run_monolithic_pipeline(input_path, output_path)
            elapsed = time.time() - t0
            _, peak_mem = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            durations.append(elapsed)
            peak_mems.append(peak_mem / (1024 * 1024))  # MB

            # Clean up output
            if os.path.exists(output_path):
                os.remove(output_path)

        avg_duration = sum(durations) / len(durations)
        avg_mem = sum(peak_mems) / len(peak_mems)
        throughput = n / avg_duration if avg_duration > 0 else 0

        result = {
            "approach": "monolith",
            "rows": n,
            "avg_duration_sec": round(avg_duration, 3),
            "min_duration_sec": round(min(durations), 3),
            "max_duration_sec": round(max(durations), 3),
            "avg_peak_memory_mb": round(avg_mem, 1),
            "throughput_rows_per_sec": round(throughput),
            "repeats": repeats,
        }
        results.append(result)
        print(f"  Monolith {n:>8,} rows: {avg_duration:.3f}s avg, {avg_mem:.1f} MB peak, {throughput:,.0f} rows/s")

    return results


def run_microservices_benchmark(scales: list[int] | None = None, repeats: int = 3) -> list[dict]:
    """Run microservices pipeline at various dataset scales."""
    scales = scales or SCALES
    results = []

    with open(SERVICES_CONFIG_PATH) as f:
        services_config = json.load(f)

    # Import Preparator (needs services running)
    from preparator.preparator_v4 import Preparator

    for n in scales:
        input_path = os.path.join(DATA_DIR, f"hr_{n // 1000}k.csv")
        if not os.path.exists(input_path):
            print(f"  SKIP {n:>8,} rows — file not found: {input_path}")
            continue

        # Copy dataset to shared volume path expected by services
        shared_path = f"/app/data/benchmark/hr_{n // 1000}k.csv"

        durations = []

        for r in range(repeats):
            dataset_name = f"bench_{n // 1000}k_r{r}"

            t0 = time.time()
            try:
                with Preparator(services_config) as prep:
                    # Step 1: Extract
                    ipc_data = prep.extract_csv(dataset_name=dataset_name, file_path=shared_path)

                    # Step 2: Quality check
                    ipc_data = prep.check_quality(
                        ipc_data, dataset_name=dataset_name,
                        rules={"min_rows": 10, "check_null_ratio": True, "threshold_null_ratio": 0.5},
                    )

                    # Step 3: Delete columns
                    ipc_data = prep.delete_columns(
                        ipc_data,
                        columns=["EmployeeCount", "Over18", "StandardHours"],
                        dataset_name=dataset_name,
                    )

                    # Step 4: Outlier detection
                    ipc_data = prep.detect_outliers(
                        ipc_data, dataset_name=dataset_name,
                        column="MonthlyIncome", z_threshold=3.0,
                    )

                    # Step 5: Clean NaN
                    ipc_data = prep.clean_nan(ipc_data, dataset_name=dataset_name)

                    # Step 6: Load
                    prep.load_data(ipc_data, format="csv", dataset_name=dataset_name)

                elapsed = time.time() - t0
                durations.append(elapsed)
            except Exception as e:
                print(f"  ERROR {n:>8,} rows run {r}: {e}")
                continue

        if not durations:
            print(f"  SKIP {n:>8,} rows — all runs failed")
            continue

        avg_duration = sum(durations) / len(durations)
        throughput = n / avg_duration if avg_duration > 0 else 0

        result = {
            "approach": "microservices",
            "rows": n,
            "avg_duration_sec": round(avg_duration, 3),
            "min_duration_sec": round(min(durations), 3),
            "max_duration_sec": round(max(durations), 3),
            "avg_peak_memory_mb": None,  # Hard to measure across containers
            "throughput_rows_per_sec": round(throughput),
            "repeats": len(durations),
        }
        results.append(result)
        print(f"  Microservices {n:>8,} rows: {avg_duration:.3f}s avg, {throughput:,.0f} rows/s")

    return results


def generate_plots(results: list[dict]):
    """Generate comparison charts from benchmark results."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    df = pd.DataFrame(results)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    approaches = df["approach"].unique()
    colors = {"monolith": "#2196F3", "microservices": "#FF9800"}

    # ── Chart 1: Duration vs Scale ──
    fig, ax = plt.subplots(figsize=(10, 6))
    for approach in approaches:
        subset = df[df["approach"] == approach]
        ax.plot(
            subset["rows"], subset["avg_duration_sec"],
            marker="o", linewidth=2, label=approach.capitalize(),
            color=colors.get(approach, "#999"),
        )
    ax.set_xlabel("Dataset Size (rows)")
    ax.set_ylabel("Duration (seconds)")
    ax.set_title("ETL Pipeline — Execution Time: Monolith vs Microservices")
    ax.set_xscale("log")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "duration_comparison.png"), dpi=150)
    plt.close()

    # ── Chart 2: Throughput vs Scale ──
    fig, ax = plt.subplots(figsize=(10, 6))
    for approach in approaches:
        subset = df[df["approach"] == approach]
        ax.plot(
            subset["rows"], subset["throughput_rows_per_sec"],
            marker="s", linewidth=2, label=approach.capitalize(),
            color=colors.get(approach, "#999"),
        )
    ax.set_xlabel("Dataset Size (rows)")
    ax.set_ylabel("Throughput (rows/sec)")
    ax.set_title("ETL Pipeline — Throughput: Monolith vs Microservices")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "throughput_comparison.png"), dpi=150)
    plt.close()

    # ── Chart 3: Overhead ratio ──
    if len(approaches) == 2:
        mono = df[df["approach"] == "monolith"].set_index("rows")["avg_duration_sec"]
        micro = df[df["approach"] == "microservices"].set_index("rows")["avg_duration_sec"]
        common_rows = mono.index.intersection(micro.index)

        if len(common_rows) > 0:
            ratio = micro[common_rows] / mono[common_rows]

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(
                range(len(common_rows)),
                ratio.values,
                color=["#FF5722" if r > 1 else "#4CAF50" for r in ratio.values],
                alpha=0.8,
            )
            ax.set_xticks(range(len(common_rows)))
            ax.set_xticklabels([f"{r:,}" for r in common_rows])
            ax.set_xlabel("Dataset Size (rows)")
            ax.set_ylabel("Microservices / Monolith ratio")
            ax.set_title("Microservices Overhead Ratio (< 1 = microservices faster)")
            ax.axhline(y=1, color="black", linestyle="--", alpha=0.5)
            ax.grid(True, alpha=0.3, axis="y")
            fig.tight_layout()
            fig.savefig(os.path.join(RESULTS_DIR, "overhead_ratio.png"), dpi=150)
            plt.close()

    print(f"\nCharts saved to {RESULTS_DIR}/")


def generate_plotly_report(results: list[dict]):
    """Generate interactive Plotly HTML report."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        print("  plotly not installed, skipping interactive report")
        return

    df = pd.DataFrame(results)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Execution Time (seconds)", "Throughput (rows/sec)"),
        vertical_spacing=0.15,
    )

    for approach in df["approach"].unique():
        subset = df[df["approach"] == approach]
        fig.add_trace(
            go.Scatter(
                x=subset["rows"], y=subset["avg_duration_sec"],
                mode="lines+markers", name=f"{approach} — duration",
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=subset["rows"], y=subset["throughput_rows_per_sec"],
                mode="lines+markers", name=f"{approach} — throughput",
            ),
            row=2, col=1,
        )

    fig.update_xaxes(type="log", title_text="Dataset Size (rows)")
    fig.update_layout(
        title_text="ETL Benchmark: Monolith vs Microservices",
        height=800,
        template="plotly_white",
    )

    html_path = os.path.join(RESULTS_DIR, "benchmark_report.html")
    fig.write_html(html_path)
    print(f"\nInteractive report: {html_path}")


def main():
    parser = argparse.ArgumentParser(description="ETL Benchmark Runner")
    parser.add_argument("--mode", choices=["monolith", "microservices", "both"], default="monolith")
    parser.add_argument("--repeats", type=int, default=3, help="Runs per scale")
    parser.add_argument("--plot", action="store_true", help="Generate comparison charts")
    parser.add_argument("--scales", type=int, nargs="+", default=None, help="Custom scales (e.g., 1000 10000)")
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    all_results = []

    if args.mode in ("monolith", "both"):
        print("═" * 50)
        print("MONOLITHIC PIPELINE BENCHMARK")
        print("═" * 50)
        mono_results = run_monolith_benchmark(args.scales, args.repeats)
        all_results.extend(mono_results)

    if args.mode in ("microservices", "both"):
        print("═" * 50)
        print("MICROSERVICES PIPELINE BENCHMARK")
        print("═" * 50)
        micro_results = run_microservices_benchmark(args.scales, args.repeats)
        all_results.extend(micro_results)

    # Save raw results
    results_path = os.path.join(RESULTS_DIR, "benchmark_results.json")
    with open(results_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {results_path}")

    # Summary table
    if all_results:
        summary = pd.DataFrame(all_results)
        print("\n" + "═" * 50)
        print("BENCHMARK SUMMARY")
        print("═" * 50)
        print(summary.to_string(index=False))

    # Generate charts
    if args.plot and all_results:
        print("\nGenerating charts...")
        generate_plots(all_results)
        generate_plotly_report(all_results)


if __name__ == "__main__":
    main()
