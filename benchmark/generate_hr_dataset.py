"""
Synthetic HR Dataset Generator

Generates realistic HR/People Analytics datasets at various scales
for benchmarking the ETL microservices platform.

Based on the IBM HR Analytics Employee Attrition dataset schema.

Usage:
    python benchmark/generate_hr_dataset.py --rows 10000 --output data/hr_10k.csv
    python benchmark/generate_hr_dataset.py --rows 100000 --output data/hr_100k.csv
"""

import argparse
import os
import time

import numpy as np
import pandas as pd

# ── Column definitions matching IBM HR Attrition schema ──
DEPARTMENTS = ["Sales", "Research & Development", "Human Resources"]
EDUCATION_FIELDS = ["Life Sciences", "Medical", "Marketing", "Technical Degree", "Human Resources", "Other"]
JOB_ROLES = [
    "Sales Executive", "Research Scientist", "Laboratory Technician",
    "Manufacturing Director", "Healthcare Representative", "Manager",
    "Sales Representative", "Research Director", "Human Resources",
]
BUSINESS_TRAVEL = ["Travel_Rarely", "Travel_Frequently", "Non-Travel"]
GENDERS = ["Male", "Female"]
MARITAL_STATUS = ["Single", "Married", "Divorced"]
OVERTIME = ["Yes", "No"]
ATTRITION = ["Yes", "No"]


def generate_hr_dataset(n_rows: int, seed: int = 42, null_ratio: float = 0.02) -> pd.DataFrame:
    """
    Generate a synthetic HR dataset with realistic distributions.

    Args:
        n_rows: Number of employee records to generate.
        seed: Random seed for reproducibility.
        null_ratio: Fraction of values to set to NaN (simulates missing data).

    Returns:
        DataFrame with HR employee data.
    """
    rng = np.random.default_rng(seed)

    age = rng.integers(18, 65, size=n_rows)
    years_at_company = np.minimum(rng.exponential(scale=5, size=n_rows).astype(int), age - 18)
    years_in_role = np.minimum(rng.exponential(scale=3, size=n_rows).astype(int), years_at_company)
    years_since_promotion = np.minimum(rng.exponential(scale=2, size=n_rows).astype(int), years_in_role)
    years_with_manager = np.minimum(rng.exponential(scale=3, size=n_rows).astype(int), years_at_company)
    total_working_years = np.maximum(years_at_company, rng.integers(0, 40, size=n_rows))

    # Monthly income: log-normal distribution (realistic salary distribution)
    monthly_income = (rng.lognormal(mean=8.5, sigma=0.6, size=n_rows)).astype(int)
    monthly_income = np.clip(monthly_income, 1000, 20000)

    # Inject a few outliers for outlier-detection testing
    n_outliers = max(2, int(n_rows * 0.005))
    outlier_idx = rng.choice(n_rows, size=n_outliers, replace=False)
    monthly_income[outlier_idx] = rng.integers(50000, 100000, size=n_outliers)

    daily_rate = rng.integers(100, 1500, size=n_rows)
    hourly_rate = rng.integers(30, 100, size=n_rows)
    monthly_rate = rng.integers(2000, 27000, size=n_rows)
    percent_salary_hike = rng.integers(11, 25, size=n_rows)

    data = {
        "EmployeeNumber": np.arange(1, n_rows + 1),
        "Age": age,
        "Attrition": rng.choice(ATTRITION, size=n_rows, p=[0.16, 0.84]),
        "BusinessTravel": rng.choice(BUSINESS_TRAVEL, size=n_rows, p=[0.71, 0.19, 0.10]),
        "DailyRate": daily_rate,
        "Department": rng.choice(DEPARTMENTS, size=n_rows, p=[0.30, 0.63, 0.07]),
        "DistanceFromHome": rng.integers(1, 30, size=n_rows),
        "Education": rng.integers(1, 6, size=n_rows),
        "EducationField": rng.choice(EDUCATION_FIELDS, size=n_rows),
        "EmployeeCount": np.ones(n_rows, dtype=int),  # Constant — should be dropped
        "EnvironmentSatisfaction": rng.integers(1, 5, size=n_rows),
        "Gender": rng.choice(GENDERS, size=n_rows),
        "HourlyRate": hourly_rate,
        "JobInvolvement": rng.integers(1, 5, size=n_rows),
        "JobLevel": rng.integers(1, 6, size=n_rows),
        "JobRole": rng.choice(JOB_ROLES, size=n_rows),
        "JobSatisfaction": rng.integers(1, 5, size=n_rows),
        "MaritalStatus": rng.choice(MARITAL_STATUS, size=n_rows),
        "MonthlyIncome": monthly_income,
        "MonthlyRate": monthly_rate,
        "NumCompaniesWorked": rng.integers(0, 10, size=n_rows),
        "Over18": np.full(n_rows, "Y"),  # Constant — should be dropped
        "OverTime": rng.choice(OVERTIME, size=n_rows),
        "PercentSalaryHike": percent_salary_hike,
        "PerformanceRating": rng.choice([3, 4], size=n_rows, p=[0.85, 0.15]),
        "RelationshipSatisfaction": rng.integers(1, 5, size=n_rows),
        "StandardHours": np.full(n_rows, 80, dtype=int),  # Constant — should be dropped
        "StockOptionLevel": rng.integers(0, 4, size=n_rows),
        "TotalWorkingYears": total_working_years,
        "TrainingTimesLastYear": rng.integers(0, 7, size=n_rows),
        "WorkLifeBalance": rng.integers(1, 5, size=n_rows),
        "YearsAtCompany": years_at_company,
        "YearsInCurrentRole": years_in_role,
        "YearsSinceLastPromotion": years_since_promotion,
        "YearsWithCurrManager": years_with_manager,
    }

    df = pd.DataFrame(data)

    # Inject NaN values for realism
    if null_ratio > 0:
        n_nulls_per_col = int(n_rows * null_ratio)
        nullable_cols = [
            "EnvironmentSatisfaction", "JobSatisfaction",
            "WorkLifeBalance", "NumCompaniesWorked", "TotalWorkingYears",
        ]
        for col in nullable_cols:
            null_idx = rng.choice(n_rows, size=n_nulls_per_col, replace=False)
            df.loc[null_idx, col] = np.nan

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic HR dataset for benchmarks")
    parser.add_argument("--rows", type=int, default=10000, help="Number of rows to generate")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--null-ratio", type=float, default=0.02, help="Fraction of NaN values per nullable col")
    parser.add_argument("--all-scales", action="store_true", help="Generate datasets at all benchmark scales")
    args = parser.parse_args()

    os.makedirs("benchmark/data", exist_ok=True)

    if args.all_scales:
        scales = [1_000, 10_000, 50_000, 100_000, 500_000]
        for n in scales:
            output = f"benchmark/data/hr_{n // 1000}k.csv"
            print(f"Generating {n:,} rows → {output} ...", end=" ", flush=True)
            t0 = time.time()
            df = generate_hr_dataset(n, seed=args.seed, null_ratio=args.null_ratio)
            df.to_csv(output, index=False)
            elapsed = time.time() - t0
            size_mb = os.path.getsize(output) / (1024 * 1024)
            print(f"done ({elapsed:.1f}s, {size_mb:.1f} MB)")
    else:
        output = args.output or f"benchmark/data/hr_{args.rows // 1000}k.csv"
        print(f"Generating {args.rows:,} rows → {output} ...", end=" ", flush=True)
        t0 = time.time()
        df = generate_hr_dataset(args.rows, seed=args.seed, null_ratio=args.null_ratio)
        df.to_csv(output, index=False)
        elapsed = time.time() - t0
        size_mb = os.path.getsize(output) / (1024 * 1024)
        print(f"done ({elapsed:.1f}s, {size_mb:.1f} MB)")
        print(f"Shape: {df.shape}, Columns: {list(df.columns)}")


if __name__ == "__main__":
    main()
