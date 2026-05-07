import pandas as pd
import pandera as pa
from pandera.errors import SchemaErrors, SchemaError
from sklearn.ensemble import IsolationForest

def check_nulls(df, filename):
    """Checks for empty values in the CSV."""
    issues = []
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            issues.append(f"Column '{col}' has {count} null values")
    return issues

def check_outliers(df, filename):
    """Detects values more than 3 standard deviations from the mean."""
    issues = []
    # Only check numeric columns
    numeric_df = df.select_dtypes(include=['number'])
    for col in numeric_df.columns:
        mean = df[col].mean()
        std = df[col].std()
        # Find values > 3 standard deviations away
        outliers = df[(df[col] > mean + 3*std) | (df[col] < mean - 3*std)]
        if not outliers.empty:
            issues.append(f"Column '{col}' has {len(outliers)} outlier(s) detected")
    return issues

def check_data_contract(df, filename):
    """Enforces strict schema using pandera Data Contracts."""
    issues = []
    # Define the data contract based on expected columns
    # nullable=True is used because null-checks are handled separately by check_nulls
    schema = pa.DataFrameSchema(
        {
            "age": pa.Column(float, pa.Check.in_range(18, 65), nullable=True, required=False),
            "salary": pa.Column(float, pa.Check.ge(0), nullable=True, required=False),
            "score": pa.Column(float, pa.Check.in_range(0, 100), nullable=True, required=False)
        },
        strict=False,
        coerce=True
    )

    try:
        schema.validate(df, lazy=True)
    except SchemaErrors as err:
        for _, error in err.failure_cases.iterrows():
            if pd.notna(error.get("failure_case")):
                issues.append(f"Data Contract Violation: Column '{error.get('column')}' failed check '{error.get('check')}' with value '{error.get('failure_case')}'")
    except SchemaError as err:
        issues.append(f"Data Contract Violation: {str(err)}")
        
    return issues

def detect_anomalies(df, filename):
    """Uses Machine Learning (Isolation Forest) to detect anomalies in numeric data."""
    issues = []
    numeric_df = df.select_dtypes(include=['number']).dropna()
    
    if len(numeric_df) < 10:  # Need enough data to train
        return issues
        
    # Train Isolation Forest dynamically on the file's numeric data
    clf = IsolationForest(random_state=42)
    clf.fit(numeric_df)
    scores = clf.decision_function(numeric_df)
    
    # Use a strict threshold to avoid flagging normal points in clean datasets
    anomaly_indices = numeric_df.index[scores < -0.15]
    
    if len(anomaly_indices) > 0:
        issues.append(f"Anomaly Detection: Found {len(anomaly_indices)} statistically anomalous row(s) based on ML model")
        
    return issues
