def check_nulls(df, filename, conn):
    """Checks for empty values in the CSV."""
    issues = []
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            issues.append(f"Column '{col}' has {count} null values")
    return issues

def check_outliers(df, filename, conn):
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
