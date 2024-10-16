from Cointegration.cointegration import run_cointegration_analysis
from Reversion.zScore import run_zscore_analysis

def process_and_display_stats():
    """Run cointegration and z-score analyses, then display filtered results."""

    # Step 1: Run cointegration analysis and get pairs with p < 0.05
    print("Running cointegration analysis...")
    passing_pairs = run_cointegration_analysis()

    # Step 2: Run z-score and half-life analysis on pairs passing cointegration
    print("\nRunning z-score analysis for mean reversion...")
    zscore_results = [result for result in run_zscore_analysis(passing_pairs) if result is not None]

    # Step 3: Display results
    print("\nCointegrated Pairs with Significant Z-scores and Mean Reversion Times:")
    for result in zscore_results:
        tick_icon = "✅"
        print(f"{result['Ax']} & {result['Bx']} - p: {result['p_value']:.4f} Z: {result['Z_score']} "
              f"Half-life: {result['half_life']}H Mean Reversion Ratio: {result['mean_reversion_ratio']} "
              f"Dominant Frequency: {result['has_dominant_frequency']} {tick_icon}")
