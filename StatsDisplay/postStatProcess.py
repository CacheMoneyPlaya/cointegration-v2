import os
from Cointegration.cointegration import run_cointegration_analysis
from Reversion.zScore import run_zscore_analysis
from dotenv import load_dotenv
from termcolor import colored

# Load the environment variables from baskets.env
load_dotenv('baskets.env')

# Define basket categories and their associated color codes
BASKETS = {
    'CHINESE_BASED': os.getenv('CHINESE_BASED').split(','),
    'DEFI': os.getenv('DEFI').split(','),
    'GAMING_METAVERSE': os.getenv('GAMING_METAVERSE').split(','),
    'LAYER1_PROTOCOLS': os.getenv('LAYER1_PROTOCOLS').split(','),
    'LAYER2_SCALING': os.getenv('LAYER2_SCALING').split(','),
    'PRIVACY_COINS': os.getenv('PRIVACY_COINS').split(','),
    'STABLECOINS': os.getenv('STABLECOINS').split(','),
    'INFRASTRUCTURE_ORACLES': os.getenv('INFRASTRUCTURE_ORACLES').split(','),
    'NFT_COLLECTIBLES': os.getenv('NFT_COLLECTIBLES').split(',')
}

COLORS = {
    'CHINESE_BASED': 'red',
    'DEFI': 'blue',
    'GAMING_METAVERSE': 'orange',
    'LAYER1_PROTOCOLS': 'purple',
    'LAYER2_SCALING': 'green',
    'PRIVACY_COINS': 'pink',
    'STABLECOINS': 'yellow',
    'INFRASTRUCTURE_ORACLES': 'white',
    'NFT_COLLECTIBLES': 'brown'
}

def find_basket(pair):
    """Identify which basket a given pair belongs to."""
    for basket_name, pairs in BASKETS.items():
        if pair in pairs:
            return basket_name
    return None

def process_and_display_stats():
    """Run cointegration and z-score analyses, then display filtered results."""

    # Step 1: Run cointegration analysis and get pairs with p < 0.05
    print("Running cointegration analysis...")
    passing_pairs = run_cointegration_analysis()

    # Step 2: Run z-score and half-life analysis on pairs passing cointegration
    print("\nRunning z-score analysis and related z-score metrics..")
    zscore_results = [result for result in run_zscore_analysis(passing_pairs) if result is not None]

    # Step 3: Display results
    print("\nGenerated pair metrics:")
    for result in zscore_results:
        basket_a = find_basket(result['Ax'])
        basket_b = find_basket(result['Bx'])

        # Check if both pairs belong to the same basket
        if basket_a and basket_a == basket_b:
            color = COLORS.get(basket_a, None)
            if color:
                # Format the output with the appropriate color
                output = colored(
                    f"{result['Ax']} & {result['Bx']} - p: {result['p_value']:.4f} Z: {result['Z_score']} "
                    f"Half-life: {result['half_life']}H Mean Reversion Ratio: {result['mean_reversion_ratio']} "
                    f"Dominant Frequency: {result['has_dominant_frequency']} ✅",
                    color
                )
            else:
                # Default to no color if color not found
                output = (
                    f"{result['Ax']} & {result['Bx']} - p: {result['p_value']:.4f} Z: {result['Z_score']} "
                    f"Half-life: {result['half_life']}H Mean Reversion Ratio: {result['mean_reversion_ratio']} "
                    f"Dominant Frequency: {result['has_dominant_frequency']} ✅"
                )
        else:
            # Default color if pairs do not belong to the same basket
            output = (
                f"{result['Ax']} & {result['Bx']} - p: {result['p_value']:.4f} Z: {result['Z_score']} "
                f"Half-life: {result['half_life']}H Mean Reversion Ratio: {result['mean_reversion_ratio']} "
                f"Dominant Frequency: {result['has_dominant_frequency']} ✅"
            )

        print(output)
